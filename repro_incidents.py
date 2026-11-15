"""
Reproduction script for the two ingest incidents.

Incident 1: a failing row mid-import must roll back EVERYTHING for the
default (non-streaming) path - rows, CREATE TABLE, ALTER-added columns,
--truncate deletes and extract lookup rows. Explicit --batch-size keeps
per-batch streaming commits.

Incident 2: an incremental `--alter` CSV import must produce the same
column types as a one-shot import of the same CSV, and `--type` must
apply to newly added columns. Existing columns (e.g. TEXT zip codes
with leading zeros) must never be rewritten.

Run: .venv/bin/python repro_incidents.py
Exit code 0 = all checks pass, 1 = at least one failure.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import sqlite_utils

CLI = [sys.executable, "-m", "sqlite_utils"]

failures = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    if not ok:
        failures.append(name)


def run_cli(args, input_text=None):
    return subprocess.run(
        CLI + args, input=input_text, capture_output=True, text=True
    )


def column_types(db_path, table):
    db = sqlite_utils.Database(db_path)
    return {c.name: c.type.upper() for c in db[table].columns}


tmp = Path(tempfile.mkdtemp(prefix="sqlite-utils-repro-"))
print(f"scratch dir: {tmp}\n")

# ---------------------------------------------------------------------
# Incident 1a: CLI insert - row 150 violates pk, nothing may survive
# ---------------------------------------------------------------------
db1 = tmp / "incident1a.db"
rows = [{"id": i, "v": i * 2} for i in range(1, 160)]
rows[149] = {"id": 1, "v": -1}  # 150th row duplicates pk of row 1
ndjson = "\n".join(json.dumps(r) for r in rows)
p = run_cli(["insert", str(db1), "t", "-", "--nl", "--pk", "id"], ndjson)
check("1a: CLI insert exits non-zero on constraint violation", p.returncode != 0)
db = sqlite_utils.Database(db1)
check(
    "1a: failed import leaves no table behind (create rolled back)",
    "t" not in db.table_names(),
    f"tables={db.table_names()} rows={db['t'].count if 't' in db.table_names() else 0}",
)

# Same but into a pre-existing table: partial rows must not survive
db1b = tmp / "incident1a2.db"
sqlite_utils.Database(db1b)["t"].insert({"id": 999, "v": 999}, pk="id")
p = run_cli(["insert", str(db1b), "t", "-", "--nl", "--pk", "id"], ndjson)
db = sqlite_utils.Database(db1b)
check("1a2: CLI insert into existing table exits non-zero", p.returncode != 0)
check(
    "1a2: existing table keeps only its original row",
    list(db["t"].rows) == [{"id": 999, "v": 999}],
    f"rows={list(db['t'].rows)[:5]}... count={db['t'].count}",
)

# upsert path: a bad record (missing pk) mid-stream must roll back too
db1c = tmp / "incident1a3.db"
up_rows = [{"id": i, "v": i} for i in range(1, 160)]
up_rows[149] = {"v": "no-pk-here"}  # upsert requires pk value
p = run_cli(
    ["upsert", str(db1c), "t", "-", "--nl", "--pk", "id"],
    "\n".join(json.dumps(r) for r in up_rows),
)
db = sqlite_utils.Database(db1c)
check("1a3: CLI upsert exits non-zero on bad record", p.returncode != 0)
check(
    "1a3: failed upsert leaves no rows behind",
    "t" not in db.table_names() or db["t"].count == 0,
)

# ---------------------------------------------------------------------
# Incident 1b: Python insert_all(alter=True) is all-or-nothing
# ---------------------------------------------------------------------
db2 = sqlite_utils.Database(memory=True)
recs = [{"id": i, "v": i} for i in range(1, 160)]
recs[149] = {"id": 1, "v": -1}  # duplicate pk
try:
    db2["t"].insert_all(recs, pk="id", alter=True)
    raised = False
except Exception:
    raised = True
check("1b: insert_all raises on constraint violation", raised)
check(
    "1b: failed insert_all leaves no table/rows behind",
    "t" not in db2.table_names(),
    f"tables={db2.table_names()}",
)

# alter-added columns roll back with the failed batch
db2b = sqlite_utils.Database(memory=True)
db2b["t"].insert({"id": 1, "a": 1}, pk="id")
recs = [{"id": i + 1, "a": i, "newcol": i} for i in range(1, 160)]
recs[149] = {"id": 1, "a": -1, "newcol": -1}  # duplicate pk
try:
    db2b["t"].insert_all(recs, pk="id", alter=True)
except Exception:
    pass
check(
    "1b2: alter-added column rolled back, original row intact",
    [c.name for c in db2b["t"].columns] == ["id", "a"]
    and list(db2b["t"].rows) == [{"id": 1, "a": 1}],
    f"columns={[c.name for c in db2b['t'].columns]} rows={list(db2b['t'].rows)}",
)

# ---------------------------------------------------------------------
# Incident 1c: --truncate delete rolls back when the import fails
# ---------------------------------------------------------------------
db3 = tmp / "incident1c.db"
sqlite_utils.Database(db3)["t"].insert_all(
    [{"id": i, "v": "old"} for i in range(1, 6)], pk="id"
)
p = run_cli(["insert", str(db3), "t", "-", "--nl", "--pk", "id", "--truncate"], ndjson)
db = sqlite_utils.Database(db3)
check("1c: --truncate import exits non-zero", p.returncode != 0)
check(
    "1c: pre-existing rows survive a failed --truncate import",
    db["t"].count == 5 and all(r["v"] == "old" for r in db["t"].rows),
    f"count={db['t'].count}",
)

# ---------------------------------------------------------------------
# Incident 1d: extract lookup rows roll back with the failed import
# ---------------------------------------------------------------------
db4 = sqlite_utils.Database(memory=True)
recs = [
    {"id": i, "species": "oak" if i % 2 else "pine"} for i in range(1, 160)
]
recs[149] = {"id": 1, "species": "birch"}  # duplicate pk
try:
    db4["trees"].insert_all(recs, pk="id", extracts={"species": "Species"})
except Exception:
    pass
check(
    "1d: extract lookup table rolled back too",
    "trees" not in db4.table_names() and "Species" not in db4.table_names(),
    f"tables={db4.table_names()}",
)

# ---------------------------------------------------------------------
# Incident 2a: --alter incremental import gets the same types as one-shot
# ---------------------------------------------------------------------
csv_full = "id,zipcode,amount\n" + "\n".join(
    f"{i},0123{i % 10},{i * 7}" for i in range(1, 21)
)
csv_no_amount = "id,zipcode\n" + "\n".join(f"{i},0123{i % 10}" for i in range(1, 21))
csv_amount_only = "id,amount\n" + "\n".join(f"{i},{i * 7}" for i in range(21, 41))

db_one_shot = tmp / "oneshot.db"
p = run_cli(["insert", str(db_one_shot), "t", "-", "--csv", "--pk", "id"], csv_full)
assert p.returncode == 0, p.stderr

db_incr = tmp / "incremental.db"
p = run_cli(
    ["insert", str(db_incr), "t", "-", "--csv", "--pk", "id"], csv_no_amount
)
assert p.returncode == 0, p.stderr
p = run_cli(
    ["insert", str(db_incr), "t", "-", "--csv", "--pk", "id", "--alter"],
    csv_amount_only,
)
assert p.returncode == 0, p.stderr

one_shot_types = column_types(db_one_shot, "t")
incr_types = column_types(db_incr, "t")
check(
    "2a: incremental --alter types match one-shot types",
    one_shot_types == incr_types,
    f"one-shot={one_shot_types} incremental={incr_types}",
)

# ---------------------------------------------------------------------
# Incident 2b: --type applies to columns added by --alter
# ---------------------------------------------------------------------
db_type = tmp / "typeoverride.db"
p = run_cli(["insert", str(db_type), "t", "-", "--csv", "--pk", "id"], csv_no_amount)
assert p.returncode == 0, p.stderr
p = run_cli(
    [
        "insert", str(db_type), "t", "-", "--csv", "--pk", "id",
        "--alter", "--type", "amount", "float",
    ],
    csv_amount_only,
)
assert p.returncode == 0, p.stderr
check(
    "2b: --type applies to newly added column",
    column_types(db_type, "t").get("amount") == "FLOAT",
    f"types={column_types(db_type, 't')}",
)

# ---------------------------------------------------------------------
# Hard constraint A: existing TEXT zipcode column is never rewritten
# ---------------------------------------------------------------------
db_zip = tmp / "zipcode.db"
sqlite_utils.Database(db_zip)["t"].create(
    {"id": int, "zipcode": str}, pk="id"
)
sqlite_utils.Database(db_zip)["t"].insert_all(
    [{"id": i, "zipcode": f"0123{i % 10}"} for i in range(1, 21)]
)
p = run_cli(
    ["insert", str(db_zip), "t", "-", "--csv", "--pk", "id", "--alter"],
    csv_amount_only,
)
assert p.returncode == 0, p.stderr
zip_types = column_types(db_zip, "t")
zip_rows = list(sqlite_utils.Database(db_zip)["t"].rows)
check(
    "A: existing zipcode column stays TEXT with leading zeros",
    zip_types.get("zipcode") == "TEXT"
    and zip_rows[0]["zipcode"] == "01231"
    and all(
        isinstance(r["zipcode"], str)
        for r in zip_rows
        if r["zipcode"] is not None
    ),
    f"zipcode type={zip_types.get('zipcode')} value={zip_rows[0]['zipcode']!r}",
)
check(
    "A2: newly added amount column still got the detected INTEGER type",
    zip_types.get("amount") == "INTEGER",
    f"amount type={zip_types.get('amount')}",
)

# ---------------------------------------------------------------------
# Hard constraint B: explicit --batch-size keeps per-batch streaming
# ---------------------------------------------------------------------
db5 = tmp / "streaming.db"
p = run_cli(
    ["insert", str(db5), "t", "-", "--nl", "--pk", "id", "--batch-size", "100"],
    ndjson,
)
db = sqlite_utils.Database(db5)
check("B: explicit --batch-size import exits non-zero", p.returncode != 0)
check(
    "B: explicit --batch-size still commits per batch (first 100 visible)",
    "t" in db.table_names() and db["t"].count == 100,
    f"count={db['t'].count if 't' in db.table_names() else 0}",
)

print()
if failures:
    print(f"{len(failures)} check(s) FAILED")
    sys.exit(1)
print("All checks passed")
