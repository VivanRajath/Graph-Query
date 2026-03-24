"""
JSONL → SQLite ingestion script for SAP O2C data.

Reads all .jsonl files from data/sap-o2c-data/<table_name>/ subdirectories
and loads each table into backend/data.db.

Usage:
    python -m backend.ingest.load_sqlite
"""

import json
import os
import sqlite3
import sys
from pathlib import Path

# ── paths ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent.parent          # graph-query-system/
DATA_DIR = ROOT / "data" / "sap-o2c-data"
DB_PATH = ROOT / "backend" / "data.db"


def flatten_value(val):
    """Flatten nested dicts/lists into a string for SQLite storage."""
    if val is None:
        return None
    if isinstance(val, dict):
        # e.g. {"hours": 6, "minutes": 49, "seconds": 13} → "06:49:13"
        if set(val.keys()) == {"hours", "minutes", "seconds"}:
            return f"{val['hours']:02d}:{val['minutes']:02d}:{val['seconds']:02d}"
        return json.dumps(val)
    if isinstance(val, list):
        return json.dumps(val)
    if isinstance(val, bool):
        return int(val)
    return val


def read_jsonl(filepath: Path) -> list[dict]:
    """Read a .jsonl file and return list of flattened dicts."""
    rows = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            flat = {k: flatten_value(v) for k, v in obj.items()}
            rows.append(flat)
    return rows


def infer_columns(rows: list[dict]) -> list[str]:
    """Collect all unique column names preserving insertion order."""
    seen = {}
    for row in rows:
        for k in row:
            if k not in seen:
                seen[k] = True
    return list(seen.keys())


def create_table(conn: sqlite3.Connection, table: str, columns: list[str]):
    """Create table with all TEXT columns (simple but flexible)."""
    cols_sql = ", ".join(f'"{c}" TEXT' for c in columns)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" ({cols_sql})')


def insert_rows(conn: sqlite3.Connection, table: str, columns: list[str], rows: list[dict]):
    """Batch-insert rows into the table."""
    placeholders = ", ".join(["?"] * len(columns))
    sql = f'INSERT INTO "{table}" ({", ".join(f"{c!r}" for c in columns)}) VALUES ({placeholders})'
    # Use proper quoting
    col_names = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})'
    
    batch = []
    for row in rows:
        vals = tuple(str(row.get(c, "")) if row.get(c) is not None else None for c in columns)
        batch.append(vals)
    
    conn.executemany(sql, batch)


def main():
    if not DATA_DIR.exists():
        print(f"[FAIL] Data directory not found: {DATA_DIR}")
        sys.exit(1)

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")

    table_dirs = sorted(
        p for p in DATA_DIR.iterdir() if p.is_dir()
    )
    
    if not table_dirs:
        print("[FAIL] No subdirectories found under data/sap-o2c-data/")
        sys.exit(1)

    total_rows = 0
    for td in table_dirs:
        table_name = td.name                              # e.g. "sales_order_headers"
        jsonl_files = sorted(td.glob("*.jsonl"))
        if not jsonl_files:
            print(f"  [WARN] Skipping {table_name}: no .jsonl files")
            continue

        # Collect all rows from all part files
        all_rows = []
        for jf in jsonl_files:
            all_rows.extend(read_jsonl(jf))

        if not all_rows:
            print(f"  [WARN] Skipping {table_name}: 0 rows")
            continue

        columns = infer_columns(all_rows)
        create_table(conn, table_name, columns)
        insert_rows(conn, table_name, columns, all_rows)
        conn.commit()

        total_rows += len(all_rows)
        print(f"  [OK]  {table_name}: {len(all_rows)} rows, {len(columns)} columns")

    conn.close()
    print(f"\n[DONE] Total: {total_rows} rows loaded into {DB_PATH}")


if __name__ == "__main__":
    main()
