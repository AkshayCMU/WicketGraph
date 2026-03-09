"""
Bulk Silver Layer Ingestion Orchestrator

Reads all Cricsheet JSON files from the bronze directory, extracts 5 DataFrames
per match via extractor.process_match(), and persists them into a single SQLite
database at data/02_silver_tables/silver.db.

Incremental: an ingestion_log table tracks processed files; already-successful
files are skipped on re-run. Failed files are retried automatically.

Usage:
    python src/ingest_all.py
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Protocol

import pandas as pd

from extractor import process_match

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

BRONZE_DIR = Path(__file__).parent.parent / "data" / "01_bronze_cricsheet"
SILVER_DB = Path(__file__).parent.parent / "data" / "02_silver_tables" / "silver.db"

# ---------------------------------------------------------------------------
# SourceAdapter Protocol
# ---------------------------------------------------------------------------

class SourceAdapter(Protocol):
    def process_file(self, file_path: Path) -> tuple[pd.DataFrame, ...]:
        ...


class CricksheetAdapter:
    """Wraps extractor.process_match() to satisfy the SourceAdapter protocol."""

    def process_file(self, file_path: Path) -> tuple[pd.DataFrame, ...]:
        return process_match(file_path)


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

INGESTION_LOG_DDL = """
CREATE TABLE IF NOT EXISTS ingestion_log (
    file_name   TEXT PRIMARY KEY,
    match_id    TEXT,
    ingested_at TEXT,
    status      TEXT
);
"""


def _init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute(INGESTION_LOG_DDL)
    conn.commit()
    return conn


def _load_processed(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT file_name FROM ingestion_log WHERE status = 'success'"
    ).fetchall()
    return {row[0] for row in rows}


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

TABLE_NAMES = ("matches", "innings", "deliveries", "wickets", "extras")


def _write_dataframes(
    conn: sqlite3.Connection,
    dataframes: tuple[pd.DataFrame, ...],
) -> None:
    for name, df in zip(TABLE_NAMES, dataframes):
        # SQLite cannot bind Python lists; serialize the fielders column to JSON.
        if name == "wickets" and "fielders" in df.columns and len(df) > 0:
            df = df.copy()
            df["fielders"] = df["fielders"].apply(json.dumps)
        df.to_sql(name, conn, if_exists="append", index=False)


def _log_success(conn: sqlite3.Connection, file_name: str, match_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO ingestion_log (file_name, match_id, ingested_at, status) "
        "VALUES (?, ?, ?, 'success')",
        (file_name, match_id, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def _delete_match_data(conn: sqlite3.Connection, match_id: str) -> None:
    """Delete all rows for a match_id from silver tables (compensating transaction)."""
    for table in TABLE_NAMES:
        try:
            conn.execute(f"DELETE FROM {table} WHERE match_id = ?", (match_id,))
        except sqlite3.OperationalError:
            pass  # table may not exist yet on first file
    try:
        conn.commit()
    except Exception:
        pass


def _log_failure(conn: sqlite3.Connection, file_name: str, error: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO ingestion_log (file_name, match_id, ingested_at, status) "
        "VALUES (?, NULL, ?, 'failed')",
        (file_name, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    print(f"  [FAILED] {file_name}: {error}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Core orchestrator
# ---------------------------------------------------------------------------

def ingest_all(
    bronze_dir: Path = BRONZE_DIR,
    db_path: Path = SILVER_DB,
    adapter: Optional[SourceAdapter] = None,
    log_every: int = 100,
) -> None:
    if adapter is None:
        adapter = CricksheetAdapter()

    conn = _init_db(db_path)
    processed = _load_processed(conn)

    json_files = sorted(bronze_dir.glob("*.json"))
    total = len(json_files)
    skipped = 0
    success = 0
    failed = 0

    print(f"Found {total} JSON files in {bronze_dir}")
    print(f"Already processed (skipping): {len(processed)}")
    print()

    for i, file_path in enumerate(json_files, start=1):
        file_name = file_path.name

        if file_name in processed:
            skipped += 1
            continue

        match_id = file_path.stem
        try:
            dataframes = adapter.process_file(file_path)
            _write_dataframes(conn, dataframes)
            _log_success(conn, file_name, match_id)
            success += 1
        except Exception as exc:
            # pandas to_sql commits internally, so rollback() is a no-op.
            # Compensating deletes remove any partial rows already committed.
            _delete_match_data(conn, match_id)
            _log_failure(conn, file_name, str(exc))
            failed += 1

        if (success + failed) % log_every == 0:
            print(f"  Progress: {i}/{total} files seen | {success} ok | {failed} failed")

    print()
    print(f"Done. {success} ingested, {skipped} skipped, {failed} failed.")

    conn.close()


# ---------------------------------------------------------------------------
# Verification report
# ---------------------------------------------------------------------------

def verify(db_path: Path = SILVER_DB) -> None:
    conn = sqlite3.connect(db_path)
    print("\n=== Verification Report ===\n")

    # Row counts
    print("Row counts:")
    for table in (*TABLE_NAMES, "ingestion_log"):
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<20} {count:>10,}")
        except sqlite3.OperationalError:
            print(f"  {table:<20} {'N/A (table missing)':>10}")

    # File coverage
    bronze_count = len(list(BRONZE_DIR.glob("*.json")))
    success_count = conn.execute(
        "SELECT COUNT(*) FROM ingestion_log WHERE status = 'success'"
    ).fetchone()[0]
    print(f"\nFile coverage: {success_count}/{bronze_count} files successfully ingested")

    # Skip deeper checks if deliveries table doesn't exist yet
    try:
        conn.execute("SELECT 1 FROM deliveries LIMIT 1")
    except sqlite3.OperationalError:
        print("\nSkipping data quality checks — deliveries table not yet created.")
        conn.close()
        return

    # Null check on key columns
    print("\nNull checks (key columns):")
    null_checks = {
        "deliveries.match_id": "SELECT COUNT(*) FROM deliveries WHERE match_id IS NULL",
        "deliveries.batter":   "SELECT COUNT(*) FROM deliveries WHERE batter IS NULL",
        "deliveries.bowler":   "SELECT COUNT(*) FROM deliveries WHERE bowler IS NULL",
        "deliveries.over":     "SELECT COUNT(*) FROM deliveries WHERE \"over\" IS NULL",
        "deliveries.ball":     "SELECT COUNT(*) FROM deliveries WHERE ball IS NULL",
    }
    all_ok = True
    for label, query in null_checks.items():
        nulls = conn.execute(query).fetchone()[0]
        status = "OK" if nulls == 0 else f"WARNING: {nulls} nulls"
        print(f"  {label:<35} {status}")
        if nulls > 0:
            all_ok = False

    # Grain check: (match_id, innings_number, over, ball) unique in deliveries
    print("\nGrain check (deliveries primary key uniqueness):")
    dup_count = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT match_id, innings_number, "over", ball, COUNT(*) AS cnt
            FROM deliveries
            GROUP BY match_id, innings_number, "over", ball
            HAVING cnt > 1
        )
    """).fetchone()[0]
    if dup_count == 0:
        print("  OK — no duplicate (match_id, innings_number, over, ball) rows")
    else:
        print(f"  WARNING: {dup_count} duplicate grain combinations found")
        all_ok = False

    # FK check: every match_id in deliveries exists in matches
    print("\nFK check (deliveries.match_id -> matches.match_id):")
    orphan_count = conn.execute("""
        SELECT COUNT(DISTINCT d.match_id)
        FROM deliveries d
        LEFT JOIN matches m ON d.match_id = m.match_id
        WHERE m.match_id IS NULL
    """).fetchone()[0]
    if orphan_count == 0:
        print("  OK — all delivery match_ids exist in matches")
    else:
        print(f"  WARNING: {orphan_count} match_ids in deliveries not found in matches")
        all_ok = False

    # Failures list
    failures = conn.execute(
        "SELECT file_name FROM ingestion_log WHERE status = 'failed'"
    ).fetchall()
    print(f"\nFailed files: {len(failures)}")
    for (fname,) in failures:
        print(f"  - {fname}")

    print("\n" + ("All checks passed." if all_ok else "Some checks have warnings — review above."))
    conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ingest_all()
    verify()
