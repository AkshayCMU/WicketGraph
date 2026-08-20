"""Shared SQLite access helpers and silver-layer index definitions."""

import sqlite3
from pathlib import Path

SILVER_DB = Path(__file__).parent.parent / "data" / "02_silver_tables" / "silver.db"


def get_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Silver layer indexes
#
# The five analytical tables are created implicitly by pandas.DataFrame.to_sql(),
# which emits no keys and no indexes. Every analyzer query in src/analyzer.py
# relies on two access patterns that are unusable without them:
#
#   1. Legal-ball recovery — a correlated
#      `NOT EXISTS (SELECT 1 FROM extras e WHERE e.<4-col key> = d.<4-col key>
#                   AND e.type = 'wides')`
#      executed once per candidate delivery. idx_extras_key covers all five
#      referenced columns, so this becomes an index-only probe.
#
#   2. Wicket attribution — `wickets JOIN deliveries` on the full 4-column
#      grain key, needed because the wickets table carries no bowler.
#
# Measured on the full silver layer (278,205 deliveries / 15,161 extras),
# counting V Kohli's legal deliveries:
#     without indexes : 20.17 s
#     with indexes    :  0.24 s   (~83x)
# ---------------------------------------------------------------------------

SILVER_INDEXES: tuple[str, ...] = (
    # Delivery grain key — target of the wickets/extras joins.
    'CREATE INDEX IF NOT EXISTS idx_deliveries_key '
    'ON deliveries(match_id, innings_number, "over", ball)',
    # Player lookups drive every batter/bowler analyzer entry point.
    "CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries(batter)",
    "CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries(bowler)",
    # Per-over aggregation for match momentum.
    'CREATE INDEX IF NOT EXISTS idx_deliveries_match '
    'ON deliveries(match_id, innings_number, "over")',
    # Covering index for the wides NOT EXISTS probe (includes `type`).
    'CREATE INDEX IF NOT EXISTS idx_extras_key '
    'ON extras(match_id, innings_number, "over", ball, type)',
    'CREATE INDEX IF NOT EXISTS idx_wickets_key '
    'ON wickets(match_id, innings_number, "over", ball)',
    "CREATE INDEX IF NOT EXISTS idx_wickets_player ON wickets(player_out)",
    "CREATE INDEX IF NOT EXISTS idx_innings_match ON innings(match_id)",
    "CREATE INDEX IF NOT EXISTS idx_matches_id ON matches(match_id)",
)


def ensure_indexes(conn: sqlite3.Connection) -> list[str]:
    """Create every silver-layer index that does not already exist.

    Idempotent — safe to call on every ingest run. Also refreshes SQLite's
    query planner statistics via ANALYZE.

    Args:
        conn: An open connection to silver.db.

    Returns:
        The list of DDL statements that were executed.
    """
    applied: list[str] = []
    for ddl in SILVER_INDEXES:
        try:
            conn.execute(ddl)
            applied.append(ddl)
        except sqlite3.OperationalError:
            # Table does not exist yet (e.g. an ingest run that wrote nothing).
            continue
    if applied:
        conn.execute("ANALYZE")
        conn.commit()
    return applied
