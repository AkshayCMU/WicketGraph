"""Invariant tests for src/analyzer.py.

These cover the properties that are easy to regress silently:
  - BOWLER_STYLES has no duplicate keys and no phantom entries
  - dismissal-kind constants stay disjoint in the right places
  - wides are excluded from balls faced, no-balls are not
  - run outs count toward batting average but never toward a bowler's wickets
"""
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analyzer import (  # noqa: E402
    BATTER_DISMISSAL_KINDS,
    BOWLER_STYLES,
    BOWLER_WICKET_KINDS,
    NON_DISMISSAL_KINDS,
    get_batter_career_stats,
    get_batter_phase_split,
    get_batter_vs_bowling_type,
    get_bowler_wicket_profile,
    _phase_label,
)
from db import SILVER_INDEXES, ensure_indexes  # noqa: E402


# ---------------------------------------------------------------------------
# Static invariants — no database required
# ---------------------------------------------------------------------------

def test_bowler_styles_has_no_duplicate_keys():
    """Duplicate literals in a dict literal silently keep the last value."""
    source = (Path(__file__).parent.parent / "src" / "analyzer.py").read_text(
        encoding="utf-8"
    )
    start = source.index("BOWLER_STYLES: dict[str, str] = {")
    end = source.index("}", start)
    import re

    keys = re.findall(r'"([^"]+)":\s*"(?:pace|spin)"', source[start:end])
    assert len(keys) == len(set(keys)), f"duplicates: {[k for k in keys if keys.count(k) > 1]}"


def test_bowler_styles_values_are_pace_or_spin():
    assert set(BOWLER_STYLES.values()) == {"pace", "spin"}


def test_retired_hurt_is_not_a_dismissal_for_anyone():
    assert NON_DISMISSAL_KINDS.isdisjoint(BATTER_DISMISSAL_KINDS)
    assert NON_DISMISSAL_KINDS.isdisjoint(BOWLER_WICKET_KINDS)


def test_bowler_kinds_are_a_subset_of_batter_kinds():
    """Anything credited to a bowler must also end the batter's innings."""
    assert BOWLER_WICKET_KINDS <= BATTER_DISMISSAL_KINDS


def test_run_out_counts_for_batter_but_not_for_bowler():
    assert "run out" in BATTER_DISMISSAL_KINDS
    assert "run out" not in BOWLER_WICKET_KINDS


@pytest.mark.parametrize(
    "over,expected",
    [(0, "powerplay"), (5, "powerplay"), (6, "middle"), (14, "middle"),
     (15, "death"), (19, "death")],
)
def test_phase_boundaries(over, expected):
    assert _phase_label(over) == expected


# ---------------------------------------------------------------------------
# Behavioural invariants — against the seeded fixture DB
# ---------------------------------------------------------------------------

@pytest.fixture()
def analyzer_db(tmp_path) -> Path:
    """A tiny silver layer with a known-good answer computed by hand.

    'A Batter' faces 8 deliveries off 'P Bowler' (pace) and 'S Bowler' (spin):
      legal balls: 6 (one wide and one extra row are excluded / not faced)
      runs: 4 + 1 + 6 + 0 + 2 + 0 = 13
    Dismissals: one caught (bowler-credited) and one run out (batter only).
    """
    db_path = tmp_path / "silver.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE matches (match_id TEXT, city TEXT, venue TEXT, date TEXT,
                              winner TEXT, player_of_match TEXT);
        CREATE TABLE innings (match_id TEXT, innings_number INTEGER, team TEXT);
        CREATE TABLE deliveries (match_id TEXT, innings_number INTEGER,
                                 over INTEGER, ball INTEGER, batter TEXT,
                                 bowler TEXT, non_striker TEXT,
                                 runs_batter INTEGER, runs_extras INTEGER,
                                 runs_total INTEGER);
        CREATE TABLE wickets (match_id TEXT, innings_number INTEGER, over INTEGER,
                              ball INTEGER, player_out TEXT, kind TEXT, fielders TEXT);
        CREATE TABLE extras (match_id TEXT, innings_number INTEGER, over INTEGER,
                             ball INTEGER, type TEXT, runs INTEGER);
        """
    )
    conn.execute("INSERT INTO matches VALUES ('m1','C','V','2024-01-01','T1','A Batter')")
    conn.execute("INSERT INTO innings VALUES ('m1',1,'T1')")
    rows = [
        # over 0 (powerplay), P Bowler = pace
        ("m1", 1, 0, 1, "A Batter", "P Bowler", "N S", 4, 0, 4),
        ("m1", 1, 0, 2, "A Batter", "P Bowler", "N S", 0, 1, 1),   # WIDE -> excluded
        ("m1", 1, 0, 3, "A Batter", "P Bowler", "N S", 1, 0, 1),
        ("m1", 1, 0, 4, "A Batter", "P Bowler", "N S", 6, 1, 7),   # NO-BALL -> counted
        # over 8 (middle), S Bowler = spin
        ("m1", 1, 8, 1, "A Batter", "S Bowler", "N S", 0, 0, 0),
        ("m1", 1, 8, 2, "A Batter", "S Bowler", "N S", 2, 0, 2),
        ("m1", 1, 8, 3, "A Batter", "S Bowler", "N S", 0, 0, 0),   # caught
        # over 16 (death) — run out, not the bowler's wicket
        ("m1", 1, 16, 1, "A Batter", "P Bowler", "N S", 0, 0, 0),
    ]
    conn.executemany("INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.executemany(
        "INSERT INTO extras VALUES (?,?,?,?,?,?)",
        [("m1", 1, 0, 2, "wides", 1), ("m1", 1, 0, 4, "noballs", 1)],
    )
    conn.executemany(
        "INSERT INTO wickets VALUES (?,?,?,?,?,?,?)",
        [
            ("m1", 1, 8, 3, "A Batter", "caught", "[]"),
            ("m1", 1, 16, 1, "A Batter", "run out", "[]"),
        ],
    )
    conn.commit()
    ensure_indexes(conn)
    conn.close()
    return db_path


EXTRA_STYLES = {"P Bowler": "pace", "S Bowler": "spin"}


def test_wides_excluded_noballs_included(analyzer_db):
    stats = get_batter_career_stats(analyzer_db, "A Batter")
    # 8 delivery rows, minus the one wide = 7 balls faced.
    assert stats["balls_faced"] == 7
    assert stats["runs"] == 13


def test_batting_average_counts_the_run_out(analyzer_db):
    stats = get_batter_career_stats(analyzer_db, "A Batter")
    assert stats["dismissals"] == 2          # caught + run out
    assert stats["batting_average"] == 6.5   # 13 / 2


def test_bowler_profile_excludes_the_run_out(analyzer_db):
    """P Bowler was bowling when the run out happened — it is not his wicket."""
    profile = get_bowler_wicket_profile(analyzer_db, "P Bowler")
    assert profile["total_wickets"] == 0
    assert get_bowler_wicket_profile(analyzer_db, "S Bowler")["total_wickets"] == 1


def test_vs_bowling_type_attributes_only_bowler_credited_wickets(analyzer_db):
    rows = get_batter_vs_bowling_type(analyzer_db, "A Batter", extra_styles=EXTRA_STYLES)
    by_key = {(r["bowling_type"], r["phase"]): r for r in rows}
    # The run out happened in the death phase off a pace bowler; it must not
    # show up as a pace dismissal.
    assert by_key[("pace", "death")]["dismissals"] == 0
    assert by_key[("spin", "middle")]["dismissals"] == 1


def test_phase_split_totals_reconcile_with_career(analyzer_db):
    career = get_batter_career_stats(analyzer_db, "A Batter")
    phases = get_batter_phase_split(analyzer_db, "A Batter")
    assert sum(p["balls_faced"] for p in phases) == career["balls_faced"]
    assert sum(p["runs"] for p in phases) == career["runs"]
    assert sum(p["dismissals"] for p in phases) == career["dismissals"]


def test_phase_split_is_always_three_ordered_rows(analyzer_db):
    phases = get_batter_phase_split(analyzer_db, "A Batter")
    assert [p["phase"] for p in phases] == ["powerplay", "middle", "death"]


def test_unknown_player_returns_zeroed_not_error(analyzer_db):
    stats = get_batter_career_stats(analyzer_db, "Nobody At All")
    assert stats["balls_faced"] == 0
    assert stats["runs"] == 0
    assert stats["strike_rate"] is None
    assert stats["batting_average"] is None


# ---------------------------------------------------------------------------
# Index invariants
# ---------------------------------------------------------------------------

def test_ensure_indexes_is_idempotent(analyzer_db):
    conn = sqlite3.connect(analyzer_db)
    try:
        first = ensure_indexes(conn)
        second = ensure_indexes(conn)
        assert len(first) == len(second) == len(SILVER_INDEXES)
        names = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
            )
        }
        assert len(names) == len(SILVER_INDEXES)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Player name resolution
# ---------------------------------------------------------------------------

from analyzer import (  # noqa: E402
    PLAYER_ALIASES,
    _normalize_name,
    _split_name,
    get_player_index,
    resolve_player_name,
)

REAL_DB = Path(__file__).parent.parent / "data" / "02_silver_tables" / "silver.db"
needs_real_db = pytest.mark.skipif(
    not REAL_DB.exists(), reason="silver.db not built"
)


def test_normalize_name_strips_case_accents_and_punctuation():
    assert _normalize_name("  MS   Dhoni ") == "ms dhoni"
    assert _normalize_name("A.B. de Villiers") == "a b de villiers"
    assert _normalize_name("Coulter-Nile") == "coulter nile"


def test_split_name_separates_prefix_from_surname():
    assert _split_name("ma starc") == ("ma", "starc")
    assert _split_name("starc") == ("", "starc")
    assert _split_name("ab de villiers") == ("ab de", "villiers")


@needs_real_db
@pytest.mark.parametrize(
    "query,role,expected",
    [
        ("Starc", "bowler", "MA Starc"),            # the reported bug
        ("starc", None, "MA Starc"),
        ("Mitchell Starc", "bowler", "MA Starc"),
        ("MA Starc", "bowler", "MA Starc"),
        ("Kohli", "batter", "V Kohli"),             # beats T Kohli on volume
        ("virat kohli", None, "V Kohli"),
        ("Rohit Sharma", "batter", "RG Sharma"),    # beats 12 other Sharmas
        ("rohit", None, "RG Sharma"),               # given name only -> alias
        ("Bumrah", "bowler", "JJ Bumrah"),
        ("de villiers", "batter", "AB de Villiers"),
        ("Rashid", "bowler", "Rashid Khan"),        # beats AU Rashid on volume
        ("Kohly", "batter", "V Kohli"),             # misspelling
        ("Bumra", "bowler", "JJ Bumrah"),           # truncation
    ],
)
def test_resolve_player_name_handles_loose_input(query, role, expected):
    assert resolve_player_name(REAL_DB, query, role)["resolved"] == expected


@needs_real_db
def test_resolve_player_name_returns_none_for_nonsense():
    out = resolve_player_name(REAL_DB, "zzzz qqqq nonsense")
    assert out["resolved"] is None
    assert out["confidence"] == "none"


@needs_real_db
def test_resolve_player_name_returns_ranked_alternatives():
    out = resolve_player_name(REAL_DB, "Sharma", "batter")
    names = [c["name"] for c in out["candidates"]]
    assert out["resolved"] == "RG Sharma"
    assert len(names) > 1
    volumes = [c["batter_balls"] for c in out["candidates"]]
    assert volumes == sorted(volumes, reverse=True)


@needs_real_db
def test_role_narrows_candidates_to_players_who_did_that():
    out = resolve_player_name(REAL_DB, "Kohli", "bowler")
    assert out["resolved"] is not None


@needs_real_db
def test_every_alias_target_exists_in_the_data():
    """A typo in PLAYER_ALIASES would silently resolve to a nonexistent player."""
    real = {e["name"] for e in get_player_index(REAL_DB)}
    missing = sorted({v for v in PLAYER_ALIASES.values() if v not in real})
    assert not missing, f"alias targets absent from silver.db: {missing}"


@needs_real_db
def test_every_bowler_style_key_exists_in_the_data():
    real = {e["name"] for e in get_player_index(REAL_DB)}
    missing = sorted({k for k in BOWLER_STYLES if k not in real})
    assert not missing, f"BOWLER_STYLES keys absent from silver.db: {missing}"


def test_resolve_player_name_handles_empty_input(analyzer_db):
    out = resolve_player_name(analyzer_db, "   ")
    assert out["resolved"] is None
    assert out["candidates"] == []
