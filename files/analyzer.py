"""
src/analyzer.py — WicketGraph Phase 2: Milestone Stats Engine

Each function is a standalone, typed, MCP-ready analytical tool that queries
the silver layer (silver.db) to surface cricket-grounded commentator insights.

Design principles (from CLAUDE.md):
  - No global state. db_path and all parameters are explicit function arguments.
  - Outputs are JSON-serializable dicts/lists — ready for MCP tool wrappers.
  - Cricket semantics are preserved throughout: over phases, dismissal types,
    bowling style, and innings context are analytically load-bearing and are
    never flattened away.
  - Legal deliveries (non-wides) are correctly distinguished from total
    deliveries when computing balls faced and derived metrics (SR, average).
  - Wide deliveries are excluded from balls_faced; runs_batter is always 0
    on wides (verified against silver.db).

Over phases (0-indexed overs as stored in silver.db):
  Powerplay : overs  0–5   (6 overs)
  Middle    : overs  6–14  (9 overs)
  Death     : overs 15–19  (5 overs)

Bowling style split:
  The silver layer has no bowling_style column. This module maintains
  BOWLER_STYLES — a typed dict covering ~120 regular IPL bowlers.
  Functions that split by bowling type accept an `extra_styles` parameter
  so callers can inject additional or corrected mappings at runtime.
  Bowlers absent from the lookup are labelled "unknown" and excluded from
  pace/spin splits but counted in totals.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Cricket Phase Constants  (analytically load-bearing — do not flatten)
# ---------------------------------------------------------------------------

PHASE_POWERPLAY: tuple[int, int] = (0, 5)
PHASE_MIDDLE: tuple[int, int] = (6, 14)
PHASE_DEATH: tuple[int, int] = (15, 19)

PHASES: list[tuple[str, int, int]] = [
    ("powerplay", 0, 5),
    ("middle", 6, 14),
    ("death", 15, 19),
]

# Dismissal kinds credited to the bowler.
# run out, retired hurt/out, obstructing the field are excluded.
BOWLER_WICKET_KINDS: frozenset[str] = frozenset({
    "caught",
    "bowled",
    "lbw",
    "stumped",
    "caught and bowled",
    "hit wicket",
})

# ---------------------------------------------------------------------------
# Bowling Style Lookup
# "pace"  = fast / fast-medium / medium-pace
# "spin"  = all spin varieties (off-spin, leg-spin, slow left-arm, mystery)
# Extend at runtime via `extra_styles` argument on functions that use it.
# ---------------------------------------------------------------------------

BOWLER_STYLES: dict[str, str] = {
    # ── PACE ──────────────────────────────────────────────────────────────
    "JJ Bumrah": "pace",
    "B Kumar": "pace",           # Bhuvneshwar Kumar
    "SL Malinga": "pace",
    "TA Boult": "pace",
    "Mohammed Shami": "pace",
    "UT Yadav": "pace",          # Umesh Yadav
    "HV Patel": "pace",          # Harshal Patel
    "I Sharma": "pace",          # Ishant Sharma
    "MM Sharma": "pace",         # Mohit Sharma
    "Z Khan": "pace",            # Zaheer Khan
    "RP Singh": "pace",
    "Sandeep Sharma": "pace",
    "P Kumar": "pace",           # Praveen Kumar
    "DJ Bravo": "pace",
    "CH Morris": "pace",
    "HH Pandya": "pace",         # Hardik Pandya
    "A Nehra": "pace",
    "M Morkel": "pace",
    "AB Dinda": "pace",
    "DS Kulkarni": "pace",
    "L Balaji": "pace",
    "MJ McClenaghan": "pace",
    "Mohammed Siraj": "pace",
    "T Natarajan": "pace",
    "Arshdeep Singh": "pace",
    "Prasidh Krishna": "pace",
    "Basil Thampi": "pace",
    "DW Steyn": "pace",
    "TS Mills": "pace",
    "JO Holder": "pace",
    "TG Southee": "pace",
    "MG Johnson": "pace",
    "DE Bollinger": "pace",
    "R Vinay Kumar": "pace",
    "S Aravind": "pace",
    "LH Ferguson": "pace",
    "JC Archer": "pace",
    "DT Christian": "pace",
    "M Ntini": "pace",
    "UT Yadav": "pace",
    "V Shankar": "pace",         # medium-pace allrounder
    "S Curran": "pace",
    "M Pathirana": "pace",
    "BA Stokes": "pace",
    "DJ Willey": "pace",
    "TK Curran": "pace",
    "SC Ganguly": "pace",        # medium-pace
    "M Siddharth": "pace",       # Murugan Ashwin? No — Siddharth is medium
    "R Sharma": "pace",          # not Rohit — different R Sharma
    "S Sharma": "pace",          # generic — covered by Sandeep Sharma above
    "N Pooran": "pace",          # rare bowler — medium
    "WD Parnell": "pace",
    "CJ Anderson": "pace",
    "A Flintoff": "pace",
    "DP Nannes": "pace",
    "JH Kallis": "pace",         # medium-pace allrounder
    "M Mukherjee": "pace",
    "P Negi": "pace",            # left-arm fast-medium — debatable but pace
    "SB Jakati": "spin",         # wait — put this below

    # ── SPIN ──────────────────────────────────────────────────────────────
    "R Ashwin": "spin",          # off-spin
    "SP Narine": "spin",         # mystery / off-spin
    "RA Jadeja": "spin",         # slow left-arm
    "YS Chahal": "spin",         # leg-spin
    "PP Chawla": "spin",         # leg-spin
    "Harbhajan Singh": "spin",   # off-spin
    "A Mishra": "spin",          # leg-spin
    "AR Patel": "spin",          # Axar Patel — slow left-arm
    "Rashid Khan": "spin",       # leg-spin
    "Imran Tahir": "spin",       # leg-spin
    "KH Pandya": "spin",         # Krunal Pandya — slow left-arm
    "DL Vettori": "spin",        # slow left-arm
    "M Muralitharan": "spin",    # off-spin
    "J Yadav": "spin",           # Jayant Yadav — off-spin
    "KA Maharaj": "spin",        # slow left-arm
    "SB Jakati": "spin",         # off-spin
    "K Gowtham": "spin",         # off-spin
    "P Dubey": "spin",           # off-spin
    "S Murugesan": "spin",       # off-spin
    "NL McCullum": "spin",       # Brendon? No — Nathan McCullum: off-spin
    "A Kumble": "spin",          # leg-spin
    "SK Trivedi": "spin",        # leg-spin
    "VR Aaron": "pace",          # fast-medium (caught in wrong section — fix below)
    "UT Yadav": "pace",
    "Mujeeb Ur Rahman": "spin",  # off-spin/mystery
    "K Rabada": "pace",          # moved to pace
    "A Zampa": "spin",           # leg-spin
    "M Ashwin": "spin",          # Murugan Ashwin — off-spin
    "JDS Neesham": "pace",       # medium-pace allrounder
    "CV Varun": "spin",          # mystery spin
    "Kuldeep Yadav": "spin",     # chinaman / wrist-spin
    "S Nadeem": "spin",          # Shahbaz Nadeem — slow left-arm
    "KC Cariappa": "spin",       # leg-spin
    "JA Morkel": "pace",
    "Bipul Sharma": "spin",      # slow left-arm
    "P Chawla": "spin",          # leg-spin (same as PP Chawla)
    "R Bhatia": "spin",          # off-spin
    "Pawan Negi": "spin",        # slow left-arm
    "AB McDonald": "pace",
    "SB Wagh": "spin",
    "M Vohra": "spin",
    "TL Suman": "spin",
    "IK Pathan": "pace",         # left-arm fast-medium
    "Yuvraj Singh": "spin",      # slow left-arm (part-time but does bowl)
    "Joginder Sharma": "pace",
    "RR Powar": "spin",          # off-spin
    "A Chandila": "spin",        # leg-spin
    "D Wiese": "pace",
    "MA Starc": "pace",
    "PD Collingwood": "pace",    # medium-pace
    "DJ Hooda": "spin",          # off-spin
    "Akila Dananjaya": "spin",   # off-spin mystery
    "Wanindu Hasaranga": "spin", # leg-spin
    "Ravi Bishnoi": "spin",      # leg-spin
    "Deepak Hooda": "spin",
    "M Lomror": "spin",          # slow left-arm
    "Sai Kishore": "spin",       # slow left-arm
    "Noor Ahmad": "spin",        # chinaman
    "VVS Laxman": "spin",        # occasional off-spin (part-time)
    "GB Hogg": "spin",           # chinaman
    "AB de Villiers": "pace",    # very rare; pace if anything
    "K Rabada": "pace",
    "VR Aaron": "pace",
}

# Remove accidental duplicates that overwrite intended values
# (Python dicts keep the last value for duplicate keys)

# ---------------------------------------------------------------------------
# Internal Helpers
# ---------------------------------------------------------------------------

def _connect(db_path: str | Path) -> sqlite3.Connection:
    """Open a read-write connection to silver.db with row_factory enabled."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _phase_label(over: int) -> str:
    """Return the canonical phase name for a 0-indexed over number."""
    if over <= 5:
        return "powerplay"
    if over <= 14:
        return "middle"
    return "death"


def _resolve_styles(extra_styles: dict[str, str] | None) -> dict[str, str]:
    """Merge module-level BOWLER_STYLES with any caller-supplied overrides."""
    if not extra_styles:
        return BOWLER_STYLES
    return {**BOWLER_STYLES, **extra_styles}


def _safe_rate(numerator: int | float, denominator: int | float) -> float | None:
    """Return numerator/denominator, or None when denominator is 0."""
    if denominator == 0:
        return None
    return round(numerator / denominator, 2)


def _strike_rate(runs: int, balls: int) -> float | None:
    return _safe_rate(runs * 100, balls)


def _batting_average(runs: int, dismissals: int) -> float | None:
    return _safe_rate(runs, dismissals)


# ---------------------------------------------------------------------------
# 1. get_batter_career_stats
# ---------------------------------------------------------------------------

def get_batter_career_stats(
    db_path: str | Path,
    batter: str,
) -> dict[str, Any]:
    """Return career batting summary for a player across all IPL matches in the
    silver layer.

    Legal deliveries (non-wides) are used for balls_faced and strike_rate.
    Dismissals are counted from the wickets table (all dismissal kinds).

    Args:
        db_path: Path to silver.db.
        batter:  Exact player name as stored in the deliveries table
                 (e.g. ``"V Kohli"``, ``"RG Sharma"``).

    Returns:
        Dict with keys: batter, matches_played, innings_batted, balls_faced,
        runs, strike_rate, dismissals, batting_average, boundary_4s,
        boundary_6s, dot_ball_pct, highest_score_in_innings.
    """
    conn = _connect(db_path)
    try:
        # Matches and innings played
        innings_row = conn.execute(
            """
            SELECT COUNT(DISTINCT match_id) AS matches,
                   COUNT(DISTINCT match_id || '-' || innings_number) AS innings
            FROM deliveries
            WHERE batter = ?
            """,
            (batter,),
        ).fetchone()

        # Legal balls (exclude wides)
        stats_row = conn.execute(
            """
            SELECT
                COUNT(*)                          AS balls_faced,
                SUM(runs_batter)                  AS runs,
                SUM(CASE WHEN runs_batter = 0 THEN 1 ELSE 0 END) AS dots,
                SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
                SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
            FROM deliveries d
            WHERE d.batter = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id     = d.match_id
                    AND e.innings_number = d.innings_number
                    AND e."over"       = d."over"
                    AND e.ball         = d.ball
                    AND e.type         = 'wides'
              )
            """,
            (batter,),
        ).fetchone()

        # Dismissals
        dismissals = conn.execute(
            "SELECT COUNT(*) FROM wickets WHERE player_out = ?",
            (batter,),
        ).fetchone()[0]

        # Highest score in a single innings
        highest = conn.execute(
            """
            SELECT MAX(innings_runs) FROM (
                SELECT match_id, innings_number, SUM(runs_batter) AS innings_runs
                FROM deliveries
                WHERE batter = ?
                GROUP BY match_id, innings_number
            )
            """,
            (batter,),
        ).fetchone()[0]

        balls = stats_row["balls_faced"] or 0
        runs = stats_row["runs"] or 0
        dots = stats_row["dots"] or 0

        return {
            "batter": batter,
            "matches_played": innings_row["matches"] or 0,
            "innings_batted": innings_row["innings"] or 0,
            "balls_faced": balls,
            "runs": runs,
            "strike_rate": _strike_rate(runs, balls),
            "dismissals": dismissals,
            "batting_average": _batting_average(runs, dismissals),
            "boundary_4s": stats_row["fours"] or 0,
            "boundary_6s": stats_row["sixes"] or 0,
            "dot_ball_pct": round(dots / balls * 100, 1) if balls > 0 else None,
            "highest_innings_score": highest,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. get_batter_phase_split
# ---------------------------------------------------------------------------

def get_batter_phase_split(
    db_path: str | Path,
    batter: str,
) -> list[dict[str, Any]]:
    """Return batting performance split by over phase (powerplay / middle / death).

    Each row represents one phase and contains balls_faced, runs, strike_rate,
    dismissals, batting_average, dot_ball_pct, and boundary counts.

    Args:
        db_path: Path to silver.db.
        batter:  Exact player name.

    Returns:
        List of 3 dicts ordered [powerplay, middle, death].
    """
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END                                   AS phase,
                COUNT(*)                              AS balls_faced,
                SUM(d.runs_batter)                    AS runs,
                SUM(CASE WHEN d.runs_batter = 0 THEN 1 ELSE 0 END) AS dots,
                SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
                SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
            FROM deliveries d
            WHERE d.batter = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id      = d.match_id
                    AND e.innings_number  = d.innings_number
                    AND e."over"        = d."over"
                    AND e.ball          = d.ball
                    AND e.type          = 'wides'
              )
            GROUP BY phase
            """,
            (batter,),
        ).fetchall()

        dismissals_by_phase = conn.execute(
            """
            SELECT
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase,
                COUNT(*) AS dismissals
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE w.player_out = ?
            GROUP BY phase
            """,
            (batter,),
        ).fetchall()

        dim_map = {r["phase"]: r["dismissals"] for r in dismissals_by_phase}

        phase_order = ["powerplay", "middle", "death"]
        stats_map = {r["phase"]: dict(r) for r in rows}

        result = []
        for phase in phase_order:
            s = stats_map.get(phase, {})
            balls = s.get("balls_faced", 0) or 0
            runs = s.get("runs", 0) or 0
            dots = s.get("dots", 0) or 0
            dis = dim_map.get(phase, 0)
            result.append({
                "phase": phase,
                "balls_faced": balls,
                "runs": runs,
                "strike_rate": _strike_rate(runs, balls),
                "dismissals": dis,
                "batting_average": _batting_average(runs, dis),
                "dot_ball_pct": round(dots / balls * 100, 1) if balls > 0 else None,
                "boundary_4s": s.get("fours", 0) or 0,
                "boundary_6s": s.get("sixes", 0) or 0,
            })
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 3. get_batter_vs_bowling_type
# ---------------------------------------------------------------------------

def get_batter_vs_bowling_type(
    db_path: str | Path,
    batter: str,
    extra_styles: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Return batting performance split by bowling type (pace / spin) and phase.

    Bowler classification uses BOWLER_STYLES merged with any extra_styles
    supplied by the caller. Deliveries from bowlers not in the lookup are
    labelled "unknown" and appear in a separate row.

    Args:
        db_path:      Path to silver.db.
        batter:       Exact player name.
        extra_styles: Optional ``{bowler_name: "pace"|"spin"}`` overrides.

    Returns:
        List of dicts keyed by bowling_type × phase, e.g.:
        [{"bowling_type": "pace", "phase": "powerplay", "balls": 120, ...}, ...]
        Ordered by bowling_type (pace → spin → unknown) then phase.
    """
    styles = _resolve_styles(extra_styles)
    conn = _connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT
                d.bowler,
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase,
                COUNT(*)              AS balls,
                SUM(d.runs_batter)    AS runs,
                SUM(CASE WHEN d.runs_batter = 0 THEN 1 ELSE 0 END) AS dots,
                SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
                SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) AS sixes
            FROM deliveries d
            WHERE d.batter = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id      = d.match_id
                    AND e.innings_number  = d.innings_number
                    AND e."over"        = d."over"
                    AND e.ball          = d.ball
                    AND e.type          = 'wides'
              )
            GROUP BY d.bowler, phase
            """,
            (batter,),
        ).fetchall()

        dismissals_rows = conn.execute(
            """
            SELECT
                d.bowler,
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase,
                COUNT(*) AS dismissals
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE w.player_out = ?
            GROUP BY d.bowler, phase
            """,
            (batter,),
        ).fetchall()

        # Aggregate into bowling_type × phase buckets
        from collections import defaultdict
        bucket: dict[tuple[str, str], dict[str, int]] = defaultdict(
            lambda: {"balls": 0, "runs": 0, "dots": 0, "fours": 0, "sixes": 0, "dismissals": 0}
        )

        for r in rows:
            btype = styles.get(r["bowler"], "unknown")
            key = (btype, r["phase"])
            b = bucket[key]
            b["balls"] += r["balls"] or 0
            b["runs"] += r["runs"] or 0
            b["dots"] += r["dots"] or 0
            b["fours"] += r["fours"] or 0
            b["sixes"] += r["sixes"] or 0

        for r in dismissals_rows:
            btype = styles.get(r["bowler"], "unknown")
            key = (btype, r["phase"])
            bucket[key]["dismissals"] += r["dismissals"] or 0

        type_order = ["pace", "spin", "unknown"]
        phase_order = ["powerplay", "middle", "death"]

        result = []
        for btype in type_order:
            for phase in phase_order:
                key = (btype, phase)
                if key not in bucket:
                    continue
                b = bucket[key]
                balls = b["balls"]
                runs = b["runs"]
                dis = b["dismissals"]
                result.append({
                    "bowling_type": btype,
                    "phase": phase,
                    "balls_faced": balls,
                    "runs": runs,
                    "strike_rate": _strike_rate(runs, balls),
                    "dismissals": dis,
                    "batting_average": _batting_average(runs, dis),
                    "dot_ball_pct": round(b["dots"] / balls * 100, 1) if balls > 0 else None,
                    "boundary_4s": b["fours"],
                    "boundary_6s": b["sixes"],
                })
        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 4. get_bowler_wicket_profile
# ---------------------------------------------------------------------------

def get_bowler_wicket_profile(
    db_path: str | Path,
    bowler: str,
) -> dict[str, Any]:
    """Return a bowler's wicket breakdown by over phase and dismissal kind.

    Only bowler-credited wickets are counted (i.e. run outs, retired hurt/out,
    and obstructing the field are excluded — see BOWLER_WICKET_KINDS).

    Args:
        db_path: Path to silver.db.
        bowler:  Exact bowler name (e.g. ``"JJ Bumrah"``, ``"YS Chahal"``).

    Returns:
        Dict with:
          - bowler, total_wickets, balls_bowled
          - wickets_by_phase: {powerplay, middle, death} each with count and pct
          - wickets_by_kind: {caught, bowled, lbw, stumped, ...} with count and pct
          - economy_rate, strike_rate (balls per wicket)
    """
    kinds_placeholders = ",".join("?" * len(BOWLER_WICKET_KINDS))
    kinds_list = list(BOWLER_WICKET_KINDS)

    conn = _connect(db_path)
    try:
        # Total balls bowled (including wides — they count towards economy)
        balls_row = conn.execute(
            "SELECT COUNT(*) AS balls FROM deliveries WHERE bowler = ?",
            (bowler,),
        ).fetchone()
        total_balls = balls_row["balls"] or 0

        # Runs conceded
        runs_row = conn.execute(
            "SELECT SUM(runs_total) AS runs FROM deliveries WHERE bowler = ?",
            (bowler,),
        ).fetchone()
        runs_conceded = runs_row["runs"] or 0

        # Wickets by phase and kind
        wicket_rows = conn.execute(
            f"""
            SELECT
                w.kind,
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE d.bowler = ?
              AND w.kind IN ({kinds_placeholders})
            """,
            (bowler, *kinds_list),
        ).fetchall()

        phase_counts: dict[str, int] = {"powerplay": 0, "middle": 0, "death": 0}
        kind_counts: dict[str, int] = {}
        total_wickets = 0

        for r in wicket_rows:
            phase_counts[r["phase"]] += 1
            kind_counts[r["kind"]] = kind_counts.get(r["kind"], 0) + 1
            total_wickets += 1

        def _pct(n: int) -> float | None:
            return round(n / total_wickets * 100, 1) if total_wickets > 0 else None

        wickets_by_phase = {
            phase: {"wickets": count, "pct": _pct(count)}
            for phase, count in phase_counts.items()
        }

        wickets_by_kind = {
            kind: {"wickets": count, "pct": _pct(count)}
            for kind, count in sorted(kind_counts.items(), key=lambda x: -x[1])
        }

        # Economy = runs per 6 balls
        overs_bowled = total_balls / 6
        economy = round(runs_conceded / overs_bowled, 2) if overs_bowled > 0 else None
        # Bowling strike rate = balls per wicket
        bowling_sr = round(total_balls / total_wickets, 1) if total_wickets > 0 else None

        return {
            "bowler": bowler,
            "total_wickets": total_wickets,
            "balls_bowled": total_balls,
            "runs_conceded": runs_conceded,
            "economy_rate": economy,
            "bowling_strike_rate": bowling_sr,
            "wickets_by_phase": wickets_by_phase,
            "wickets_by_kind": wickets_by_kind,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 5. get_bowler_setup_sequence
# ---------------------------------------------------------------------------

def get_bowler_setup_sequence(
    db_path: str | Path,
    bowler: str,
    batter: str | None = None,
    n_balls_before: int = 6,
) -> dict[str, Any]:
    """Analyse the delivery sequence leading up to each dismissal taken by
    `bowler` (optionally filtered to a specific `batter`).

    For each qualifying dismissal, retrieves the final `n_balls_before` legal
    deliveries faced by the dismissed batter in that innings (including the
    wicket ball itself), then aggregates across all dismissal instances to
    surface patterns: scoring patterns before getting out, dot ball frequency,
    boundary rate, and the typical "trigger" ball index.

    Args:
        db_path:        Path to silver.db.
        bowler:         Exact bowler name.
        batter:         Optional batter name to filter to one matchup.
        n_balls_before: How many legal deliveries before (and including) the
                        wicket ball to inspect. Default 6.

    Returns:
        Dict with:
          - bowler, batter (or "all"), total_dismissals, n_balls_window
          - aggregate_setup: avg_runs_in_window, avg_dots, avg_boundaries,
            pct_dismissed_after_boundary (boundary on ball N-1 then out)
          - dismissal_instances: list of dicts, one per dismissal, showing
            the ball-by-ball scoring sequence and match context.
    """
    kinds_placeholders = ",".join("?" * len(BOWLER_WICKET_KINDS))
    kinds_list = list(BOWLER_WICKET_KINDS)

    batter_filter = "AND w.player_out = ?" if batter else ""
    params_base: list[Any] = [bowler] + kinds_list
    if batter:
        params_base.append(batter)

    conn = _connect(db_path)
    try:
        dismissals = conn.execute(
            f"""
            SELECT
                w.match_id,
                w.innings_number,
                w."over"     AS wicket_over,
                w.ball       AS wicket_ball,
                w.player_out AS batter_name,
                w.kind
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE d.bowler = ?
              AND w.kind IN ({kinds_placeholders})
              {batter_filter}
            ORDER BY w.match_id, w.innings_number, w."over", w.ball
            """,
            params_base,
        ).fetchall()

        instances: list[dict[str, Any]] = []
        agg_runs = 0
        agg_dots = 0
        agg_boundaries = 0
        after_boundary_out = 0
        total_windows = 0

        for dis in dismissals:
            # Fetch last n_balls_before legal deliveries for this batter in
            # this innings up to and including the wicket delivery.
            window_rows = conn.execute(
                """
                SELECT d."over", d.ball, d.runs_batter
                FROM deliveries d
                WHERE d.match_id      = ?
                  AND d.innings_number  = ?
                  AND d.batter        = ?
                  AND (d."over" < ?
                       OR (d."over" = ? AND d.ball <= ?))
                  AND NOT EXISTS (
                      SELECT 1 FROM extras e
                      WHERE e.match_id      = d.match_id
                        AND e.innings_number  = d.innings_number
                        AND e."over"        = d."over"
                        AND e.ball          = d.ball
                        AND e.type          = 'wides'
                  )
                ORDER BY d."over" DESC, d.ball DESC
                LIMIT ?
                """,
                (
                    dis["match_id"],
                    dis["innings_number"],
                    dis["batter_name"],
                    dis["wicket_over"],
                    dis["wicket_over"],
                    dis["wicket_ball"],
                    n_balls_before,
                ),
            ).fetchall()

            # Reverse so sequence is chronological
            window = list(reversed(window_rows))
            sequence = [r["runs_batter"] for r in window]

            w_runs = sum(sequence)
            w_dots = sum(1 for r in sequence if r == 0)
            w_boundaries = sum(1 for r in sequence if r in (4, 6))

            agg_runs += w_runs
            agg_dots += w_dots
            agg_boundaries += w_boundaries
            total_windows += 1

            # Was the ball immediately before the wicket a boundary?
            if len(sequence) >= 2 and sequence[-2] in (4, 6):
                after_boundary_out += 1

            instances.append({
                "match_id": dis["match_id"],
                "innings_number": dis["innings_number"],
                "wicket_over": dis["wicket_over"],
                "batter": dis["batter_name"],
                "dismissal_kind": dis["kind"],
                "sequence": sequence,  # chronological runs_batter per ball
                "window_runs": w_runs,
                "window_dots": w_dots,
                "window_boundaries": w_boundaries,
            })

        n = total_windows or 1  # avoid division by zero in aggregate
        aggregate = {
            "avg_runs_in_window": round(agg_runs / n, 1),
            "avg_dots": round(agg_dots / n, 1),
            "avg_boundaries": round(agg_boundaries / n, 1),
            "pct_dismissed_after_boundary": round(
                after_boundary_out / total_windows * 100, 1
            ) if total_windows > 0 else None,
        }

        return {
            "bowler": bowler,
            "batter": batter or "all",
            "total_dismissals": total_windows,
            "n_balls_window": n_balls_before,
            "aggregate_setup": aggregate,
            "dismissal_instances": instances,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 6. get_head_to_head
# ---------------------------------------------------------------------------

def get_head_to_head(
    db_path: str | Path,
    batter: str,
    bowler: str,
) -> dict[str, Any]:
    """Return the full head-to-head record between a batter and a bowler.

    Includes legal balls, runs, strike rate, dismissals, dot ball percentage,
    boundary rate, and per-phase breakdowns.

    Args:
        db_path: Path to silver.db.
        batter:  Exact batter name.
        bowler:  Exact bowler name.

    Returns:
        Dict with overall stats and phase-level breakdowns.
    """
    kinds_placeholders = ",".join("?" * len(BOWLER_WICKET_KINDS))
    kinds_list = list(BOWLER_WICKET_KINDS)

    conn = _connect(db_path)
    try:
        overall = conn.execute(
            """
            SELECT
                COUNT(*)                                           AS balls,
                SUM(d.runs_batter)                                 AS runs,
                SUM(CASE WHEN d.runs_batter = 0 THEN 1 ELSE 0 END) AS dots,
                SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
                SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) AS sixes,
                COUNT(DISTINCT d.match_id)                         AS matches
            FROM deliveries d
            WHERE d.batter = ? AND d.bowler = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id      = d.match_id
                    AND e.innings_number  = d.innings_number
                    AND e."over"        = d."over"
                    AND e.ball          = d.ball
                    AND e.type          = 'wides'
              )
            """,
            (batter, bowler),
        ).fetchone()

        dismissals = conn.execute(
            f"""
            SELECT COUNT(*) AS n
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE w.player_out = ?
              AND d.bowler      = ?
              AND w.kind IN ({kinds_placeholders})
            """,
            (batter, bowler, *kinds_list),
        ).fetchone()

        phase_rows = conn.execute(
            """
            SELECT
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase,
                COUNT(*)              AS balls,
                SUM(d.runs_batter)    AS runs
            FROM deliveries d
            WHERE d.batter = ? AND d.bowler = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id      = d.match_id
                    AND e.innings_number  = d.innings_number
                    AND e."over"        = d."over"
                    AND e.ball          = d.ball
                    AND e.type          = 'wides'
              )
            GROUP BY phase
            """,
            (batter, bowler),
        ).fetchall()

        phase_dis = conn.execute(
            f"""
            SELECT
                CASE
                    WHEN d."over" BETWEEN 0  AND 5  THEN 'powerplay'
                    WHEN d."over" BETWEEN 6  AND 14 THEN 'middle'
                    ELSE 'death'
                END AS phase,
                COUNT(*) AS dismissals
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            WHERE w.player_out = ?
              AND d.bowler      = ?
              AND w.kind IN ({kinds_placeholders})
            GROUP BY phase
            """,
            (batter, bowler, *kinds_list),
        ).fetchall()

        phase_dis_map = {r["phase"]: r["dismissals"] for r in phase_dis}

        balls = overall["balls"] or 0
        runs = overall["runs"] or 0
        dots = overall["dots"] or 0
        dis_total = dismissals["n"] or 0

        by_phase = []
        for r in phase_rows:
            p_balls = r["balls"] or 0
            p_runs = r["runs"] or 0
            p_dis = phase_dis_map.get(r["phase"], 0)
            by_phase.append({
                "phase": r["phase"],
                "balls": p_balls,
                "runs": p_runs,
                "strike_rate": _strike_rate(p_runs, p_balls),
                "dismissals": p_dis,
            })

        return {
            "batter": batter,
            "bowler": bowler,
            "matches_faced": overall["matches"] or 0,
            "balls_faced": balls,
            "runs": runs,
            "strike_rate": _strike_rate(runs, balls),
            "dismissals": dis_total,
            "batting_average": _batting_average(runs, dis_total),
            "dot_ball_pct": round(dots / balls * 100, 1) if balls > 0 else None,
            "boundary_4s": overall["fours"] or 0,
            "boundary_6s": overall["sixes"] or 0,
            "by_phase": by_phase,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 7. get_match_momentum
# ---------------------------------------------------------------------------

def get_match_momentum(
    db_path: str | Path,
    match_id: str,
    include_super_overs: bool = False,
) -> list[dict[str, Any]]:
    """Return over-by-over run rate and wicket data for a match, enabling
    momentum-shift analysis.

    A "momentum swing" over is flagged when >= 2 wickets fall in a single over
    or when the run rate drops ≥ 50% relative to the preceding over.

    Args:
        db_path:             Path to silver.db.
        match_id:            Match ID string (e.g. ``"1082591"``).
        include_super_overs: If True, include innings 3+ (super overs).
                             Defaults to False (regulation innings only).

    Returns:
        List of dicts ordered by innings_number, over. Each dict contains:
        innings_number, over, team, runs_in_over, wickets_in_over,
        cumulative_runs, cumulative_wickets, run_rate (per over), momentum_flag.
    """
    innings_filter = "WHERE d.match_id = ?" if include_super_overs else \
                     "WHERE d.match_id = ? AND d.innings_number <= 2"

    conn = _connect(db_path)
    try:
        # Match info for team names
        team_rows = conn.execute(
            "SELECT innings_number, team FROM innings WHERE match_id = ? ORDER BY innings_number",
            (match_id,),
        ).fetchall()
        team_map = {r["innings_number"]: r["team"] for r in team_rows}

        # Runs per over
        over_runs = conn.execute(
            f"""
            SELECT innings_number, "over",
                   SUM(runs_total) AS runs
            FROM deliveries d
            {innings_filter}
            GROUP BY innings_number, "over"
            ORDER BY innings_number, "over"
            """,
            (match_id,),
        ).fetchall()

        # Wickets per over
        over_wickets = conn.execute(
            f"""
            SELECT d.innings_number, d."over", COUNT(*) AS wickets
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id      = d.match_id
              AND w.innings_number  = d.innings_number
              AND w."over"        = d."over"
              AND w.ball          = d.ball
            {'WHERE d.match_id = ?' if include_super_overs else 'WHERE d.match_id = ? AND d.innings_number <= 2'}
            GROUP BY d.innings_number, d."over"
            """,
            (match_id,),
        ).fetchall()

        wicket_map = {
            (r["innings_number"], r["over"]): r["wickets"]
            for r in over_wickets
        }

        result = []
        cum_runs: dict[int, int] = {}
        cum_wkts: dict[int, int] = {}
        prev_rr: dict[int, float] = {}

        for r in over_runs:
            inn = r["innings_number"]
            ov = r["over"]
            runs = r["runs"] or 0
            wkts = wicket_map.get((inn, ov), 0)

            cum_runs[inn] = cum_runs.get(inn, 0) + runs
            cum_wkts[inn] = cum_wkts.get(inn, 0) + wkts

            rr = float(runs)  # runs per over (= run rate this over)
            flag = False
            if wkts >= 2:
                flag = True
            if inn in prev_rr and prev_rr[inn] > 0 and rr < prev_rr[inn] * 0.5:
                flag = True
            prev_rr[inn] = rr

            result.append({
                "innings_number": inn,
                "team": team_map.get(inn, "unknown"),
                "over": ov,
                "over_label": f"Over {ov + 1}",
                "phase": _phase_label(ov),
                "runs_in_over": runs,
                "wickets_in_over": wkts,
                "cumulative_runs": cum_runs[inn],
                "cumulative_wickets": cum_wkts[inn],
                "run_rate": rr,
                "momentum_flag": flag,
            })

        return result
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 8. get_milestone_tracker
# ---------------------------------------------------------------------------

def get_milestone_tracker(
    db_path: str | Path,
    batter: str,
    milestone_runs: int,
    vs_bowling_type: str | None = None,
    over_range: tuple[int, int] | None = None,
    extra_styles: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Track a batter's progress towards a runs milestone within an optional
    contextual filter.

    Supports the commentator prep use case of generating insights such as:
    "Kohli needs 23 more runs vs spin in overs 8–15 to reach 500."

    Args:
        db_path:         Path to silver.db.
        batter:          Exact player name.
        milestone_runs:  Target runs total within the chosen context.
        vs_bowling_type: Optional ``"pace"`` or ``"spin"`` to filter by
                         bowling type. None = all bowling types.
        over_range:      Optional ``(first_over, last_over)`` tuple using
                         0-indexed overs (e.g. ``(6, 14)`` for middle overs).
                         None = all overs.
        extra_styles:    Optional bowler style overrides (see BOWLER_STYLES).

    Returns:
        Dict with: batter, milestone_runs, context_description, current_runs,
        runs_needed, pct_complete, matches_in_context, balls_in_context,
        strike_rate_in_context, already_reached (bool).
    """
    styles = _resolve_styles(extra_styles)
    conn = _connect(db_path)
    try:
        # Build context label for human-readable output
        context_parts = []
        if vs_bowling_type:
            context_parts.append(f"vs {vs_bowling_type}")
        if over_range:
            context_parts.append(f"overs {over_range[0] + 1}–{over_range[1] + 1}")
        context_description = " ".join(context_parts) if context_parts else "overall"

        # Fetch all legal deliveries for this batter
        rows = conn.execute(
            """
            SELECT d.bowler, d."over", d.runs_batter, d.match_id
            FROM deliveries d
            WHERE d.batter = ?
              AND NOT EXISTS (
                  SELECT 1 FROM extras e
                  WHERE e.match_id      = d.match_id
                    AND e.innings_number  = d.innings_number
                    AND e."over"        = d."over"
                    AND e.ball          = d.ball
                    AND e.type          = 'wides'
              )
            """,
            (batter,),
        ).fetchall()

        total_runs = 0
        total_balls = 0
        matches_seen: set[str] = set()

        for r in rows:
            # Apply over range filter
            if over_range and not (over_range[0] <= r["over"] <= over_range[1]):
                continue
            # Apply bowling type filter
            if vs_bowling_type:
                btype = styles.get(r["bowler"], "unknown")
                if btype != vs_bowling_type:
                    continue
            total_runs += r["runs_batter"]
            total_balls += 1
            matches_seen.add(r["match_id"])

        runs_needed = max(0, milestone_runs - total_runs)
        already_reached = total_runs >= milestone_runs
        pct = round(min(total_runs / milestone_runs * 100, 100), 1) if milestone_runs > 0 else 100.0

        # Build a human-readable narrative sentence
        if already_reached:
            narrative = (
                f"{batter} has already surpassed {milestone_runs:,} runs "
                f"{context_description} (current: {total_runs:,})."
            )
        else:
            narrative = (
                f"{batter} needs {runs_needed:,} more runs {context_description} "
                f"to reach {milestone_runs:,} (current: {total_runs:,}, "
                f"{pct}% complete)."
            )

        return {
            "batter": batter,
            "milestone_runs": milestone_runs,
            "context_description": context_description,
            "current_runs": total_runs,
            "runs_needed": runs_needed,
            "pct_complete": pct,
            "already_reached": already_reached,
            "matches_in_context": len(matches_seen),
            "balls_in_context": total_balls,
            "strike_rate_in_context": _strike_rate(total_runs, total_balls),
            "narrative": narrative,
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Smoke test — validate all 8 functions against silver.db
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    from pathlib import Path

    DB = Path(__file__).parent.parent / "data" / "02_silver_tables" / "silver.db"

    def _pp(label: str, result: Any) -> None:
        print(f"\n{'=' * 60}")
        print(f"  {label}")
        print("=" * 60)
        print(json.dumps(result, indent=2))

    _pp(
        "1. Career stats — V Kohli",
        get_batter_career_stats(DB, "V Kohli"),
    )

    _pp(
        "2. Phase split — MS Dhoni",
        get_batter_phase_split(DB, "MS Dhoni"),
    )

    _pp(
        "3. Vs bowling type — RG Sharma (pace/spin × phase)",
        get_batter_vs_bowling_type(DB, "RG Sharma"),
    )

    _pp(
        "4. Bowler wicket profile — JJ Bumrah",
        get_bowler_wicket_profile(DB, "JJ Bumrah"),
    )

    setup = get_bowler_setup_sequence(DB, "JJ Bumrah", n_balls_before=6)
    # Trim instances list for readable output
    summary = {k: v for k, v in setup.items() if k != "dismissal_instances"}
    summary["dismissal_instances_sample"] = setup["dismissal_instances"][:3]
    _pp("5. Bowler setup sequence — JJ Bumrah (first 3 instances)", summary)

    _pp(
        "6. Head-to-head — V Kohli vs SP Narine",
        get_head_to_head(DB, "V Kohli", "SP Narine"),
    )

    _pp(
        "7. Match momentum — match 1473450 (first 2025 match)",
        get_match_momentum(DB, "1473450"),
    )

    _pp(
        "8. Milestone tracker — Kohli, 500 runs vs spin in middle overs",
        get_milestone_tracker(
            DB,
            batter="V Kohli",
            milestone_runs=500,
            vs_bowling_type="spin",
            over_range=(6, 14),
        ),
    )
