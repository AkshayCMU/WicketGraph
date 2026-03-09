"""
Phase 1 ingestion layer: parse a single Cricsheet IPL JSON file into 5
normalized Pandas DataFrames (matches, innings, deliveries, wickets, extras).
"""

import json
from pathlib import Path

import pandas as pd

BRONZE_DIR = Path(__file__).parent.parent / "data" / "01_bronze_cricsheet"

# ---------------------------------------------------------------------------
# Schema stubs – returned when a match has no rows for a given table
# ---------------------------------------------------------------------------

MATCHES_COLS = ["match_id", "city", "venue", "date", "winner", "player_of_match"]
INNINGS_COLS = ["match_id", "innings_number", "team"]
DELIVERIES_COLS = [
    "match_id", "innings_number", "over", "ball",
    "batter", "bowler", "non_striker",
    "runs_batter", "runs_extras", "runs_total",
]
WICKETS_COLS = ["match_id", "innings_number", "over", "ball", "player_out", "kind", "fielders"]
EXTRAS_COLS = ["match_id", "innings_number", "over", "ball", "type", "runs"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _empty(cols: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=cols)


def _parse_match(match_id: str, info: dict) -> pd.DataFrame:
    dates = info.get("dates", [])
    date_str = dates[0] if dates else None

    pom_list = info.get("player_of_match", [])
    player_of_match = ", ".join(pom_list) if pom_list else None

    outcome = info.get("outcome", {})
    winner = outcome.get("winner")

    row = {
        "match_id": match_id,
        "city": info.get("city"),
        "venue": info.get("venue"),
        "date": date_str,
        "winner": winner,
        "player_of_match": player_of_match,
    }
    return pd.DataFrame([row], columns=MATCHES_COLS)


def _parse_innings(match_id: str, innings_list: list) -> pd.DataFrame:
    rows = []
    for idx, inning in enumerate(innings_list, start=1):
        rows.append({
            "match_id": match_id,
            "innings_number": idx,
            "team": inning.get("team"),
        })
    if not rows:
        return _empty(INNINGS_COLS)
    return pd.DataFrame(rows, columns=INNINGS_COLS)


def _parse_deliveries_wickets_extras(
    match_id: str, innings_list: list
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    delivery_rows = []
    wicket_rows = []
    extras_rows = []

    for inn_idx, inning in enumerate(innings_list, start=1):
        for over_obj in inning.get("overs", []):
            over_num = int(over_obj.get("over", 0))
            for ball_idx, ball in enumerate(over_obj.get("deliveries", []), start=1):
                runs = ball.get("runs", {})
                key = (match_id, inn_idx, over_num, ball_idx)

                delivery_rows.append({
                    "match_id": match_id,
                    "innings_number": inn_idx,
                    "over": over_num,
                    "ball": ball_idx,
                    "batter": ball.get("batter"),
                    "bowler": ball.get("bowler"),
                    "non_striker": ball.get("non_striker"),
                    "runs_batter": runs.get("batter", 0),
                    "runs_extras": runs.get("extras", 0),
                    "runs_total": runs.get("total", 0),
                })

                # Extras: one row per extra type on this delivery
                for extra_type, extra_runs in ball.get("extras", {}).items():
                    extras_rows.append({
                        "match_id": match_id,
                        "innings_number": inn_idx,
                        "over": over_num,
                        "ball": ball_idx,
                        "type": extra_type,
                        "runs": extra_runs,
                    })

                # Wickets: one row per wicket on this delivery (rare but possible)
                for wicket in ball.get("wickets", []):
                    fielders = [
                        f.get("name") for f in wicket.get("fielders", []) if f.get("name")
                    ]
                    wicket_rows.append({
                        "match_id": match_id,
                        "innings_number": inn_idx,
                        "over": over_num,
                        "ball": ball_idx,
                        "player_out": wicket.get("player_out"),
                        "kind": wicket.get("kind"),
                        "fielders": fielders,
                    })

    deliveries = pd.DataFrame(delivery_rows, columns=DELIVERIES_COLS) if delivery_rows else _empty(DELIVERIES_COLS)
    wickets = pd.DataFrame(wicket_rows, columns=WICKETS_COLS) if wicket_rows else _empty(WICKETS_COLS)
    extras = pd.DataFrame(extras_rows, columns=EXTRAS_COLS) if extras_rows else _empty(EXTRAS_COLS)
    return deliveries, wickets, extras


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def process_match(file_path: str | Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Parse a single Cricsheet JSON file.

    Returns:
        (matches, innings, deliveries, wickets, extras) as Pandas DataFrames.
    """
    file_path = Path(file_path)
    match_id = file_path.stem

    with open(file_path, encoding="utf-8") as fh:
        data = json.load(fh)

    info = data.get("info", {})
    innings_list = data.get("innings", [])

    matches = _parse_match(match_id, info)
    innings = _parse_innings(match_id, innings_list)
    deliveries, wickets, extras = _parse_deliveries_wickets_extras(match_id, innings_list)

    return matches, innings, deliveries, wickets, extras


# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    sample = next(BRONZE_DIR.glob("*.json"), None)
    if sample is None:
        raise FileNotFoundError(f"No JSON files found in {BRONZE_DIR}")

    print(f"Processing: {sample.name}\n")
    matches, innings, deliveries, wickets, extras = process_match(sample)

    for name, df in [
        ("matches", matches),
        ("innings", innings),
        ("deliveries", deliveries),
        ("wickets", wickets),
        ("extras", extras),
    ]:
        print(f"-- {name} {df.shape} --")
        print(df.head().to_string(index=False))
        print()
