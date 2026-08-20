import sys
from pathlib import Path

# Allow importing from src/ without installing the package
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from analyzers.bowler_setup import get_bowler_setup


def test_returns_one_result_per_non_runout_dismissal(seeded_db):
    results = get_bowler_setup(seeded_db, "A Batter")
    assert len(results) == 1
    assert results[0]["dismissal_kind"] == "bowled"


def test_preceding_deliveries_order_and_count(seeded_db):
    results = get_bowler_setup(seeded_db, "A Batter", last_n_balls=12)
    deliveries = results[0]["preceding_deliveries"]
    # 15 total deliveries faced, capped at 12
    assert len(deliveries) == 12
    # Verify ascending order
    pairs = [(d["over"], d["ball"]) for d in deliveries]
    assert pairs == sorted(pairs)


def test_is_dismissal_ball_flag(seeded_db):
    results = get_bowler_setup(seeded_db, "A Batter")
    deliveries = results[0]["preceding_deliveries"]
    # Only the last delivery should be flagged
    flags = [d["is_dismissal_ball"] for d in deliveries]
    assert flags[-1] is True
    assert all(not f for f in flags[:-1])


def test_match_id_filter(seeded_db):
    results_m1 = get_bowler_setup(seeded_db, "A Batter", match_id="m1")
    assert len(results_m1) == 1

    results_unknown = get_bowler_setup(seeded_db, "A Batter", match_id="m999")
    assert results_unknown == []


def test_empty_result_for_unknown_batter(seeded_db):
    results = get_bowler_setup(seeded_db, "Z Unknown")
    assert results == []


def test_last_n_balls_default_and_custom(seeded_db):
    results_12 = get_bowler_setup(seeded_db, "A Batter", last_n_balls=12)
    assert len(results_12[0]["preceding_deliveries"]) == 12

    results_6 = get_bowler_setup(seeded_db, "A Batter", last_n_balls=6)
    assert len(results_6[0]["preceding_deliveries"]) == 6
