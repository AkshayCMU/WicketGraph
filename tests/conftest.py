import sqlite3
import pytest
from pathlib import Path


@pytest.fixture()
def seeded_db(tmp_path) -> Path:
    db_path = tmp_path / "test_silver.db"
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE matches (match_id TEXT, city TEXT, venue TEXT,
                              date TEXT, winner TEXT, player_of_match TEXT);
        CREATE TABLE innings (match_id TEXT, innings_number INTEGER, team TEXT);
        CREATE TABLE deliveries (match_id TEXT, innings_number INTEGER,
                                 over INTEGER, ball INTEGER, batter TEXT,
                                 bowler TEXT, non_striker TEXT,
                                 runs_batter INTEGER, runs_extras INTEGER,
                                 runs_total INTEGER);
        CREATE TABLE wickets (match_id TEXT, innings_number INTEGER,
                              over INTEGER, ball INTEGER, player_out TEXT,
                              kind TEXT, fielders TEXT);
        CREATE TABLE extras (match_id TEXT, innings_number INTEGER,
                             over INTEGER, ball INTEGER, type TEXT, runs INTEGER);
    """)

    # 15 deliveries across 3 overs, batter "A Batter" vs "B Bowler"
    conn.executemany(
        "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)",
        [
            ("m1", 1, over, ball, "A Batter", "B Bowler", "C Player", r, 0, r)
            for over, ball, r in [
                (0, 1, 1), (0, 2, 0), (0, 3, 4), (0, 4, 0), (0, 5, 1), (0, 6, 0),
                (1, 1, 0), (1, 2, 6), (1, 3, 0), (1, 4, 1), (1, 5, 0), (1, 6, 0),
                (2, 1, 1), (2, 2, 0), (2, 3, 0),
            ]
        ],
    )
    # Bowled dismissal at over=2, ball=3
    conn.execute("INSERT INTO wickets VALUES ('m1',1,2,3,'A Batter','bowled','[]')")

    # Run-out — should be excluded; add extra deliveries and wicket
    conn.executemany(
        "INSERT INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)",
        [("m1", 1, 3, b, "A Batter", "X Bowler", "C Player", 0, 0, 0) for b in [1, 2]],
    )
    conn.execute("INSERT INTO wickets VALUES ('m1',1,3,2,'A Batter','run out','[]')")

    conn.commit()
    conn.close()
    return db_path
