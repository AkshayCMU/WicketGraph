from pathlib import Path
from db import get_connection

_EXCLUDED_DISMISSAL_KINDS = ("run out", "retired hurt", "retired out", "obstructing the field")


def get_bowler_setup(
    db_path: Path,
    batter: str,
    match_id: str | None = None,
    last_n_balls: int = 12,
) -> list[dict]:
    """
    Return the sequence of deliveries faced by a batter before each dismissal,
    along with dismissal metadata. Excludes run-outs and retired dismissals.

    Args:
        db_path:      Path to the silver SQLite database.
        batter:       Cricsheet player name (e.g. "RG Sharma").
        match_id:     Optional match ID to narrow results to one match.
        last_n_balls: Number of balls before (and including) the dismissal ball to include.

    Returns:
        List of dicts, one per qualifying dismissal.
    """
    conn = get_connection(db_path)
    try:
        exclusions_placeholder = ",".join("?" * len(_EXCLUDED_DISMISSAL_KINDS))
        dismissal_query = f"""
            SELECT w.match_id, w.innings_number, w.over, w.ball,
                   w.kind, d.bowler
            FROM wickets w
            JOIN deliveries d
              ON  w.match_id       = d.match_id
              AND w.innings_number  = d.innings_number
              AND w.over            = d.over
              AND w.ball            = d.ball
            WHERE w.player_out = ?
              AND w.kind NOT IN ({exclusions_placeholder})
        """
        params: list = [batter, *_EXCLUDED_DISMISSAL_KINDS]

        if match_id is not None:
            dismissal_query += " AND w.match_id = ?"
            params.append(match_id)

        dismissals = conn.execute(dismissal_query, params).fetchall()

        results = []
        for row in dismissals:
            m_id = row["match_id"]
            inn = row["innings_number"]
            d_over = row["over"]
            d_ball = row["ball"]
            kind = row["kind"]
            bowler = row["bowler"]

            deliveries = conn.execute(
                """
                SELECT over, ball, bowler, runs_batter, runs_total
                FROM deliveries
                WHERE match_id       = ?
                  AND innings_number  = ?
                  AND batter          = ?
                ORDER BY over ASC, ball ASC
                """,
                (m_id, inn, batter),
            ).fetchall()

            # Slice to last_n_balls ending at the dismissal delivery
            delivery_dicts = [
                {
                    "over": d["over"],
                    "ball": d["ball"],
                    "bowler": d["bowler"],
                    "runs_batter": d["runs_batter"],
                    "runs_total": d["runs_total"],
                    "is_dismissal_ball": False,
                }
                for d in deliveries
            ]

            # Find the dismissal ball index
            dismissal_idx = next(
                (i for i, d in enumerate(delivery_dicts) if d["over"] == d_over and d["ball"] == d_ball),
                len(delivery_dicts) - 1,
            )

            window = delivery_dicts[max(0, dismissal_idx - last_n_balls + 1): dismissal_idx + 1]
            if window:
                window[-1]["is_dismissal_ball"] = True

            dismissing_bowler_deliveries = [
                d for d in window if d["bowler"] == bowler and not d["is_dismissal_ball"]
            ]

            results.append({
                "match_id": m_id,
                "innings_number": inn,
                "dismissal_over": d_over,
                "dismissal_ball": d_ball,
                "dismissal_kind": kind,
                "dismissing_bowler": bowler,
                "preceding_deliveries": window,
                "balls_by_dismissing_bowler": len(dismissing_bowler_deliveries),
                "runs_off_dismissing_bowler": sum(d["runs_batter"] for d in dismissing_bowler_deliveries),
            })

        return results
    finally:
        conn.close()
