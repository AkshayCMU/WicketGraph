import sys
from pathlib import Path

# Ensure src/ is on the path when run as a script
sys.path.insert(0, str(Path(__file__).parent))

from mcp.server.fastmcp import FastMCP
from analyzers.bowler_setup import get_bowler_setup
from db import SILVER_DB

mcp = FastMCP("wicketgraph")


@mcp.tool()
def bowler_setup(
    batter: str,
    match_id: str | None = None,
    last_n_balls: int = 12,
) -> list[dict]:
    """
    Return the sequence of deliveries faced by a batter before each dismissal,
    along with dismissal metadata. Excludes run-outs and retired dismissals.

    Args:
        batter:       Cricsheet player name (e.g. "RG Sharma")
        match_id:     Optional match ID to narrow results to one match
        last_n_balls: Number of balls before dismissal to include (default 12)
    """
    return get_bowler_setup(
        db_path=SILVER_DB,
        batter=batter,
        match_id=match_id,
        last_n_balls=last_n_balls,
    )


if __name__ == "__main__":
    mcp.run()
