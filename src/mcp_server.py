"""
src/mcp_server.py — WicketGraph Phase 2b: MCP Tool Server

Registers all 8 analyzer functions from src/analyzer.py as MCP tools.
Runs as a stdio MCP server compatible with Claude Desktop and other MCP clients.

Each tool signature mirrors its underlying analyzer function exactly, so LLM clients
can invoke them without custom glue code.

Usage:
    python src/mcp_server.py

The server will listen for incoming MCP JSON-RPC requests on stdin and respond
on stdout. It is designed to be spawned by MCP clients (e.g. Claude Desktop,
or a LangGraph workflow).
"""

import sys
from pathlib import Path

# Add the src directory to sys.path so we can import analyzer
sys.path.insert(0, str(Path(__file__).parent))

from typing import Any, Optional
from analyzer import (
    resolve_player_name,
    get_batter_career_stats,
    get_batter_phase_split,
    get_batter_vs_bowling_type,
    get_bowler_wicket_profile,
    get_bowler_setup_sequence,
    get_head_to_head,
    get_match_momentum,
    get_milestone_tracker,
)

try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print(
        "Error: mcp[server] is not installed. Please run: pip install mcp[server]",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# MCP Server Setup
# ---------------------------------------------------------------------------

mcp = FastMCP("WicketGraph")

# Database path — assumes src/ is parent of this script
DB_PATH = Path(__file__).parent.parent / "data" / "02_silver_tables" / "silver.db"


# ---------------------------------------------------------------------------
# MCP Tools Registration
# ---------------------------------------------------------------------------


@mcp.tool()
def resolve_player(
    query: str, role: Optional[str] = None, limit: int = 5
) -> dict[str, Any]:
    """Resolve a loosely-typed player name to the exact name stored in silver.db.

    Call this FIRST whenever a player name comes from a human. Every other tool
    matches names exactly, so an unresolved string like "Starc" or "virat kohli"
    returns a silently zeroed result rather than an error. This tool turns those
    into "MA Starc" and "V Kohli".

    Handles surnames alone, full names, nicknames, lowercase, accents, and light
    misspellings. Where a surname is ambiguous (12+ players are named Sharma),
    it disambiguates by given-name initial and then by how many balls the player
    actually faced or bowled.

    Args:
        query: Whatever the user typed, e.g. "Starc", "rohit", "de villiers".
        role: Optional "batter" or "bowler". Narrows candidates to players who
              performed that role, which resolves most ambiguity on its own.
        limit: Maximum alternative candidates to return. Default 5.

    Returns:
        Dict with: query, resolved (exact stored name or None), match_type,
        confidence ("high"|"medium"|"low"|"none"), and candidates — a ranked
        list of {name, match_type, batter_balls, bowler_balls}. When resolved
        is None, show the candidates to the user rather than guessing.
    """
    return resolve_player_name(DB_PATH, query, role, limit)


@mcp.tool()
def batter_career_stats(batter: str) -> dict[str, Any]:
    """Return career batting summary for a player across all IPL matches.

    Args:
        batter: Exact player name as stored in silver.db (e.g. "V Kohli", "RG Sharma").

    Returns:
        Dict with keys: batter, matches_played, innings_batted, balls_faced, runs,
        strike_rate, dismissals, batting_average, boundary_4s, boundary_6s,
        dot_ball_pct, highest_innings_score.
    """
    return get_batter_career_stats(DB_PATH, batter)


@mcp.tool()
def batter_phase_split(batter: str) -> list[dict[str, Any]]:
    """Return batting performance split by over phase (powerplay / middle / death).

    Each row contains statistics broken down by phase, including runs, SR, dismissals,
    dot ball %, and boundary counts.

    Args:
        batter: Exact player name (e.g. "MS Dhoni").

    Returns:
        List of 3 dicts ordered [powerplay, middle, death].
    """
    return get_batter_phase_split(DB_PATH, batter)


@mcp.tool()
def batter_vs_bowling_type(
    batter: str, extra_styles: Optional[dict[str, str]] = None
) -> list[dict[str, Any]]:
    """Return batting performance split by bowling type (pace / spin) and phase.

    Produces a 3×3 grid of pace/spin × powerplay/middle/death statistics,
    plus an "unknown" row for bowlers not in the lookup.

    Args:
        batter: Exact player name.
        extra_styles: Optional {bowler_name: "pace"|"spin"} overrides to the
                      module's built-in BOWLER_STYLES.

    Returns:
        List of dicts keyed by bowling_type × phase, ordered by type then phase.
    """
    return get_batter_vs_bowling_type(DB_PATH, batter, extra_styles)


@mcp.tool()
def bowler_wicket_profile(bowler: str) -> dict[str, Any]:
    """Return a bowler's wicket breakdown by over phase and dismissal kind.

    Only bowler-credited wickets are counted (run outs, retired hurt/out,
    and obstructing the field are excluded).

    Args:
        bowler: Exact bowler name (e.g. "JJ Bumrah", "YS Chahal").

    Returns:
        Dict with: bowler, total_wickets, balls_bowled, runs_conceded,
        economy_rate, bowling_strike_rate, wickets_by_phase,
        wickets_by_kind (each with wickets and pct subkeys).
    """
    return get_bowler_wicket_profile(DB_PATH, bowler)


@mcp.tool()
def bowler_setup_sequence(
    bowler: str, batter: Optional[str] = None, n_balls_before: int = 6
) -> dict[str, Any]:
    """Analyse the delivery sequence leading up to each dismissal taken by a bowler.

    For each dismissal, retrieves the final n_balls_before legal deliveries
    faced by the dismissed batter (including the wicket ball), then aggregates
    across all instances to surface setup patterns and "trigger" ball indices.

    Args:
        bowler: Exact bowler name.
        batter: Optional batter name to filter to one matchup. If None, aggregates
                across all of the bowler's dismissals.
        n_balls_before: How many legal deliveries before (and including) the
                        wicket ball to inspect. Default is 6.

    Returns:
        Dict with: bowler, batter (or "all"), total_dismissals, n_balls_window,
        aggregate_setup (avg_runs_in_window, avg_dots, avg_boundaries,
        pct_dismissed_after_boundary), and dismissal_instances (list of
        dicts showing ball-by-ball sequences and match context).
    """
    return get_bowler_setup_sequence(DB_PATH, bowler, batter, n_balls_before)


@mcp.tool()
def head_to_head(batter: str, bowler: str) -> dict[str, Any]:
    """Return the full head-to-head record between a batter and a bowler.

    Includes legal balls, runs, strike rate, dismissals, dot ball percentage,
    boundary rate, and per-phase breakdowns.

    Args:
        batter: Exact batter name.
        bowler: Exact bowler name.

    Returns:
        Dict with overall stats (balls_faced, runs, strike_rate, dismissals,
        batting_average, dot_ball_pct, boundary_4s, boundary_6s, matches_faced)
        and per-phase breakdown (by_phase list with phase, balls, runs, sr, dis).
    """
    return get_head_to_head(DB_PATH, batter, bowler)


@mcp.tool()
def match_momentum(
    match_id: str, include_super_overs: bool = False
) -> list[dict[str, Any]]:
    """Return over-by-over run rate and wicket data for a match.

    Enables momentum-shift analysis. Flags overs where >= 2 wickets fall or
    where the run rate drops >= 50% relative to the preceding over.

    Args:
        match_id: Match ID string (e.g. "1082591").
        include_super_overs: If True, include innings 3+ (super overs).
                             Default is False (regulation innings only).

    Returns:
        List of dicts ordered by innings_number and over. Each dict contains:
        innings_number, team, over, over_label, phase, runs_in_over,
        wickets_in_over, cumulative_runs, cumulative_wickets, run_rate,
        momentum_flag.
    """
    return get_match_momentum(DB_PATH, match_id, include_super_overs)


@mcp.tool()
def milestone_tracker(
    batter: str,
    milestone_runs: int,
    vs_bowling_type: Optional[str] = None,
    over_range: Optional[tuple[int, int]] = None,
    extra_styles: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Track a batter's progress towards a runs milestone within a contextual filter.

    Supports commentator prep use cases: "Kohli needs 23 more runs vs spin
    in overs 8–15 to reach 500."

    Args:
        batter: Exact player name.
        milestone_runs: Target runs total within the chosen context.
        vs_bowling_type: Optional "pace" or "spin" to filter by bowling type.
                         None = all bowling types.
        over_range: Optional (first_over, last_over) tuple using 0-indexed overs
                    (e.g. (6, 14) for middle overs). None = all overs.
        extra_styles: Optional bowler style overrides.

    Returns:
        Dict with: batter, milestone_runs, context_description, current_runs,
        runs_needed, pct_complete, already_reached, matches_in_context,
        balls_in_context, strike_rate_in_context, narrative (human-readable
        summary sentence).
    """
    return get_milestone_tracker(
        DB_PATH, batter, milestone_runs, vs_bowling_type, over_range, extra_styles
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Verify silver.db exists
    if not DB_PATH.exists():
        print(f"Error: silver.db not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    # Run the MCP server on stdio
    mcp.run()
