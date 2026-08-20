from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from langgraph_agent import run_pre_match_briefing

TOOL_DESCRIPTIONS: dict[str, str] = {
    "resolve_player": "Resolve a loosely-typed player name to its exact silver-layer spelling.",
    "head_to_head": "Head-to-head batting summary between a batter and bowler.",
    "batter_vs_bowling_type": "Batting performance split by pace vs spin and over phase.",
    "bowler_wicket_profile": "Bowler wicket breakdown by phase and dismissal type.",
    "bowler_setup_sequence": "Delivery setup pattern before each wicket or dismissal.",
    "match_momentum": "Over-by-over momentum and wicket swings within a match.",
    "milestone_tracker": "Runs progress toward a milestone in a specific contextual filter.",
    "batter_career_stats": "Career batting summary across all IPL appearances.",
    "batter_phase_split": "Batting performance by powerplay, middle, and death phases.",
}


class BriefingRequest(BaseModel):
    question: str = Field(..., min_length=1)


app = FastAPI(title="WicketGraph API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/tools")
def tools() -> list[dict[str, str]]:
    return [
        {"name": name, "description": description}
        for name, description in TOOL_DESCRIPTIONS.items()
    ]


@app.post("/briefing")
def briefing(payload: BriefingRequest) -> dict:
    question = payload.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty.")

    try:
        return run_pre_match_briefing(question)
    except Exception as exc:  # pragma: no cover - exercised at runtime, not unit tested here
        raise HTTPException(status_code=500, detail=str(exc)) from exc
