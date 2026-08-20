from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency fallback
    def load_dotenv(*args, **kwargs):
        return False

try:
    from langchain_mcp_adapters.client import MultiServerMCPClient
    from langchain_mcp_adapters.sessions import StdioConnection
    from langchain_mcp_adapters.tools import load_mcp_tools

    _MCP_IMPORT_ERROR: str | None = None
except Exception as _exc:  # pragma: no cover - optional dependency fallback
    # Keep the real cause. A version conflict (e.g. mcp>=2.0 removing
    # RequestContext, which langchain-mcp-adapters 0.3.x imports) is NOT the
    # same failure as the package being absent, and reporting it as "not
    # installed" sends you chasing the wrong problem.
    MultiServerMCPClient = None
    StdioConnection = None
    load_mcp_tools = None
    _MCP_IMPORT_ERROR = f"{type(_exc).__name__}: {_exc}"

try:
    from langgraph.graph import END, START, StateGraph
except Exception:  # pragma: no cover - optional dependency fallback
    END = START = StateGraph = None

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def get_model_name() -> str:
    return os.getenv("MODEL_NAME", "gpt-4o-mini")


def get_chat_model():
    provider = os.getenv("MODEL_PROVIDER", "openai").lower()
    if provider == "anthropic":
        try:
            from langchain_anthropic import ChatAnthropic
        except Exception as exc:  # pragma: no cover - optional dependency fallback
            raise RuntimeError("langchain-anthropic is not installed.") from exc
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not set.")
        return ChatAnthropic(model=get_model_name(), temperature=0.2)

    try:
        from langchain_openai import ChatOpenAI
    except Exception as exc:  # pragma: no cover - optional dependency fallback
        raise RuntimeError("langchain-openai is not installed.") from exc
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    return ChatOpenAI(model=get_model_name(), temperature=0.2)


def _normalize_player_name(name: str) -> str:
    if not isinstance(name, str):
        return str(name)
    cleaned = " ".join(name.strip().split())
    if cleaned.lower() in {"kohli", "virat kohli", "vkohli"}:
        return "V Kohli"
    if cleaned.lower() in {"bumrah", "j j bumrah", "jj bumrah", "ja bumrah"}:
        return "JJ Bumrah"
    if cleaned.lower() in {"rohit", "rohit sharma", "rg sharma"}:
        return "RG Sharma"
    return cleaned


def repair_tool_arguments(tool_schema: dict[str, Any] | None, raw_args: dict[str, Any]) -> dict[str, Any]:
    """Coerce tool inputs into the expected schema shape for retry loops."""
    if raw_args is None:
        return {}

    properties = {}
    if tool_schema:
        properties = tool_schema.get("properties", {}) or {}

    repaired: dict[str, Any] = {}
    for key, value in (raw_args or {}).items():
        if value is None:
            continue
        prop = properties.get(key, {})
        prop_type = prop.get("type")
        if isinstance(value, list) and key == "over_range":
            value = tuple(int(v) for v in value)
        if isinstance(value, str):
            value = value.strip()
            if key in {"batter", "bowler"}:
                value = _normalize_player_name(value)
        if prop_type in {"integer", "int"} and not isinstance(value, bool):
            if isinstance(value, str):
                value = value.strip()
                if value:
                    try:
                        value = int(value)
                    except ValueError:
                        pass
            elif isinstance(value, float):
                value = int(value)
        elif prop_type == "number" and not isinstance(value, bool):
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    pass
        elif prop_type == "boolean":
            if isinstance(value, str):
                value = value.lower() in {"true", "1", "yes", "y"}
        elif key in {"batter", "bowler"} and not isinstance(value, str):
            value = str(value)
        repaired[key] = value

    required = []
    if tool_schema:
        required = tool_schema.get("required", []) or []

    for required_key in required:
        if required_key not in repaired and required_key in raw_args:
            repaired[required_key] = raw_args[required_key]

    return repaired


def safe_tool_dispatch(tool_callable: Callable[..., Any], kwargs: dict[str, Any], max_retries: int = 2) -> Any:
    """Call a tool safely, repairing bad arguments and retrying on common type errors.

    LangChain StructuredTool objects do not support sync invocation; they expose
    `ainvoke()` and must be executed via the async path.
    """
    current_kwargs = dict(kwargs or {})
    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
            if hasattr(tool_callable, "ainvoke"):
                async def _invoke_async():
                    return await tool_callable.ainvoke(current_kwargs)

                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    return asyncio.run(_invoke_async())

                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(lambda: asyncio.run(_invoke_async()))
                    return future.result()
            if hasattr(tool_callable, "invoke"):
                try:
                    return tool_callable.invoke(current_kwargs)
                except NotImplementedError:
                    # StructuredTool sync invocation is intentionally unsupported.
                    if hasattr(tool_callable, "ainvoke"):
                        async def _invoke_async_2():
                            return await tool_callable.ainvoke(current_kwargs)

                        try:
                            asyncio.get_running_loop()
                        except RuntimeError:
                            return asyncio.run(_invoke_async_2())
                        with ThreadPoolExecutor(max_workers=1) as pool:
                            future = pool.submit(lambda: asyncio.run(_invoke_async_2()))
                            return future.result()
                    raise
            return tool_callable(**current_kwargs)
        except TypeError as exc:
            last_error = exc
            schema = getattr(tool_callable, "args_schema", None)
            if schema is not None:
                repaired = repair_tool_arguments(schema.schema() if hasattr(schema, "schema") else {}, current_kwargs)
            else:
                repaired = repair_tool_arguments({}, current_kwargs)
            if repaired != current_kwargs:
                current_kwargs = repaired
                continue
            if attempt == max_retries - 1:
                raise
        except ValueError as exc:
            last_error = exc
            if attempt == max_retries - 1:
                raise
        except Exception as exc:  # pragma: no cover - broad safety net for tool errors
            last_error = exc
            if attempt == max_retries - 1:
                raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("Tool dispatch failed without a captured exception.")


def build_pre_match_briefing_card(batter: str, bowler: str, tool_results: list[dict[str, Any]]) -> dict[str, Any]:
    """Compose a structured pre-match briefing card from operational tool results."""
    sections = []
    for entry in tool_results:
        if not isinstance(entry, dict):
            continue
        section = {
            "title": entry.get("title", "Match signal"),
            "summary": entry.get("summary", str(entry)),
        }
        # Carry the structured payload and tool name through to the UI, which
        # renders `data` as stat tiles and phase tables. `summary` stays the
        # flattened form the LLM sees.
        if "tool" in entry:
            section["tool"] = entry["tool"]
        if "data" in entry:
            section["data"] = entry["data"]
        if entry.get("error"):
            section["error"] = True
        sections.append(section)

    headline = f"{batter} vs {bowler}: pre-match briefing"
    summary = (
        f"{batter} and {bowler} are the main matchup in this briefing. "
        f"The analysis combines head-to-head form, bowling patterns, and wicket profiles."
    )

    return {
        "batter": batter,
        "bowler": bowler,
        "headline": headline,
        "summary": summary,
        "sections": sections,
    }


# Words that surround a name in a natural question but are never part of one.
# Stripped from both sides of the "vs" separator before resolution.
_FILLER_WORDS: frozenset[str] = frozenset({
    # request framing
    "brief", "briefing", "me", "on", "about", "tell", "show", "give", "get",
    "how", "does", "do", "did", "what", "whats", "is", "are", "the", "a", "an",
    "for", "in", "of", "to", "and", "with", "please", "can", "you", "i", "want",
    "need", "look", "at", "analyse", "analyze", "compare", "matchup", "match",
    "up", "prematch", "pre", "post", "summary", "report", "stats", "statistics",
    "analysis", "breakdown", "insight", "insights", "data", "numbers",
    # cricket analysis vocabulary that trails a name
    "powerplay", "middle", "death", "over", "overs", "phase", "phases",
    "setup", "pattern", "patterns", "spin", "pace", "seam", "swing",
    "momentum", "milestone", "milestones", "wicket", "wickets", "dismissal",
    "dismissals", "economy", "strike", "rate", "average", "form", "record",
    "records", "career", "history", "head", "h2h", "runs", "balls", "innings",
    "bowling", "batting", "batter", "bowler", "against", "vs", "versus", "v",
})

_SEPARATORS = (" vs ", " v ", " versus ", " against ", " faces ", " facing ")


def _clean_name_fragment(fragment: str) -> str:
    """Strip filler words from a fragment, keeping only plausible name tokens."""
    tokens = re.split(r"[^A-Za-z'\-]+", fragment or "")
    kept = [t for t in tokens if t and t.lower() not in _FILLER_WORDS]
    return " ".join(kept).strip()


def split_question_into_names(question: str) -> tuple[str, str]:
    """Split a free-text question into two raw player-name fragments.

    Returns the fragments as typed — resolution to exact silver-layer spellings
    happens separately, against the database.
    """
    text = " " + (question or "").strip() + " "
    lowered = text.lower()

    cut = -1
    sep_len = 0
    for sep in _SEPARATORS:
        idx = lowered.find(sep)
        if idx != -1 and (cut == -1 or idx < cut):
            cut, sep_len = idx, len(sep)

    if cut == -1:
        # No separator: treat every non-filler token as one name fragment.
        return _clean_name_fragment(text), ""

    return (
        _clean_name_fragment(text[:cut]),
        _clean_name_fragment(text[cut + sep_len:]),
    )


def _question_to_player_pair(question: str) -> tuple[str, str]:
    """Text-only fallback used when the resolver tool is unavailable.

    Prefer _resolve_players(), which matches against the actual player index.
    """
    left, right = split_question_into_names(question)
    left = _normalize_player_name(left) if left else "V Kohli"
    right = _normalize_player_name(right) if right else "JJ Bumrah"
    return left, right


async def _resolve_players(
    question: str, tool_map: dict[str, Any]
) -> tuple[str, str, list[dict[str, Any]]]:
    """Resolve both names in a question against the silver layer via MCP.

    Returns (batter, bowler, resolutions). `resolutions` records what each raw
    fragment mapped to, so the caller can show "Starc -> MA Starc" instead of
    silently substituting a different player.
    """
    raw_batter, raw_bowler = split_question_into_names(question)
    fallback_batter, fallback_bowler = _question_to_player_pair(question)

    resolver = tool_map.get("resolve_player")
    if resolver is None:
        return fallback_batter, fallback_bowler, []

    async def _resolve(raw: str, role: str, fallback: str) -> tuple[str, dict | None]:
        if not raw:
            return fallback, None
        try:
            payload = unwrap_mcp_result(
                await _dispatch_async(resolver, {"query": raw, "role": role})
            )
        except Exception:  # pragma: no cover - resolver is best-effort
            logger.exception("Player resolution failed for %r", raw)
            return fallback, None
        if not isinstance(payload, dict) or not payload.get("resolved"):
            return fallback, {
                "query": raw,
                "role": role,
                "resolved": None,
                "confidence": "none",
                "candidates": (payload or {}).get("candidates", [])
                if isinstance(payload, dict) else [],
            }
        return payload["resolved"], {
            "query": raw,
            "role": role,
            "resolved": payload["resolved"],
            "match_type": payload.get("match_type"),
            "confidence": payload.get("confidence"),
            "candidates": payload.get("candidates", [])[1:4],
        }

    batter, batter_res = await _resolve(raw_batter, "batter", fallback_batter)
    bowler, bowler_res = await _resolve(raw_bowler, "bowler", fallback_bowler)

    return batter, bowler, [r for r in (batter_res, bowler_res) if r]


def _tool_plan_for_question(question: str) -> list[str]:
    q = question.lower()
    plan: list[str] = []

    if any(k in q for k in ["head", "matchup", "versus", "vs"]):
        plan.append("head_to_head")
    if any(k in q for k in ["pace", "spin", "bowling type", "phase", "powerplay", "middle", "death"]):
        plan.append("batter_vs_bowling_type")
    if any(k in q for k in ["wicket", "dismissal", "bowler", "pattern", "setup"]):
        plan.append("bowler_wicket_profile")
    if any(k in q for k in ["setup", "before dismissal", "trigger", "sequence"]):
        plan.append("bowler_setup_sequence")
    if any(k in q for k in ["momentum", "spell", "match shift", "turn"]):
        plan.append("match_momentum")
    if any(k in q for k in ["milestone", "needs", "reaches", "500", "1000", "run target"]):
        plan.append("milestone_tracker")

    if not plan:
        return ["head_to_head", "batter_vs_bowling_type", "bowler_wicket_profile"]

    # Preserve the most relevant three signals.
    deduped = []
    for name in plan:
        if name not in deduped:
            deduped.append(name)
    return deduped[:3]


def _require_mcp() -> None:
    if MultiServerMCPClient is None or StdioConnection is None:
        raise RuntimeError(
            "langchain-mcp-adapters is unavailable. Underlying import error: "
            f"{_MCP_IMPORT_ERROR}. If this mentions RequestContext, the installed "
            "mcp package is >=2.0 — pin it with `pip install 'mcp[cli]<2.0'` "
            "(see requirements.txt)."
        )


def _build_connection(server_path: str | Path | None = None) -> "StdioConnection":
    server_target = server_path or (PROJECT_ROOT / "src" / "mcp_server.py")
    return {
        "transport": "stdio",
        "command": sys.executable,
        "args": [str(server_target)],
        "cwd": str(PROJECT_ROOT),
    }


@asynccontextmanager
async def mcp_tool_session(server_path: str | Path | None = None):
    """Yield MCP tools bound to a single, reused stdio session.

    Tools returned by `client.get_tools()` carry no session, so every
    `.ainvoke()` opens a fresh one — which for a stdio server means spawning
    `python src/mcp_server.py` and re-importing pandas per tool call, about
    10s each. A briefing calls 3-5 tools, so the cost was linear in tool count.

    Holding one session open for the whole request makes that a single spawn.
    """
    _require_mcp()
    client = MultiServerMCPClient(
        {"wicketgraph": _build_connection(server_path)}, handle_tool_errors=True
    )
    async with client.session("wicketgraph") as session:
        yield await load_mcp_tools(session)


async def _discover_mcp_tools(server_path: str | Path | None = None) -> list[Any]:
    """Return session-less tools. Each invocation respawns the server.

    Retained for callers that only need the tool list (schemas, names). Prefer
    mcp_tool_session() whenever tools will actually be invoked.
    """
    _require_mcp()
    client = MultiServerMCPClient(
        {"wicketgraph": _build_connection(server_path)}, handle_tool_errors=True
    )
    return await client.get_tools(server_name="wicketgraph")


async def _dispatch_async(tool_callable: Callable[..., Any], kwargs: dict[str, Any]) -> Any:
    """Invoke a LangChain StructuredTool from inside a running event loop."""
    if hasattr(tool_callable, "ainvoke"):
        return await tool_callable.ainvoke(kwargs)
    return tool_callable(**kwargs)


def unwrap_mcp_result(result: Any) -> Any:
    """Unwrap an MCP tool response into the plain Python object it carries.

    MCP returns content blocks, not bare values: a tool that returns a dict
    arrives as ``[{"type": "text", "text": "<json>"}]`` (or as an object with
    a ``.content`` list, depending on adapter version). Passing that straight
    through stringifies the envelope and truncates the payload mid-JSON, which
    is both unreadable in the UI and lossy for the LLM.

    Falls back to returning the input unchanged when it is not an envelope.
    """
    # Some adapter versions wrap blocks in an object with `.content`.
    content = getattr(result, "content", None)
    if content is not None and not isinstance(result, (dict, list, str)):
        result = content

    if isinstance(result, list) and result:
        texts: list[str] = []
        for block in result:
            if isinstance(block, dict) and block.get("type") == "text":
                texts.append(block.get("text", ""))
            elif getattr(block, "type", None) == "text":
                texts.append(getattr(block, "text", "") or "")
            else:
                return result  # non-text content (image/resource) — leave as-is
        if texts:
            joined = "".join(texts)
            try:
                return json.loads(joined)
            except (ValueError, TypeError):
                pass

            # FastMCP emits ONE text block per element when a tool returns a
            # list, so a 9-row pace/spin split arrives as 9 separate blocks.
            # Concatenating those yields "{...}{...}" — not valid JSON — so
            # parse each block on its own and rebuild the list.
            parsed: list[Any] = []
            for chunk in texts:
                try:
                    parsed.append(json.loads(chunk))
                except (ValueError, TypeError):
                    return joined
            if parsed:
                return parsed if len(parsed) > 1 else parsed[0]
            return joined

    if isinstance(result, str):
        try:
            return json.loads(result)
        except (ValueError, TypeError):
            return result

    return result


async def _tool_result_from_name(tool_name: str, tool_map: dict[str, Any], batter: str, bowler: str) -> dict[str, Any]:
    tool = tool_map.get(tool_name)
    if tool is None:
        return {"title": tool_name, "summary": "Tool unavailable in MCP server."}

    kwargs: dict[str, Any]
    if tool_name == "head_to_head":
        kwargs = {"batter": batter, "bowler": bowler}
    elif tool_name == "batter_vs_bowling_type":
        kwargs = {"batter": batter}
    elif tool_name == "bowler_wicket_profile":
        kwargs = {"bowler": bowler}
    elif tool_name == "bowler_setup_sequence":
        kwargs = {"bowler": bowler, "batter": batter, "n_balls_before": 6}
    elif tool_name == "milestone_tracker":
        # over_range is 0-indexed: (7, 14) == overs 8-15 in commentary terms.
        kwargs = {"batter": batter, "milestone_runs": 500, "vs_bowling_type": "spin", "over_range": (7, 14)}
    else:
        kwargs = {"batter": batter, "bowler": bowler}

    try:
        # Awaited inline: the tool is bound to the caller's open session, and
        # safe_tool_dispatch would offload to a second event loop in a thread.
        result = await _dispatch_async(tool, kwargs)
    except Exception as exc:  # pragma: no cover - runtime recovery path
        logger.exception("Tool %s failed for %s vs %s", tool_name, batter, bowler)
        return {
            "title": tool_name,
            "summary": f"Tool failed: {type(exc).__name__}: {exc}",
            "error": True,
        }

    result = unwrap_mcp_result(result)

    summary = ""
    if isinstance(result, dict):
        if "batter" in result and "bowler" in result:
            summary = json.dumps({k: v for k, v in result.items() if k in {"matches_faced", "balls_faced", "runs", "strike_rate", "dismissals"}}, default=str)
        elif "bowler" in result:
            summary = json.dumps({k: v for k, v in result.items() if k in {"total_wickets", "economy_rate", "bowling_strike_rate", "wickets_by_phase"}}, default=str)
        else:
            summary = json.dumps(result, default=str)[:500]
    else:
        summary = json.dumps(result, default=str)[:500]

    return {
        "title": tool_name.replace("_", " ").title(),
        "tool": tool_name,
        "summary": summary,
        # Full structured payload — the UI renders this; the LLM sees `summary`.
        "data": result,
    }


async def run_graph_agent(question: str, server_path: str | Path | None = None) -> dict[str, Any]:
    """Run an MCP-connected LangGraph-style tool-calling workflow for a cricket briefing."""
    # One stdio session for the whole request: every tool call reuses the same
    # server process instead of spawning a new one.
    async with mcp_tool_session(server_path) as tools:
        tool_map = {getattr(tool, "name", ""): tool for tool in tools}

        # Names come from a human, so resolve them against the real player
        # index before any analyzer sees them. Exact-match tools return zeroed
        # results for an unknown spelling, which is indistinguishable from
        # "no data".
        batter, bowler, resolutions = await _resolve_players(question, tool_map)

        selected_tools = _tool_plan_for_question(question)
        matched = [name for name in selected_tools if name in tool_map]

        if not matched:
            matched = [
                name
                for name in ("head_to_head", "batter_vs_bowling_type",
                             "bowler_wicket_profile")
                if name in tool_map
            ]

        tool_results = [
            await _tool_result_from_name(name, tool_map, batter, bowler)
            for name in matched
        ]

    card = build_pre_match_briefing_card(batter, bowler, tool_results)
    llm = None
    try:
        llm = get_chat_model()
    except RuntimeError:
        logger.info("LLM not configured; returning rule-based briefing card only.")

    if llm is not None:
        try:
            prompt = (
                "You are a cricket analyst. Turn the following tool outputs into a concise, "
                "broadcast-ready pre-match briefing. Only use the supplied facts.\n\n"
                f"Question: {question}\n\n"
                f"Tool outputs: {json.dumps(tool_results, default=str)}"
            )
            ai_response = llm.invoke(prompt)
            card["ai_summary"] = getattr(ai_response, "content", str(ai_response))
        except Exception as exc:  # pragma: no cover - model/network error recovery path
            logger.exception("LLM summary generation failed")
            card["ai_summary"] = f"Summary skipped: {type(exc).__name__}: {exc}"

    if resolutions:
        card["resolutions"] = resolutions

    return {
        "question": question,
        "batter": batter,
        "bowler": bowler,
        "resolutions": resolutions,
        "tool_names": matched,
        "tool_results": tool_results,
        "briefing_card": card,
    }


def _build_langgraph_graph():
    if StateGraph is None:
        raise RuntimeError("langgraph is not installed.")
    workflow = StateGraph(dict)

    def discover_tools(state):
        return {**state, "tools": asyncio.run(_discover_mcp_tools())}

    def choose_tools(state):
        question = state.get("question", "")
        tool_names = _tool_plan_for_question(question)
        state["selected_tools"] = tool_names
        return state

    def call_tools(state):
        tool_map = {getattr(tool, "name", ""): tool for tool in state.get("tools", [])}
        batter, bowler = _question_to_player_pair(state.get("question", ""))
        tool_results = []
        for name in state.get("selected_tools", []):
            if name in tool_map:
                tool_results.append(asyncio.run(_tool_result_from_name(name, tool_map, batter, bowler)))
        state["tool_results"] = tool_results
        return state

    def compose_briefing(state):
        batter, bowler = _question_to_player_pair(state.get("question", ""))
        state["briefing_card"] = build_pre_match_briefing_card(batter, bowler, state.get("tool_results", []))
        return state

    workflow.add_node("discover_tools", discover_tools)
    workflow.add_node("choose_tools", choose_tools)
    workflow.add_node("call_tools", call_tools)
    workflow.add_node("compose_briefing", compose_briefing)

    workflow.add_edge(START, "discover_tools")
    workflow.add_edge("discover_tools", "choose_tools")
    workflow.add_edge("choose_tools", "call_tools")
    workflow.add_edge("call_tools", "compose_briefing")
    workflow.add_edge("compose_briefing", END)
    return workflow.compile()


_GRAPH = None


def get_graph():
    """Lazily compile the StateGraph.

    Built on demand rather than at import time: _build_langgraph_graph() raises
    when langgraph is absent, which would defeat the optional-dependency guards
    at the top of this module and break `import langgraph_agent` for the unit
    tests that only exercise the pure helpers.
    """
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = _build_langgraph_graph()
    return _GRAPH


def run_pre_match_briefing(question: str, server_path: str | Path | None = None) -> dict[str, Any]:
    """Public entry point for the project; works in sync code and returns a briefing card."""
    return asyncio.run(run_graph_agent(question, server_path))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    question = " ".join(sys.argv[1:]) or "brief me on Kohli vs Bumrah"
    result = run_pre_match_briefing(question)
    print(json.dumps(result["briefing_card"], indent=2, default=str))
