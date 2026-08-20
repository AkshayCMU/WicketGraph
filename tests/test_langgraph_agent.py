import pytest

from src.langgraph_agent import (
    repair_tool_arguments,
    safe_tool_dispatch,
    build_pre_match_briefing_card,
)


def test_repair_tool_arguments_handles_string_to_int_and_missing_fields():
    tool_schema = {
        "name": "head_to_head",
        "properties": {
            "batter": {"type": "string"},
            "bowler": {"type": "string"},
        },
        "required": ["batter", "bowler"],
    }

    repaired = repair_tool_arguments(
        tool_schema,
        {"batter": "  V Kohli  ", "bowler": 12345},
    )

    assert repaired["batter"] == "V Kohli"
    assert repaired["bowler"] == "12345"


def test_safe_tool_dispatch_retries_after_type_error_and_continues():
    calls = []

    def flaky_tool(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise TypeError("missing positional arg")
        return {"status": "ok", "runs": 42}

    result = safe_tool_dispatch(
        flaky_tool,
        {"batter": "V Kohli", "bowler": "JJ Bumrah"},
        max_retries=2,
    )

    assert result["status"] == "ok"
    assert len(calls) == 2


def test_safe_tool_dispatch_uses_async_structured_tool_path():
    class AsyncTool:
        def __init__(self):
            self.calls = []

        async def ainvoke(self, input_data):
            self.calls.append(input_data)
            return {"status": "async-ok", "runs": 17}

    async_tool = AsyncTool()
    result = safe_tool_dispatch(async_tool, {"batter": "V Kohli"})

    assert result["status"] == "async-ok"
    assert result["runs"] == 17
    assert async_tool.calls == [{"batter": "V Kohli"}]


def test_build_pre_match_briefing_card_uses_tool_results_to_compose_summary():
    tool_results = [
        {"title": "Head to head", "summary": "Kohli scores at 155 vs Bumrah"},
        {"title": "Bowling profile", "summary": "Bumrah gets 41% wickets in powerplay"},
    ]

    card = build_pre_match_briefing_card(
        batter="V Kohli",
        bowler="JJ Bumrah",
        tool_results=tool_results,
    )

    assert card["batter"] == "V Kohli"
    assert card["bowler"] == "JJ Bumrah"
    assert card["sections"][0]["title"] == "Head to head"
    assert "Kohli" in card["headline"]


# ---------------------------------------------------------------------------
# MCP envelope unwrapping
# ---------------------------------------------------------------------------

def test_unwrap_mcp_result_parses_text_content_blocks():
    from src.langgraph_agent import unwrap_mcp_result

    envelope = [{"type": "text", "text": '{"batter": "V Kohli", "runs": 155}'}]
    assert unwrap_mcp_result(envelope) == {"batter": "V Kohli", "runs": 155}


def test_unwrap_mcp_result_joins_split_text_blocks():
    from src.langgraph_agent import unwrap_mcp_result

    envelope = [
        {"type": "text", "text": '{"runs":'},
        {"type": "text", "text": " 155}"},
    ]
    assert unwrap_mcp_result(envelope) == {"runs": 155}


def test_unwrap_mcp_result_passes_through_plain_values():
    from src.langgraph_agent import unwrap_mcp_result

    assert unwrap_mcp_result({"already": "a dict"}) == {"already": "a dict"}
    assert unwrap_mcp_result("not json at all") == "not json at all"
    assert unwrap_mcp_result(None) is None


def test_unwrap_mcp_result_handles_content_attribute():
    from src.langgraph_agent import unwrap_mcp_result

    class Block:
        type = "text"
        text = '{"ok": true}'

    class Response:
        content = [Block()]

    assert unwrap_mcp_result(Response()) == {"ok": True}


def test_briefing_card_carries_structured_data_and_tool_name():
    """The UI renders `data`; dropping it silently degrades to plain text."""
    card = build_pre_match_briefing_card(
        batter="V Kohli",
        bowler="JJ Bumrah",
        tool_results=[
            {
                "title": "Head To Head",
                "tool": "head_to_head",
                "summary": '{"runs": 155}',
                "data": {"runs": 155, "by_phase": [{"phase": "death"}]},
            }
        ],
    )
    section = card["sections"][0]
    assert section["tool"] == "head_to_head"
    assert section["data"]["runs"] == 155
    assert section["data"]["by_phase"][0]["phase"] == "death"


def test_briefing_card_omits_data_when_tool_returned_none():
    card = build_pre_match_briefing_card(
        batter="V Kohli",
        bowler="JJ Bumrah",
        tool_results=[{"title": "Broken", "summary": "Tool failed", "error": True}],
    )
    assert "data" not in card["sections"][0]
    assert card["sections"][0]["error"] is True


# ---------------------------------------------------------------------------
# Question -> name fragments
# ---------------------------------------------------------------------------

import pytest as _pytest  # noqa: E402


@_pytest.mark.parametrize(
    "question,expected",
    [
        ("kohli vs starc", ("kohli", "starc")),
        ("Kohli vs Starc", ("Kohli", "Starc")),
        ("brief me on Kohli vs Bumrah", ("Kohli", "Bumrah")),
        ("how does Rohit Sharma do against Rashid Khan",
         ("Rohit Sharma", "Rashid Khan")),
        ("tell me about MS Dhoni versus Jasprit Bumrah",
         ("MS Dhoni", "Jasprit Bumrah")),
        ("Kohli vs Starc powerplay and death phase setup patterns",
         ("Kohli", "Starc")),
        ("de villiers vs narine", ("de villiers", "narine")),
    ],
)
def test_split_question_into_names(question, expected):
    from src.langgraph_agent import split_question_into_names

    assert split_question_into_names(question) == expected


def test_split_question_without_separator_returns_single_fragment():
    from src.langgraph_agent import split_question_into_names

    left, right = split_question_into_names("starc")
    assert left == "starc"
    assert right == ""


def test_question_pair_falls_back_when_nothing_parses():
    """An empty question must not crash the endpoint."""
    from src.langgraph_agent import _question_to_player_pair

    batter, bowler = _question_to_player_pair("")
    assert batter and bowler


def test_unwrap_mcp_result_rebuilds_a_list_from_per_element_blocks():
    """FastMCP emits one text block per element when a tool returns a list."""
    from src.langgraph_agent import unwrap_mcp_result

    envelope = [
        {"type": "text", "text": '{"phase": "powerplay", "runs": 10}'},
        {"type": "text", "text": '{"phase": "middle", "runs": 20}'},
        {"type": "text", "text": '{"phase": "death", "runs": 30}'},
    ]
    out = unwrap_mcp_result(envelope)
    assert isinstance(out, list)
    assert [r["phase"] for r in out] == ["powerplay", "middle", "death"]
    assert sum(r["runs"] for r in out) == 60


def test_unwrap_mcp_result_single_block_stays_an_object():
    from src.langgraph_agent import unwrap_mcp_result

    out = unwrap_mcp_result([{"type": "text", "text": '{"runs": 155}'}])
    assert out == {"runs": 155}


def test_unwrap_mcp_result_falls_back_to_text_when_blocks_are_not_json():
    from src.langgraph_agent import unwrap_mcp_result

    envelope = [
        {"type": "text", "text": "Tool failed: "},
        {"type": "text", "text": "no such player"},
    ]
    assert unwrap_mcp_result(envelope) == "Tool failed: no such player"
