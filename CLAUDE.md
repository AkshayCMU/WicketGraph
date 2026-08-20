# WicketGraph: MCP-Enabled Cricket Intelligence Platform

**Role:** You are a Senior AI Engineer building an MCP-enabled cricket intelligence platform with strong cricket domain awareness.

**Project Vision:**
WicketGraph is a structured cricket intelligence platform that transforms raw Cricsheet ball-by-ball JSON data into normalized analytical datasets, exposes those datasets through a custom MCP server, and enables LLMs to behave like professional cricket analysts. The system combines structured match data (ball-by-ball deliveries, dismissals, extras, innings context) with unstructured contextual inputs (match narratives, commentary, tactical notes) to support richer, domain-aware analysis.

- **Cricket Intelligence Layer:** normalize and store historical match data in a queryable SQLite silver layer, structured for analytical reuse across tools.
- **MCP Server Layer:** expose analytical functions as MCP tools so that any MCP-compatible LLM client can query match data, retrieve patterns, and generate cricket-grounded explanations.
- **Orchestration Layer:** an MCP-connected agent that plans which tools to call and composes the results into a briefing.

**Primary Input Source:**
Cricsheet JSON match files, beginning with IPL matches.

**Tech Stack:**
Python, Pandas, SQLite (Silver Layer), MCP Python SDK, LangGraph + langchain-mcp-adapters, FastAPI, React/Vite, Docker, Kubernetes.

---

## Architecture Principles

- Treat ingestion, normalization, feature engineering, and analytical querying as separate modules.
- Treat every analytical capability as a tool that can later be invoked by a LangGraph agent or called directly via MCP.
- Prefer deterministic data pipelines before adding agent complexity.
- Design outputs so they are reusable for both post-match explanation and future live prediction systems.
- **Atomic Ingestion:** ingestion must preserve SQLite integrity; use safe transactional behavior and compensating deletes to ensure no partial match data is committed on failure.
- **Tool-First Design:** scripts and analyzers should be designed as modular, MCP-ready tools — isolated functions with typed inputs and explicit outputs, not pipeline scripts.
- **The LLM narrates, it never computes.** Every number in any output must trace to a specific SQL query. Models receive tool output and are instructed to use only supplied facts.

## Rules

- All ingestion logic must robustly parse nested Cricsheet JSON structures, validate schema assumptions, handle missing or optional fields gracefully, and produce normalized tabular outputs.
- Code must be modular, with extraction and analysis logic implemented as isolated, testable functions or tools.
- Do not use global variables; rely on strictly typed function arguments and explicit return values.
- Preserve grain correctly: delivery-level data must not be accidentally duplicated or aggregated prematurely.
- Separate raw, normalized, and feature-engineered data layers clearly.
- Write code that is easy to expose as MCP tools or convert into LangGraph tool nodes later.
- Prefer clarity and correctness over premature optimization.
- Cricket analysis logic must preserve cricket semantics — do not flatten away meaningful distinctions such as over phase (powerplay vs middle vs death), dismissal type, bowling style (pace vs spin), or innings context. These distinctions are analytically load-bearing.
- **Any new analyzer query must be index-supported.** See "Performance contract" below.

---

## Current State (source of truth: the code)

| Phase | Status | Where |
|---|---|---|
| 1. Ingestion foundation | **Complete** | `src/extractor.py`, `src/ingest_all.py` |
| 2a. Analyzer engine | **Complete** — 8 functions + a coverage tool | `src/analyzer.py` |
| 2b. MCP server | **Complete** — 9 tools over stdio | `src/mcp_server.py` |
| 2c. Commentator briefing UI | **Superseded** — see note below | `src/api/`, `frontend/` |
| 3. Orchestration | **Partial** — linear agent live, StateGraph scaffolded but unused | `src/langgraph_agent.py` |
| 4. Deployment | **Complete** | `Dockerfile`, `docker-compose.yml`, `k8s/` |

**Note on 2c:** the original plan specified a Streamlit "commentator prep" app as the only sanctioned frontend. That is not what was built. `src/commentator_prep.py` does not exist. Instead the project shipped a FastAPI service (`src/api/app.py`) and a React/Vite SPA (`frontend/`), containerized and deployable to Kubernetes. **The FastAPI + React stack is now the sanctioned frontend. Do not add Streamlit.** The earlier "do not build live APIs / frontend dashboards yet" constraints are retired — they described Phase 2 sequencing, and that sequencing is done.

---

## Layer Map

```
data/01_bronze_cricsheet/*.json      1,169 raw Cricsheet IPL match files
  └─ src/extractor.py                pure JSON → 5 DataFrames, no DB awareness
      └─ src/ingest_all.py           orchestration: idempotency + compensating deletes + indexes
          └─ data/02_silver_tables/silver.db
              └─ src/analyzer.py     8 typed analytical functions (db_path injected)
                  └─ src/mcp_server.py       FastMCP stdio server, 9 tools
                      └─ src/langgraph_agent.py   MCP client, tool planner, LLM narrator
                          └─ src/api/app.py       FastAPI: /health /tools /briefing
                              └─ frontend/        React + Vite, served by nginx
```

`data/03_gold_features/` exists but is empty — there is no gold layer yet.

## Silver Schema

| table | grain | rows |
|---|---|---|
| `matches` | one row per match | 1,169 |
| `innings` | one row per (match, innings) | 2,365 |
| `deliveries` | one row per **delivery attempt**, legal or not | 278,205 |
| `wickets` | one row per dismissal | 13,823 |
| `extras` | one row per (delivery, extra type) | 15,161 |
| `ingestion_log` | one row per source file | 1,169 |

**Critical grain detail:** `deliveries.ball` is a *positional index within the over* produced by `enumerate(...)`, including illegal deliveries. It is **not** the legal ball number. An over containing a wide has 7 rows. This makes `(match_id, innings_number, over, ball)` a guaranteed-unique surrogate key, but it means **legality is not encoded in the delivery row** and must be recovered by an anti-join against `extras`.

The five analytical tables are created implicitly by `pandas.to_sql()`, so they carry **no declared primary or foreign keys**. Grain and referential integrity are *verified* by `ingest_all.verify()`, not enforced by the engine.

## Performance contract

Indexes are defined in `src/db.py` (`SILVER_INDEXES`) and applied idempotently by `db.ensure_indexes()`, which `ingest_all()` calls on every run. `verify()` reports any that are missing.

They are not optional. The legal-ball anti-join runs a correlated `NOT EXISTS` against `extras` once per candidate delivery. Measured on the full corpus, counting one batter's legal deliveries:

- without indexes: **20.17 s**
- with indexes: **0.24 s** (~83x)

`idx_extras_key` deliberately includes `type` as a trailing column so the probe is index-only.

**When adding an analyzer function:** confirm it is covered by an existing index or add one to `SILVER_INDEXES`, and add the index name to the expected set in `verify()`.

## Cricket semantics: the constants that matter

**Over phases** (0-indexed, as stored):

| phase | overs (0-idx) | overs (spoken) |
|---|---|---|
| powerplay | 0–5 | 1–6 |
| middle | 6–14 | 7–15 |
| death | 15–19 | 16–20 |

Enforced by a SQL `CASE` in every phase-aware query and mirrored by `analyzer._phase_label()`.

**Dismissals — two constants, deliberately distinct.** Conflating them is a real analytical error:

- `BOWLER_WICKET_KINDS` — credited to the bowler. Used for bowler profiles, head-to-head, setup sequences, and pace/spin splits. Excludes run outs: a run out off a spinner's over is not the spinner's wicket.
- `BATTER_DISMISSAL_KINDS` — every way a batter's innings actually ends, used as the denominator for batting average. **Includes run outs.** Excludes only `retired hurt`, since the batter may resume.

**Extras.** Only `wides` are excluded from balls faced. No-balls are still balls faced (cricket-correct) and still count toward a bowler's economy. Observed types: `wides`, `legbyes`, `noballs`, `byes`, `penalty`.

**Player names.** The silver layer stores Cricsheet's abbreviated form (`MA Starc`, `V Kohli`, `RG Sharma`), but every analyzer matches names **exactly** — an unresolved string returns a silently zeroed result, which reads as "no data" when it is really "no such name". Always run a human-supplied name through `analyzer.resolve_player_name()` (MCP tool: `resolve_player`) first.

Resolution uses three signals, because surname alone is ambiguous — 12+ players are named Sharma and two are named Kohli:

1. **surname** — the last token, matched exactly
2. **given initial** — "Rohit" → R, separating `RG Sharma` from `I Sharma`
3. **involvement** — balls faced / bowled, ranking the prominent player first

Passing `role="batter"` or `role="bowler"` narrows the pool to players who actually did that, which resolves most remaining ambiguity. Falls back through substring and `difflib` fuzzy matching, so `Kohly` and `Bumra` still resolve. Returns `confidence` plus ranked `candidates` — surface those rather than guessing when confidence is low.

`PLAYER_ALIASES` handles given-name-only queries (`rohit`, `virat`, `msd`). This is irreducibly manual: the stored form keeps *initials*, so "RG Sharma" contains no trace of the string "rohit" and no data-driven matching can bridge it. A test asserts every alias target exists in the delivery data.

**Bowling style.** Cricsheet supplies no bowling-style field and the extractor drops the player registry, so `analyzer.BOWLER_STYLES` is a hand-maintained map of **191 bowlers covering 84.4% of all deliveries**. The remaining 15.6% are labelled `"unknown"` and surfaced as their own row — never silently folded into pace or spin. Call `analyzer.bowling_style_coverage(db_path)` to measure the gap. Callers may inject corrections at runtime via `extra_styles`; do not mutate the module dict.

## Known limits

- **Dropped source fields.** `extractor._parse_match` keeps only city, venue, date, winner, and player-of-match. `teams`, `toss`, `event`, `outcome.by`, and the player registry are discarded. Consequently there is **no chasing/defending context and no batting position**, so target insights like *"Dhoni in death overs when chasing"* are not currently answerable. Extending the extractor is the prerequisite, not a new analyzer.
- **Two MCP servers exist.** `src/mcp_server.py` (current, 9 tools) and `src/server.py` + `src/analyzers/bowler_setup.py` (earlier prototype, 1 tool). They implement overlapping setup-sequence logic with different dismissal filters. Prefer `mcp_server.py`. Treat `server.py` as legacy.
- **The StateGraph is scaffolding.** `langgraph_agent._build_langgraph_graph()` compiles a real 4-node graph, but production traffic goes through the linear `run_graph_agent()`. The graph is reached only via `get_graph()`, which nothing currently calls.
- **Tool planning is rule-based, not model-driven.** `_tool_plan_for_question()` is keyword matching capped at three tools. The LLM does not choose tools. (Player-name resolution is no longer hardcoded — see below.)

## Local development

```bash
pip install -r requirements-api.txt
python src/ingest_all.py          # ingest + verify + ensure indexes (idempotent)
python -m pytest tests/ -q        # 68 tests
python src/mcp_server.py          # stdio MCP server for Claude Desktop
docker compose up -d --build      # API on :8100, UI on :5273
```

**Ports.** `docker compose` binds **8100** (API) and **5273** (UI), not 8000/5173 — those clash with other local projects. Override with `API_PORT` / `UI_PORT`. `VITE_API_BASE` is a **build argument**, not a runtime variable: Vite inlines `import.meta.env` at compile time, so changing it requires a UI rebuild.

**Dependency pin.** `mcp` must stay `<2.0`. Version 2.x removed `RequestContext` from `mcp.shared.context`, which `langchain-mcp-adapters` 0.3.x imports at module load, breaking the `/briefing` path.

## Non-Goals

- Do not add Streamlit or any additional frontend. React + FastAPI is the sanctioned stack.
- Do not train prediction models yet.
- Do not add vector databases or LLM memory until contextual research tools are designed.
- Do not let the LLM compute statistics. It narrates tool output, nothing more.

## Next

- **Extractor v2:** persist `teams`, `toss`, `event`, and `outcome.by` to unlock chasing/defending and batting-position context.
- **Model-driven planning:** replace `_tool_plan_for_question` keyword matching with real LLM tool selection, and route it through the compiled StateGraph instead of the linear path.
- **Session reuse across requests:** `mcp_tool_session()` already holds one stdio session per request (previously one subprocess spawn *per tool call*, ~10s each). A process-lifetime pool would remove the remaining per-request spawn.
- **Contextual research tools:** pull unstructured inputs (commentary, match reports) alongside silver-layer data.
- **Gold layer:** `data/03_gold_features/` is still empty.
