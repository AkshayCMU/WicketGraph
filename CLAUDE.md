# WicketGraph: MCP-Enabled Cricket Intelligence Platform

**Role:** You are a Senior AI Engineer building an MCP-enabled cricket intelligence platform with strong cricket domain awareness.

**Project Vision:**
WicketGraph is a structured cricket intelligence platform that transforms raw Cricsheet ball-by-ball JSON data into normalized analytical datasets, exposes those datasets through a custom MCP server, and enables LLMs to behave like professional cricket analysts. The system combines structured match data (ball-by-ball deliveries, dismissals, extras, innings context) with unstructured contextual inputs (match narratives, commentary, tactical notes) to support richer, domain-aware analysis.

- **Cricket Intelligence Layer:** normalize and store historical match data in a queryable SQLite silver layer, structured for analytical reuse across tools.
- **MCP Server Layer:** expose analytical functions as MCP tools so that any MCP-compatible LLM client can query match data, retrieve patterns, and generate cricket-grounded explanations.
- **Future Orchestration Layer:** wrap MCP tools inside a LangGraph workflow for multi-step reasoning over match sequences, player matchups, and tactical patterns.

**Primary Input Source:**
Cricsheet JSON match files, beginning with IPL matches.

**Tech Stack:**
Python, Pandas, SQLite (Silver Layer), MCP Python SDK, and LangGraph (future orchestration layer — not current build priority).

**Architecture Principles:**

- Treat ingestion, normalization, feature engineering, and analytical querying as separate modules.
- Treat every analytical capability as a tool that can later be invoked by a LangGraph agent or called directly via MCP.
- Prefer deterministic data pipelines before adding agent complexity.
- Design outputs so they are reusable for both post-match explanation and future live prediction systems.
- **Atomic Ingestion:** ingestion must preserve SQLite integrity; use safe transactional behavior and compensating deletes to ensure no partial match data is committed on failure.
- **Tool-First Design:** scripts and analyzers should be designed as modular, MCP-ready tools — isolated functions with typed inputs and explicit outputs, not pipeline scripts.

**Rules:**

- All ingestion logic must robustly parse nested Cricsheet JSON structures, validate schema assumptions, handle missing or optional fields gracefully, and produce normalized tabular outputs.
- Code must be modular, with extraction and analysis logic implemented as isolated, testable functions or tools.
- Do not use global variables; rely on strictly typed function arguments and explicit return values.
- Preserve grain correctly: delivery-level data must not be accidentally duplicated or aggregated prematurely.
- Separate raw, normalized, and feature-engineered data layers clearly.
- Write code that is easy to expose as MCP tools or convert into LangGraph tool nodes later.
- Prefer clarity and correctness over premature optimization.
- Cricket analysis logic must preserve cricket semantics — do not flatten away meaningful distinctions such as over phase (powerplay vs middle vs death), dismissal type, bowling style (pace vs spin), or innings context. These distinctions are analytically load-bearing.

**Phase 1: Ingestion Foundation — COMPLETED**
Built a reliable ingestion and normalization pipeline for Cricsheet IPL JSON files. All 1,169 IPL matches have been processed into a SQLite silver layer (`data/02_silver_tables/silver.db`) with the following validated tables:

- `matches` — 1,169 rows
- `innings` — 2,365 rows
- `deliveries` — 278,205 rows (grain-checked: no duplicate `(match_id, innings_number, over, ball)` combinations)
- `wickets` — 13,823 rows
- `extras` — 15,161 rows
- `ingestion_log` — 1,169 rows (all `success`; incremental re-runs skip already-processed files)

**Phase 2 Goal (current):**
Build analyzers and the MCP server. Analytical functions should answer:

- How did a bowler set up a batter before dismissal?
- What patterns appeared in the last 6 to 12 balls faced by a batter?
- Which over or spell shifted match momentum?
- How did a batter score against pace vs spin, length, and phase?

Each analyzer is implemented as a standalone, typed function and then registered as an MCP tool so LLM clients can invoke it directly.

**Phase 3 Goal:**
Wrap MCP tools inside a LangGraph workflow for multi-step reasoning. Add contextual research tools that can pull unstructured inputs (commentary, match reports) alongside structured silver layer data. One node retrieves relevant structured data; another evaluates patterns and generates cricket-grounded explanations.

**Non-Goals for Now:**

- Do not build live APIs yet.
- Do not build frontend dashboards yet.
- Do not train prediction models yet.
- Do not add vector databases or LLM memory until the MCP server layer is stable.
