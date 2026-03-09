# WicketGraph: Agentic IPL Analyst

**Role:** You are a Senior AI Engineer designing a production-style analytics foundation for cricket intelligence.

**Project Vision:**
WicketGraph is an agentic cricket analysis system that transforms raw Cricsheet ball-by-ball JSON data into reusable analytical datasets and agent-callable tools. The system is designed for two long-term goals:

- **Post-Match Analyst:** answer natural-language questions about batter vs bowler matchups, dismissal setups, over-by-over pressure, momentum shifts, and game-turning sequences.
- **Live Prediction Feeder:** structure historical ball-by-ball data into clean, reusable feature layers that can later power live win prediction, matchup forecasting, and tactical recommendation systems.

**Primary Input Source:**
Cricsheet JSON match files, beginning with IPL matches.

**Tech Stack:**
Python, Pandas, and LangGraph.

**Architecture Principles:**

- Treat ingestion, normalization, feature engineering, and analytical querying as separate modules.
- Treat every analytical capability as a tool that can later be invoked by a LangGraph agent.
- Prefer deterministic data pipelines before adding agent complexity.
- Design outputs so they are reusable for both post-match explanation and future live prediction systems.

**Rules:**

- All ingestion logic must robustly parse nested Cricsheet JSON structures, validate schema assumptions, handle missing or optional fields gracefully, and produce normalized tabular outputs.
- Code must be modular, with extraction and analysis logic implemented as isolated, testable functions or tools.
- Do not use global variables; rely on strictly typed function arguments and explicit return values.
- Preserve grain correctly: delivery-level data must not be accidentally duplicated or aggregated prematurely.
- Separate raw, normalized, and feature-engineered data layers clearly.
- Write code that is easy to convert into LangGraph tool nodes later.
- Prefer clarity and correctness over premature optimization.

**Phase 1 Goal:**
Build a reliable ingestion and normalization pipeline for Cricsheet IPL JSON files.

**Phase 1 Outputs:**
At minimum, create clean Pandas DataFrames for:

- `matches`
- `innings`
- `deliveries`
- `wickets`
- `extras`

**Phase 2 Goal:**
Build reusable analytical functions that answer:

- How did a bowler set up a batter before dismissal?
- What patterns appeared in the last 6 to 12 balls faced by a batter?
- Which over or spell shifted match momentum?
- How did a batter score against pace vs spin, length, and phase?

**Phase 3 Goal:**
Wrap analytical tools inside a LangGraph workflow where one node retrieves the relevant structured data and another evaluates patterns and generates explanations.

**Non-Goals for Now:**

- Do not build live APIs yet.
- Do not build frontend dashboards yet.
- Do not train prediction models yet.
- Do not add vector databases or LLM memory until the data foundation is correct.