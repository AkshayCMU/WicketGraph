import { useMemo, useState } from "react";
import { fetchBriefing } from "./api";

type BriefingSection = {
  title: string;
  tool?: string;
  summary: string;
  data?: unknown;
};

type Resolution = {
  query: string;
  role?: string;
  resolved: string | null;
  match_type?: string;
  confidence?: string;
  candidates?: { name: string; batter_balls: number; bowler_balls: number }[];
};

type BriefingCard = {
  batter?: string;
  bowler?: string;
  headline?: string;
  summary?: string;
  sections?: BriefingSection[];
  ai_summary?: string;
};

type PhaseRow = {
  phase: string;
  balls?: number;
  balls_faced?: number;
  runs: number;
  strike_rate: number | null;
  dismissals: number;
};

/** The layers a single /briefing call actually traverses, in order. */
const PIPELINE = [
  { id: "ui", label: "React UI", detail: "POST /briefing" },
  { id: "api", label: "FastAPI", detail: "src/api/app.py" },
  { id: "resolve", label: "Resolve names", detail: "\"Starc\" → \"MA Starc\"" },
  { id: "agent", label: "Agent", detail: "plan tools" },
  { id: "mcp", label: "MCP server", detail: "stdio JSON-RPC · src/mcp_server.py" },
  { id: "analyzer", label: "Analyzer", detail: "typed SQL · src/analyzer.py" },
  { id: "db", label: "silver.db", detail: "278,205 deliveries" },
  { id: "llm", label: "LLM narrator", detail: "facts in → prose out" },
];

function isPhaseRow(value: unknown): value is PhaseRow {
  return (
    typeof value === "object" &&
    value !== null &&
    "phase" in value &&
    "runs" in value
  );
}

/** Pull a [powerplay, middle, death] table out of whatever a tool returned. */
function extractPhaseRows(data: unknown): PhaseRow[] | null {
  if (Array.isArray(data) && data.every(isPhaseRow)) return data as PhaseRow[];
  if (typeof data === "object" && data !== null) {
    const byPhase = (data as Record<string, unknown>).by_phase;
    if (Array.isArray(byPhase) && byPhase.every(isPhaseRow)) {
      return byPhase as PhaseRow[];
    }
  }
  return null;
}

const PHASE_ORDER = ["powerplay", "middle", "death"];

function sortPhases(rows: PhaseRow[]): PhaseRow[] {
  return [...rows].sort(
    (a, b) => PHASE_ORDER.indexOf(a.phase) - PHASE_ORDER.indexOf(b.phase)
  );
}

/** Scalar headline stats, excluding nested structures we render separately. */
function scalarEntries(data: unknown): [string, string][] {
  if (typeof data !== "object" || data === null || Array.isArray(data)) return [];
  const skip = new Set(["batter", "bowler", "by_phase", "narrative"]);
  return Object.entries(data as Record<string, unknown>)
    .filter(
      ([k, v]) =>
        !skip.has(k) &&
        (typeof v === "number" || typeof v === "string" || v === null)
    )
    .map(([k, v]) => [
      k.replace(/_/g, " "),
      v === null ? "—" : String(v),
    ]);
}

function PhaseTable({ rows }: { rows: PhaseRow[] }) {
  const max = Math.max(...rows.map((r) => r.strike_rate ?? 0), 1);
  return (
    <div className="table-scroll">
      <table className="phase-table">
        <thead>
          <tr>
            <th>Phase</th>
            <th>Balls</th>
            <th>Runs</th>
            <th>Strike rate</th>
            <th>Dismissals</th>
          </tr>
        </thead>
        <tbody>
          {sortPhases(rows).map((r) => (
            <tr key={r.phase}>
              <td>
                <span className={`phase-pill phase-${r.phase}`}>{r.phase}</span>
              </td>
              <td>{r.balls ?? r.balls_faced ?? 0}</td>
              <td>{r.runs}</td>
              <td>
                <div className="sr-cell">
                  <span>{r.strike_rate ?? "—"}</span>
                  <div className="sr-bar">
                    <div
                      className="sr-fill"
                      style={{
                        width: `${((r.strike_rate ?? 0) / max) * 100}%`,
                      }}
                    />
                  </div>
                </div>
              </td>
              <td>{r.dismissals}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Resolutions({ items }: { items: Resolution[] }) {
  if (!items.length) return null;
  return (
    <div className="resolutions">
      {items.map((r, i) => (
        <div
          key={`${r.query}-${i}`}
          className={`res ${r.resolved ? "" : "res-miss"}`}
        >
          <span className="res-role">{r.role}</span>
          <span className="res-q">{r.query}</span>
          <span className="res-arrow" aria-hidden="true">→</span>
          {r.resolved ? (
            <>
              <strong className="res-name">{r.resolved}</strong>
              {r.confidence && (
                <span className={`res-conf conf-${r.confidence}`}>
                  {r.confidence}
                </span>
              )}
              {!!r.candidates?.length && (
                <span className="res-alt">
                  or {r.candidates.map((c) => c.name).join(", ")}
                </span>
              )}
            </>
          ) : (
            <strong className="res-name">no match in the data</strong>
          )}
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const [question, setQuestion] = useState("brief me on Kohli vs Bumrah");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<any>(null);
  const [elapsed, setElapsed] = useState<number | null>(null);
  const [showRaw, setShowRaw] = useState(false);

  const briefing = useMemo<BriefingCard | null>(() => {
    if (!result) return null;
    return result.briefing_card || result;
  }, [result]);

  const toolNames: string[] = result?.tool_names || [];
  const resolutions: Resolution[] = result?.resolutions || [];

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setLoading(true);
    setError(null);
    setElapsed(null);
    const started = performance.now();

    try {
      const payload = await fetchBriefing(question);
      setResult(payload);
      setElapsed(Math.round(performance.now() - started));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Request failed");
    } finally {
      setLoading(false);
    }
  };

  const active = loading || !!briefing;

  return (
    <div className="page-shell">
      <div className="card">
        <header className="header">
          <div>
            <p className="eyebrow">WicketGraph</p>
            <h1>Pre-match briefing</h1>
          </div>
          {elapsed !== null && (
            <span className="timing">{elapsed} ms end to end</span>
          )}
        </header>

        <form onSubmit={handleSubmit} className="search-row">
          <input
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="brief me on Kohli vs Bumrah"
            aria-label="Question"
          />
          <button type="submit" disabled={loading}>
            {loading ? "Loading..." : "Submit"}
          </button>
        </form>

        {/* Pipeline trace — makes the layer hops visible rather than implied. */}
        <ol className={`pipeline ${active ? "pipeline-active" : ""}`}>
          {PIPELINE.map((step, i) => (
            <li key={step.id} className={loading ? "step-pending" : briefing ? "step-done" : ""}>
              <span className="step-index">{i + 1}</span>
              <span className="step-body">
                <strong>{step.label}</strong>
                <em>{step.detail}</em>
              </span>
            </li>
          ))}
        </ol>

        {!loading && resolutions.length > 0 && (
          <Resolutions items={resolutions} />
        )}

        {error && <div className="alert error">{error}</div>}

        {!loading && !error && briefing && (
          <div className="briefing">
            <div className="headline-wrap">
              <h2>{briefing.headline || "Briefing card"}</h2>
              <div className="chip-row">
                {toolNames.map((toolName: string) => (
                  <span key={toolName} className="chip">
                    {toolName}
                  </span>
                ))}
              </div>
            </div>

            <p className="summary">{briefing.summary}</p>

            {briefing.sections?.map((section, index) => {
              const phases = extractPhaseRows(section.data);
              const scalars = scalarEntries(section.data);
              return (
                <section key={`${section.title}-${index}`} className="section">
                  <h3>
                    {section.title}
                    {section.tool && <code className="tool-tag">{section.tool}</code>}
                  </h3>

                  {scalars.length > 0 && (
                    <div className="stat-grid">
                      {scalars.map(([label, value]) => (
                        <div className="stat" key={label}>
                          <span className="stat-value">{value}</span>
                          <span className="stat-label">{label}</span>
                        </div>
                      ))}
                    </div>
                  )}

                  {phases && <PhaseTable rows={phases} />}

                  {scalars.length === 0 && !phases && <p>{section.summary}</p>}
                </section>
              );
            })}

            {briefing.ai_summary && (
              <section className="section ai-section">
                <h3>AI commentary</h3>
                <p className="ai-note">
                  Generated from tool output only — every figure above comes from SQL.
                </p>
                {briefing.ai_summary
                  .split("\n")
                  .filter((line) => line.trim())
                  .map((line, i) => (
                    <p key={i}>{line.replace(/\*\*/g, "")}</p>
                  ))}
              </section>
            )}

            <button
              type="button"
              className="raw-toggle"
              onClick={() => setShowRaw((v) => !v)}
            >
              {showRaw ? "Hide" : "Show"} raw response
            </button>
            {showRaw && (
              <pre className="raw-json">{JSON.stringify(result, null, 2)}</pre>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
