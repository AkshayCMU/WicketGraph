// Falls back to the docker-compose host port (see docker-compose.yml).
// 8000 is deliberately avoided — it is commonly taken by other local projects.
const DEFAULT_API_BASE = "http://localhost:8100";

export const API_BASE = (import.meta.env.VITE_API_BASE || DEFAULT_API_BASE).replace(/\/$/, "");

export async function fetchBriefing(question: string) {
  const response = await fetch(`${API_BASE}/briefing`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(errorText || `Request failed with status ${response.status}`);
  }

  return response.json();
}
