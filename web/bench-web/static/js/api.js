// Thin wrappers over the control endpoints from `bench-web/src/api.rs`. Every function returns
// `{ ok, status, message }` rather than throwing, so callers (controls.js) can show a real
// message on failure (a 409 "a load is already running", a 400 from the clean confirmation gate)
// instead of a generic "something went wrong".

async function post(path, body) {
  let res;
  try {
    res = await fetch(path, {
      method: 'POST',
      headers: body === undefined ? {} : { 'content-type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { ok: false, status: 0, message: `network error: ${e.message}` };
  }
  const text = await res.text().catch(() => '');
  return { ok: res.ok, status: res.status, message: text || res.statusText };
}

export const clusterUp = () => post('/api/cluster/up');
export const clusterDown = () => post('/api/cluster/down');
export const clusterClean = () => post('/api/cluster/clean', { confirm: 'clean' });
// `lateness` in seconds, or null to keep the pipeline SQL's own declaration.
export const pipelineRebuild = (lateness = null) =>
  post('/api/pipeline/rebuild', lateness == null ? {} : { lateness_secs: lateness });

export const scenarioList = async () => {
  const res = await fetch('/api/scenarios');
  return res.ok ? res.json() : [];
};

export const scenarioRun = async (name) => {
  const res = await fetch('/api/scenarios/run', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name }),
  });
  return { ok: res.ok, body: await res.json().catch(() => null) };
};
/** Run hand-written SQL, statement at a time. Returns `{ ok, body }` like `scenarioRun` — a failing
 * statement is a result to display, not a transport error, so the body carries the failure. */
export const sqlRun = async (sql) => {
  const res = await fetch('/api/sql/run', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return { ok: res.ok, body: await res.json().catch(() => null) };
};

/** The user's own tables, views, sources and sinks. Empty when the cluster is down. */
export const catalog = async () => {
  try {
    const res = await fetch('/api/catalog');
    return res.ok ? await res.json() : [];
  } catch {
    return [];
  }
};

export const loadStart = (overrides = {}) => post('/api/load/start', overrides);
export const loadStop = () => post('/api/load/stop');
export const loadRate = (rate) => post('/api/load/rate', { rate });
export const probeStart = (rounds) => post('/api/probe/start', { rounds });

export async function getStatus() {
  try {
    const res = await fetch('/api/status');
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}
