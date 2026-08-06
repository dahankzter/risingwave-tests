// The details tab: trustworthy-numbers view. Latency/throughput mirrors the live store; operator
// metrics come from the Metrics event; pipeline state and the run environment are fetched from
// their endpoints while the tab is open. The environment panel is the honesty layer — a
// screenshot of these panels carries its own caveats (emulated, unpinned, too few cores).

const POLL_MS = 5000;

function set(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

const ms = (v) => (v == null ? '—' : `${Math.round(v)} ms`);
const num = (v) => (v == null ? '—' : Number(v).toLocaleString());

export function renderDetailsStats(store) {
  const s = store.statsHistory.at(-1);
  set('det-p50', s ? ms(s.p50_ms) : '—');
  set('det-p95', s ? ms(s.p95_ms) : '—');
  set('det-p99', s ? ms(s.p99_ms) : '—');
  set('det-n', s ? num(s.n) : '—');
  const r = store.rate;
  set('det-rows', r ? num(r.rows_per_sec_in) : '—');
  set('det-alerts', r ? num(r.alerts_per_sec_out) : '—');
}

export function renderDetailsMetrics(store) {
  const m = store.metrics;
  set('det-m-matches', m ? num(m.matches_emitted) : '—');
  set('det-m-evicted', m ? num(m.evicted_rows) : '—');
  set('det-m-exhausted', m ? num(m.scan_budget_exhausted) : '—');
}

async function refreshPipeline() {
  try {
    const res = await fetch('/api/pipeline/stats');
    const p = await res.json();
    set('det-p-base', num(p.base_rows));
    set('det-p-matches', num(p.matches));
    set('det-p-alerts', num(p.alert_rows));
  } catch {
    /* down cluster: leave dashes */
  }
}

async function refreshEnv() {
  try {
    const res = await fetch('/api/env');
    const e = await res.json();
    set('det-e-image', e.image);
    set('det-e-host', `${e.host_os}/${e.host_arch}${e.emulated ? ' (emulated)' : ''}`);
    set('det-e-cores', String(e.cores));
    set('det-e-pin', e.pin_why);
    const trust = document.getElementById('det-e-trust');
    if (trust) {
      if (e.trusted) {
        trust.textContent = 'measurement-grade environment';
        trust.dataset.trust = 'ok';
      } else {
        trust.textContent = `shape-check only: ${e.reasons.join('; ')}`;
        trust.dataset.trust = 'warn';
      }
    }
  } catch {
    /* server unreachable: nothing to label */
  }
}

/** Wire the toggle button; poll the fetched panels only while the tab is visible. */
export function wireDetails(store) {
  const btn = document.getElementById('btn-details');
  const panel = document.getElementById('details-panel');
  if (!btn || !panel) return;

  let timer = null;
  const refresh = () => {
    renderDetailsStats(store);
    renderDetailsMetrics(store);
    refreshPipeline();
  };
  btn.addEventListener('click', () => {
    const open = panel.hidden;
    panel.hidden = !open;
    btn.setAttribute('aria-expanded', String(open));
    if (open) {
      refreshEnv();
      refresh();
      timer = setInterval(refresh, POLL_MS);
    } else if (timer) {
      clearInterval(timer);
      timer = null;
    }
  });

  // Live values piggyback on store events while open.
  store.addEventListener('stats', () => {
    if (!panel.hidden) renderDetailsStats(store);
  });
  store.addEventListener('metrics', () => {
    if (!panel.hidden) renderDetailsMetrics(store);
  });
}
