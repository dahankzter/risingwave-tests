// Connection badge, the three status chips, and the metrics card (hidden until the first
// `Metrics` event actually arrives — see state.js's doc comment on why zeros must not be faked).

const CONNECTION_LABEL = {
  connecting: 'connecting…',
  connected: 'connected',
  disconnected: 'disconnected — reconnecting…',
};

export function renderConnectionBadge(badgeEl, connection) {
  badgeEl.textContent = CONNECTION_LABEL[connection] ?? connection;
  badgeEl.className = `badge badge--${connection}`;
}

// Which words mean "this part of the pipeline is working". Everything else is either a definite
// no (amber) or genuinely unknown (grey). The chips are the durable answer to "did that button
// do anything" — the log strip only holds the most recent line, and a background message can
// overwrite an action's confirmation a second after it appears.
const GOOD = new Set(['up', 'present', 'running', 'rebuilt', 'finished']);
const BAD = new Set(['down', 'absent', 'failed']);

function state(value) {
  if (GOOD.has(value)) return 'good';
  if (BAD.has(value)) return 'bad';
  return 'unknown';
}

export function renderStatus(els, status) {
  for (const [el, value] of [
    [els.clusterEl, status.cluster],
    [els.pipelineEl, status.pipeline],
    [els.loadEl, status.load],
  ]) {
    el.textContent = value;
    // The attribute goes on the chip, not the value, so the whole chip can tint.
    const chip = el.closest('.status-chip') ?? el;
    chip.dataset.state = state(value);
  }
}

export function renderMetrics(els, metrics) {
  if (!metrics) {
    els.cardEl.hidden = true;
    return;
  }
  els.cardEl.hidden = false;
  els.matchesEl.textContent = metrics.matchesEmitted.toLocaleString('en-US');
  els.evictedEl.textContent = metrics.evictedRows.toLocaleString('en-US');
  els.exhaustedEl.textContent = metrics.scanBudgetExhausted.toLocaleString('en-US');
}
