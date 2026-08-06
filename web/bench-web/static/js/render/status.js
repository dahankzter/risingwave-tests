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

export function renderStatus(els, status) {
  els.clusterEl.textContent = status.cluster;
  els.pipelineEl.textContent = status.pipeline;
  els.loadEl.textContent = status.load;
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
