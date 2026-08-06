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

/** The live watermark chip, and whether the selector disagrees with it. A selector left at 1s
 * while the table still says 5s is the trap this exists to close: changing a dropdown cannot
 * rebuild a pipeline on its own, and without this the page would keep reporting latencies whose
 * biggest component is not the number on screen. */
export function renderLateness(els, liveSecs, selectedRaw) {
  const { chipEl, valueEl, rebuildBtn } = els;
  if (!chipEl || !valueEl) return;
  if (liveSecs == null) {
    chipEl.hidden = true;
    if (rebuildBtn) rebuildBtn.textContent = 'rebuild pipeline';
    return;
  }
  chipEl.hidden = false;
  const selected = selectedRaw === '' ? 5 : Number(selectedRaw);
  const pending = selected !== liveSecs;
  valueEl.textContent = pending ? `${liveSecs}s \u2192 ${selected}s pending` : `${liveSecs}s`;
  chipEl.dataset.state = pending ? 'pending' : 'good';
  // Say what pressing the button will do, so the change is not silently unapplied.
  if (rebuildBtn) {
    rebuildBtn.textContent = pending ? `rebuild pipeline (${selected}s)` : 'rebuild pipeline';
  }
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
