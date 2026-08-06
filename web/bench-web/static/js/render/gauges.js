// The rows/s speedometer (with the requested rate marked, so a generator falling behind is
// visible) and the alerts/s number.

import { formatInt } from '../format.js';

/** Scale the gauge track to comfortably fit whichever of "observed" or "requested" is larger, so
 * a generator that's falling behind (observed well under requested) is visibly short of the
 * target marker rather than pinned at 100%. */
function scaleFor(observed, requested) {
  return Math.max(observed * 1.15, requested * 1.15, 1000);
}

export function renderRowsGauge(els, rate) {
  const { valueEl, captionEl, fillEl, targetEl } = els;
  valueEl.textContent = formatInt(rate.rowsPerSecIn);
  captionEl.textContent = `requested ${formatInt(rate.rowsPerSecRequested)}/s`;

  const scale = scaleFor(rate.rowsPerSecIn, rate.rowsPerSecRequested);
  const fillPct = Math.min(100, (rate.rowsPerSecIn / scale) * 100);
  const targetPct = Math.min(100, (rate.rowsPerSecRequested / scale) * 100);
  fillEl.style.width = `${fillPct}%`;
  targetEl.style.left = `${targetPct}%`;
}

export function renderAlertsGauge(valueEl, rate) {
  valueEl.textContent = formatInt(rate.alertsPerSecOut);
}
