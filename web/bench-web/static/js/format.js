// Formatting and severity-bucketing helpers shared by the render modules. Kept dependency-free
// (no DOM, no state) so each is trivially testable in a console if needed.

/** Rounds to an integer and inserts thousands separators, e.g. 12345 -> "12,345". */
export function formatInt(n) {
  return Math.round(n).toLocaleString('en-US');
}

/** One decimal place, e.g. 1234.5 -> "1,234.5". */
export function formatFixed1(n) {
  return n.toLocaleString('en-US', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

/** Milliseconds as a compact human string: "842ms" under a second, "6.3s" at or above. */
export function formatLatency(ms) {
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

/**
 * Buckets an alert's latency into a severity that is never conveyed by colour alone — every
 * caller also renders `label` as text next to the colour swatch. Thresholds are set around the
 * benchmark's own target (p50 settles near 6s under normal load): comfortably under that is
 * "ok", up to double is "slow" (queue building but not alarming), beyond that is "late".
 */
export function severityFor(latencyMs) {
  if (latencyMs < 8000) return { cls: 'ok', label: 'ok' };
  if (latencyMs < 15000) return { cls: 'slow', label: 'slow' };
  return { cls: 'late', label: 'late' };
}

/** `alert_ts` (an RFC3339 string) to a short local time-of-day string for the feed row. */
export function formatTimeOfDay(iso) {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toLocaleTimeString('en-US', { hour12: false });
}
