// Renders the alert feed: newest at top, capped at 50 by the store. Builds DOM nodes with
// `document.createElement` / `textContent` — never `innerHTML` — because every field here
// (partition, chain length, latency, timestamp) comes off the wire from the server.

import { formatInt, formatLatency, formatTimeOfDay, severityFor } from '../format.js';

const AGE_BUCKETS = [10, 25]; // index < 10 -> age 0 (newest), < 25 -> age 1, else age 2

function ageFor(index) {
  if (index < AGE_BUCKETS[0]) return 0;
  if (index < AGE_BUCKETS[1]) return 1;
  return 2;
}

function buildRow(alert, index) {
  const li = document.createElement('li');
  li.className = 'feed-row';
  li.dataset.age = String(ageFor(index));

  const severity = severityFor(alert.latency_ms);
  const badge = document.createElement('span');
  badge.className = `feed-row__severity feed-row__severity--${severity.cls}`;
  badge.textContent = severity.label;

  const partition = document.createElement('span');
  partition.className = 'feed-row__partition';
  partition.textContent = `p${alert.partition}`;

  const chain = document.createElement('span');
  chain.className = 'feed-row__chain';
  chain.textContent = `chain ×${formatInt(alert.chain_len)} · ${formatTimeOfDay(alert.alert_ts)}`;

  const latency = document.createElement('span');
  latency.className = 'feed-row__latency';
  latency.textContent = formatLatency(alert.latency_ms);

  li.append(badge, partition, chain, latency);
  return li;
}

/** Replaces the feed list's contents with `alerts` (newest first). Rebuilding the whole list on
 * every update (rather than diffing) is simple and, at ~20 rows/s against a 50-row cap, cheap
 * enough — the list itself scrolls independently of the page (see `.feed-list` in style.css), so
 * this never causes a page reflow. */
export function renderFeed(listEl, alerts) {
  listEl.replaceChildren(...alerts.map(buildRow));
}

export function renderFeedCaption(captionEl, alertsPerSecOut) {
  const n = Math.max(0, Math.round(alertsPerSecOut));
  captionEl.textContent = `showing ~20 of ~${n} alerts/s · percentiles cover all`;
}
