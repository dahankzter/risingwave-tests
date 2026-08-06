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

function buildRow(alert) {
  const li = document.createElement('li');
  li.className = 'feed-row';

  const severity = severityFor(alert.latency_ms, alert.stale);
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

// Caches the rendered <li> for each alert object, keyed by the object's own identity. This is
// what makes "new rows animate in; existing rows don't" actually true: `state.js` keeps each
// alert's object reference stable across renders (a live alert is `unshift`ed once and never
// rebuilt; only a `Snapshot` — an initial connect or a lag resync — replaces the array wholesale
// with fresh objects). Rebuilding every row from scratch on every tick, as an earlier version of
// this file did via `replaceChildren(...alerts.map(buildRow))`, tore down and recreated all 50
// `<li>` elements on every single new alert. Since `.feed-row`'s entrance animation starts at
// `opacity: 0`, that meant the *entire* feed was perpetually mid-fade-in at a ~20/s tick rate —
// not just a performance problem, but a real correctness one: any paint caught between two ticks
// (a freshly loaded page's first frame, a headless screenshot, a slow monitor) could catch every
// row at or near invisible, which is exactly what happened — see the task report's fix-round
// notes. Reusing the cached node for an alert that's still present means `replaceChildren` only
// *repositions* it (a no-op for CSS animations, which restart only on element creation, not on a
// DOM move), so only a row's first appearance ever animates.
const rowCache = new WeakMap();

/** `animate`: whether a row seen here for the first time should play the entrance animation.
 * `false` for a `Snapshot` (page load, or a lag resync) — those rows are history being restored,
 * not something arriving in front of the viewer — `true` for a genuine live `Alert`. */
function rowFor(alert, index, animate) {
  let li = rowCache.get(alert);
  if (!li) {
    li = buildRow(alert);
    if (animate) {
      li.classList.add('feed-row--enter');
      li.addEventListener('animationend', () => li.classList.remove('feed-row--enter'), { once: true });
    }
    rowCache.set(alert, li);
  }
  // Age (and therefore the dimmed-text tier) depends on position, which does change release over
  // release even for a row whose content doesn't — update it every render regardless of cache hit.
  li.dataset.age = String(ageFor(index));
  return li;
}

/** Replaces the feed list's contents with `alerts` (newest first), reusing cached nodes for
 * alerts already on screen — see `rowFor`'s doc comment for why that matters beyond performance.
 * `animate` controls whether a row seen for the first time here plays the entrance animation;
 * pass `false` when `alerts` came from a `Snapshot` rather than a live `Alert`. */
export function renderFeed(listEl, alerts, animate = true) {
  listEl.replaceChildren(...alerts.map((alert, index) => rowFor(alert, index, animate)));
}

export function renderFeedCaption(captionEl, alertsPerSecOut) {
  const n = Math.max(0, Math.round(alertsPerSecOut));
  captionEl.textContent = `showing ~20 of ~${n} alerts/s · percentiles cover all`;
}
