// Entry point. Wires DOM element lookups, the WebSocket, the store, the render modules, and the
// controls together. Deliberately thin — every piece of actual logic lives in `js/*`, so this
// file is mostly "grab elements, subscribe, call a render function."

import { Store } from './js/state.js';
import { ReconnectingSocket } from './js/socket.js';
import * as api from './js/api.js';
import { wireControls } from './js/controls.js';
import { renderFeed, renderFeedCaption } from './js/render/feed.js';
import { renderRowsGauge, renderAlertsGauge } from './js/render/gauges.js';
import { renderLatencyChart } from './js/render/chart.js';
import { renderConnectionBadge, renderStatus, renderMetrics, renderLateness } from './js/render/status.js';
import { renderLog } from './js/render/log.js';
import { wireDetails } from './js/render/details.js';
import { renderScenarioList } from './js/render/scenarios.js';

// A genuinely uncaught exception (a bug in a render callback, say) must not fail silently the
// way a dropped WebSocket must not: surface it on the page itself, in the same log strip used for
// server `Log` events and control-call results, rather than only in a devtools console nobody at
// the demo is looking at.
window.addEventListener('error', (e) => {
  const strip = document.getElementById('log-strip');
  if (strip) {
    strip.textContent = `[error] ${e.message} (${e.filename}:${e.lineno}:${e.colno})`;
    strip.dataset.level = 'error';
  }
});
window.addEventListener('unhandledrejection', (e) => {
  const strip = document.getElementById('log-strip');
  if (strip) {
    strip.textContent = `[error] unhandled rejection: ${e.reason}`;
    strip.dataset.level = 'error';
  }
});

const dom = {
  connBadge: document.getElementById('conn-badge'),
  statusCluster: document.getElementById('status-cluster'),
  statusPipeline: document.getElementById('status-pipeline'),
  statusLoad: document.getElementById('status-load'),
  logStrip: document.getElementById('log-strip'),

  feedList: document.getElementById('feed-list'),
  feedCaption: document.getElementById('feed-caption'),

  gaugeRowsValue: document.getElementById('gauge-rows-value'),
  gaugeRowsCaption: document.getElementById('gauge-rows-caption'),
  gaugeRowsDial: document.getElementById('gauge-rows-dial'),
  gaugeAlertsValue: document.getElementById('gauge-alerts-value'),
  gaugeAlertsDial: document.getElementById('gauge-alerts-dial'),
  latenessSelect: document.getElementById('lateness-select'),
  chipLateness: document.getElementById('chip-lateness'),
  statusLateness: document.getElementById('status-lateness'),
  scenarioList: document.getElementById('scenario-list'),
  btnScenarioClose: document.getElementById('btn-scenario-close'),
  scenarioPanel: document.getElementById('scenario-panel'),
  scenarioTitle: document.getElementById('scenario-title'),
  scenarioOutput: document.getElementById('scenario-output'),

  latencyChart: document.getElementById('latency-chart'),
  latencyCaption: document.getElementById('latency-caption'),

  metricsCard: document.getElementById('metrics-card'),
  metricMatches: document.getElementById('metric-matches'),
  metricEvicted: document.getElementById('metric-evicted'),
  metricExhausted: document.getElementById('metric-exhausted'),

  btnClusterUp: document.getElementById('btn-cluster-up'),
  btnClusterDown: document.getElementById('btn-cluster-down'),
  btnPipelineRebuild: document.getElementById('btn-pipeline-rebuild'),
  btnLoadStart: document.getElementById('btn-load-start'),
  btnLoadStop: document.getElementById('btn-load-stop'),
  btnProbeStart: document.getElementById('btn-probe-start'),
  rateSlider: document.getElementById('rate-slider'),
  rateValue: document.getElementById('rate-value'),
  btnCleanOpen: document.getElementById('btn-cluster-clean'),
  cleanDialog: document.getElementById('clean-dialog'),
  cleanForm: document.getElementById('clean-form'),
  cleanInput: document.getElementById('clean-input'),
  cleanConfirm: document.getElementById('clean-confirm'),
  cleanCancel: document.getElementById('clean-cancel'),
};

const store = new Store();

// ---- render wiring: each store event re-renders exactly the DOM it owns -------------------------

store.addEventListener('connection', () => renderConnectionBadge(dom.connBadge, store.connection));

store.addEventListener('status', () =>
  renderStatusAndLateness(),
);

store.addEventListener('feed', (ev) => {
  renderFeed(dom.feedList, store.feed, !ev.detail.fromSnapshot);
  renderFeedCaption(dom.feedCaption, store.rate.alertsPerSecOut);
});

store.addEventListener('rate', () => {
  renderRowsGauge(
    {
      valueEl: dom.gaugeRowsValue,
      captionEl: dom.gaugeRowsCaption,
      dialEl: dom.gaugeRowsDial,
    },
    store.rate,
  );
  renderAlertsGauge({ valueEl: dom.gaugeAlertsValue, dialEl: dom.gaugeAlertsDial }, store.rate);
  renderFeedCaption(dom.feedCaption, store.rate.alertsPerSecOut);
});

store.addEventListener('stats', () =>
  renderLatencyChart(dom.latencyChart, dom.latencyCaption, store.statsHistory),
);

store.addEventListener('metrics', () =>
  renderMetrics(
    {
      cardEl: dom.metricsCard,
      matchesEl: dom.metricMatches,
      evictedEl: dom.metricEvicted,
      exhaustedEl: dom.metricExhausted,
    },
    store.metrics,
  ),
);

store.addEventListener('log', () => renderLog(dom.logStrip, store.lastLog));

// `probe` events (POST /api/probe/start's per-round results) have no dedicated card — the log
// strip is the one place on the page for "here is what just happened," same as showMessage below.
store.addEventListener('probe', (ev) => {
  const { round, latencyMs } = ev.detail;
  renderLog(dom.logStrip, { level: 'info', text: `probe round ${round}: ${latencyMs} ms` });
});

// A locally-originated message (a control call's result, including errors like "409 a load is
// already running") reuses the same log strip, since it is the one place on the page dedicated
// to "here is what just happened."
function showMessage(level, text) {
  renderLog(dom.logStrip, { level, text });
}

// ---- socket ---------------------------------------------------------------------------------

const socket = new ReconnectingSocket(
  (ev) => store.handleEvent(ev),
  (state) => store.setConnection(state),
);
socket.connect();

// ---- controls ---------------------------------------------------------------------------------

wireControls(dom, api, showMessage);
// The details tab is self-wiring: it owns its toggle, its own poll timer (only while visible),
// and its own store subscriptions.
wireDetails(store);

// Tabs: one visible view at a time. The details tab keeps its own polling — see details.js — so
// switching away from it stops that work rather than leaving it running behind a hidden panel.
const TABS = [
  ['tab-live', 'view-live'],
  ['tab-correctness', 'view-correctness'],
  ['tab-details', 'view-details'],
];
function selectTab(activeId) {
  for (const [tabId, viewId] of TABS) {
    const tab = document.getElementById(tabId);
    const view = document.getElementById(viewId);
    if (!tab || !view) continue;
    const active = tabId === activeId;
    tab.setAttribute('aria-selected', String(active));
    view.hidden = !active;
  }
}
for (const [tabId] of TABS) {
  document.getElementById(tabId)?.addEventListener('click', () => selectTab(tabId));
}
selectTab('tab-live');

// The check cards, built once from the server's list (name + the prose each scenario file opens
// with).
api.scenarioList().then((scenarios) => renderScenarioList(dom, api, scenarios, showMessage));

// Initial paint before the first events arrive, so the page isn't visually empty for a moment.
/** The two header renderers that share a trigger: a status event carries the live lateness, and
 * the selector's own change must re-render the comparison immediately rather than waiting for the
 * next poll. */
function renderStatusAndLateness() {
  renderStatus(
    { clusterEl: dom.statusCluster, pipelineEl: dom.statusPipeline, loadEl: dom.statusLoad },
    store.status,
  );
  renderLateness(
    { chipEl: dom.chipLateness, valueEl: dom.statusLateness, rebuildBtn: dom.btnPipelineRebuild },
    store.liveLateness ?? null,
    dom.latenessSelect?.value ?? '',
  );
}
dom.latenessSelect?.addEventListener('change', renderStatusAndLateness);

renderConnectionBadge(dom.connBadge, store.connection);
renderStatusAndLateness();
renderFeedCaption(dom.feedCaption, 0);
renderLatencyChart(dom.latencyChart, dom.latencyCaption, store.statsHistory);
