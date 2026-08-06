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
import { renderConnectionBadge, renderStatus, renderMetrics } from './js/render/status.js';
import { renderLog } from './js/render/log.js';

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
  gaugeRowsFill: document.getElementById('gauge-rows-fill'),
  gaugeRowsTarget: document.getElementById('gauge-rows-target'),
  gaugeAlertsValue: document.getElementById('gauge-alerts-value'),

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
  renderStatus({ clusterEl: dom.statusCluster, pipelineEl: dom.statusPipeline, loadEl: dom.statusLoad }, store.status),
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
      fillEl: dom.gaugeRowsFill,
      targetEl: dom.gaugeRowsTarget,
    },
    store.rate,
  );
  renderAlertsGauge(dom.gaugeAlertsValue, store.rate);
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

// Initial paint before the first events arrive, so the page isn't visually empty for a moment.
renderConnectionBadge(dom.connBadge, store.connection);
renderStatus({ clusterEl: dom.statusCluster, pipelineEl: dom.statusPipeline, loadEl: dom.statusLoad }, store.status);
renderFeedCaption(dom.feedCaption, 0);
renderLatencyChart(dom.latencyChart, dom.latencyCaption, store.statsHistory);
