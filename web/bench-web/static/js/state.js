// The client-side store: turns the WebSocket's `Event` stream into a small set of named buffers
// (feed, rate, stats history, status, metrics, connection) and notifies render modules via
// `EventTarget`. No DOM access happens here — `app.js` wires `Store` events to the `render/*`
// modules, which is the only place the DOM gets touched. Kept this way so the event-to-state
// reduction (what a `Snapshot` replaces, how the feed ring buffer trims, how the stats history
// caps at 120) is independent of how it ends up on screen.

const FEED_CAPACITY = 50;
const STATS_HISTORY_CAPACITY = 120;

export class Store extends EventTarget {
  constructor() {
    super();
    this.connection = 'connecting'; // 'connecting' | 'connected' | 'disconnected'
    this.status = { cluster: 'unknown', pipeline: 'unknown', load: 'stopped' };
    this.rate = { rowsPerSecIn: 0, rowsPerSecRequested: 0, alertsPerSecOut: 0 };
    this.feed = []; // newest first, capped at FEED_CAPACITY
    this.statsHistory = []; // oldest first, capped at STATS_HISTORY_CAPACITY
    this.metrics = null; // null until the first Metrics event arrives (Task 7; may never arrive)
    this.lastLog = null;
  }

  setConnection(state) {
    this.connection = state;
    this.dispatchEvent(new CustomEvent('connection'));
  }

  /** Routes one decoded server `Event` to the right reducer. `Snapshot` recurses into its parts. */
  handleEvent(ev) {
    switch (ev.type) {
      case 'alert':
        this._pushAlert(ev);
        break;
      case 'rate':
        this.rate = {
          rowsPerSecIn: ev.rows_per_sec_in,
          rowsPerSecRequested: ev.rows_per_sec_requested,
          alertsPerSecOut: ev.alerts_per_sec_out,
        };
        this.dispatchEvent(new CustomEvent('rate'));
        break;
      case 'stats':
        this._pushStats(ev);
        break;
      case 'status':
        this.status = { cluster: ev.cluster, pipeline: ev.pipeline, load: ev.load };
        this.dispatchEvent(new CustomEvent('status'));
        break;
      case 'metrics':
        this.metrics = {
          matchesEmitted: ev.matches_emitted,
          evictedRows: ev.evicted_rows,
          scanBudgetExhausted: ev.scan_budget_exhausted,
        };
        this.dispatchEvent(new CustomEvent('metrics'));
        break;
      case 'stats_reset':
        // New measurement epoch (load started / pipeline rebuilt): the chart restarts so its
        // percentiles describe the current run.
        this.statsHistory = [];
        this.dispatchEvent(new CustomEvent('stats'));
        break;
      case 'probe':
        this.dispatchEvent(new CustomEvent('probe', { detail: { round: ev.round, latencyMs: ev.latency_ms } }));
        break;
      case 'snapshot':
        this._applySnapshot(ev);
        break;
      case 'log':
        this.lastLog = { level: ev.level, text: ev.text };
        this.dispatchEvent(new CustomEvent('log'));
        break;
      default:
        // Unknown event types are ignored rather than thrown on: a future server addition must
        // not break an older cached page.
        break;
    }
  }

  _pushAlert(ev) {
    this.feed.unshift(ev);
    if (this.feed.length > FEED_CAPACITY) this.feed.length = FEED_CAPACITY;
    // `fromSnapshot: false` — a genuine live arrival, the one case the feed's entrance animation
    // should play for (see render/feed.js's rowFor).
    this.dispatchEvent(new CustomEvent('feed', { detail: { fromSnapshot: false } }));
  }

  _pushStats(ev) {
    this.statsHistory.push(ev);
    if (this.statsHistory.length > STATS_HISTORY_CAPACITY) this.statsHistory.shift();
    this.dispatchEvent(new CustomEvent('stats'));
  }

  /** A `Snapshot` replaces status, the feed, and stats wholesale — it is sent on connect and
   * after a client falls behind, so partial-merge would leave stale entries mixed with fresh
   * ones. */
  _applySnapshot(ev) {
    if (ev.status) {
      this.status = { cluster: ev.status.cluster, pipeline: ev.status.pipeline, load: ev.status.load };
    }
    // `recent` arrives oldest-first (see AppState::recent_snapshot); the feed renders newest
    // first, so reverse it once here rather than in the renderer.
    this.feed = [...ev.recent].reverse().slice(0, FEED_CAPACITY);
    if (ev.stats) {
      this.statsHistory = [ev.stats];
    }
    this.dispatchEvent(new CustomEvent('status'));
    // `fromSnapshot: true` — these rows are history being restored (initial connect, or a lag
    // resync), not something happening live in front of the viewer, so they must not animate in;
    // see render/feed.js's rowFor for why that distinction is the actual fix, not just polish.
    this.dispatchEvent(new CustomEvent('feed', { detail: { fromSnapshot: true } }));
    this.dispatchEvent(new CustomEvent('stats'));
  }
}
