// Owns the one WebSocket connection to `/ws`. Reconnects automatically with exponential backoff
// (capped) whenever the socket closes or errors — a demo that dies silently on a dropped socket
// is worse than one that says it dropped, so `onConnectionChange` is called on every transition
// and the caller (app.js) wires it straight to the connection badge.

const BACKOFF_BASE_MS = 500;
const BACKOFF_MAX_MS = 10_000;

export class ReconnectingSocket {
  /**
   * @param {(event: object) => void} onEvent - called with each decoded server `Event`.
   * @param {(state: 'connecting'|'connected'|'disconnected') => void} onConnectionChange
   */
  constructor(onEvent, onConnectionChange) {
    this.onEvent = onEvent;
    this.onConnectionChange = onConnectionChange;
    this.attempt = 0;
    this.socket = null;
    this.closedByUser = false;
  }

  connect() {
    this.closedByUser = false;
    this._open();
  }

  close() {
    this.closedByUser = true;
    this.socket?.close();
  }

  _open() {
    this.onConnectionChange('connecting');
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const socket = new WebSocket(`${proto}//${window.location.host}/ws`);
    this.socket = socket;

    socket.addEventListener('open', () => {
      this.attempt = 0;
      this.onConnectionChange('connected');
    });

    socket.addEventListener('message', (msg) => {
      let ev;
      try {
        ev = JSON.parse(msg.data);
      } catch {
        return; // A malformed frame must not take the UI down; just drop it.
      }
      this.onEvent(ev);
    });

    socket.addEventListener('close', () => this._scheduleReconnect());
    socket.addEventListener('error', () => socket.close());
  }

  _scheduleReconnect() {
    if (this.closedByUser) return;
    this.onConnectionChange('disconnected');
    const delay = Math.min(BACKOFF_BASE_MS * 2 ** this.attempt, BACKOFF_MAX_MS);
    this.attempt += 1;
    setTimeout(() => {
      if (!this.closedByUser) this._open();
    }, delay);
  }
}
