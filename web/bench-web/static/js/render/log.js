// The thin log strip under the top bar: shows the most recent `Log` event so `warn`/`error`
// lines (a lost cursor, a rejected clean, a reconnect) don't vanish silently.

export function renderLog(stripEl, log) {
  if (!log) {
    stripEl.textContent = '';
    stripEl.removeAttribute('data-level');
    return;
  }
  stripEl.textContent = `[${log.level}] ${log.text}`;
  stripEl.dataset.level = log.level;
}
