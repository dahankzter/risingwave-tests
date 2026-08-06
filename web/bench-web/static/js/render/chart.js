// Hand-rolled canvas line chart for the last ~120 `Stats` samples' p50/p95. No charting library
// (no CDN, no build step), so this draws two polylines directly on a 2D context. Colours are read
// from the M3 tokens at draw time via `getComputedStyle` so the chart follows the active scheme
// (including a live OS theme switch) without duplicating hex values here.

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function drawLine(ctx, points, color, w, h, maxMs) {
  if (points.length < 2) return;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((ms, i) => {
    const x = (i / (points.length - 1)) * w;
    const y = h - (ms / maxMs) * h;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

/** Matches the canvas's backing resolution to its CSS size (times devicePixelRatio) so the chart
 * stays crisp regardless of the card's actual rendered width — the CSS sets `width: 100%`, so the
 * `width`/`height` attributes alone would otherwise stretch a fixed-resolution bitmap. */
function sizeToDisplay(canvas) {
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth || 360;
  const cssHeight = canvas.clientHeight || 160;
  const targetW = Math.round(cssWidth * dpr);
  const targetH = Math.round(cssHeight * dpr);
  if (canvas.width !== targetW || canvas.height !== targetH) {
    canvas.width = targetW;
    canvas.height = targetH;
  }
}

export function renderLatencyChart(canvas, captionEl, statsHistory) {
  sizeToDisplay(canvas);
  const ctx = canvas.getContext('2d');
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (statsHistory.length === 0) {
    captionEl.textContent = 'no samples yet';
    return;
  }

  const p50 = statsHistory.map((s) => s.p50_ms);
  const p95 = statsHistory.map((s) => s.p95_ms);
  const maxMs = Math.max(...p95, ...p50, 1) * 1.1;

  // Gridline at the halfway mark, for scale — decorative, so it uses outline-variant directly
  // rather than needing its own AA check (it carries no text).
  ctx.strokeStyle = cssVar('--md-sys-color-outline-variant');
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, h / 2);
  ctx.lineTo(w, h / 2);
  ctx.stroke();

  drawLine(ctx, p50, cssVar('--md-sys-color-primary'), w, h, maxMs);
  drawLine(ctx, p95, cssVar('--md-sys-color-tertiary'), w, h, maxMs);

  const latest = statsHistory[statsHistory.length - 1];
  captionEl.textContent =
    `p50 ${(latest.p50_ms / 1000).toFixed(1)}s · p95 ${(latest.p95_ms / 1000).toFixed(1)}s ` +
    `(n=${latest.n}) — primary line p50, tertiary line p95`;
}
