// Hand-rolled canvas line chart for the last ~120 `Stats` samples' p50/p95, with labelled axes.
// No charting library (no CDN, no build step), so this draws the plot area, gridlines, tick labels
// and two polylines directly on a 2D context. Colours are read from the M3 tokens at draw time via
// `getComputedStyle` so the chart follows the active scheme (including a live OS theme switch)
// without duplicating hex values here.

function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

// Room for the y labels on the left and the x labels underneath. In CSS pixels; scaled by dpr.
const PAD = { left: 38, right: 6, top: 8, bottom: 16 };

/** Seconds per sample, so the x axis can be labelled in elapsed time rather than sample counts.
 * The aggregator publishes `Stats` on a fixed tick; this must match `AGG_TICK` in ws.rs. */
const SECONDS_PER_SAMPLE = 0.25;

/** A tick step from the 1/2/5 ladder that yields about `target` divisions — so labels read 200,
 * 500, 1000 rather than 333 or 1666. */
function niceStep(range, target = 4) {
  const raw = range / target;
  const mag = 10 ** Math.floor(Math.log10(raw));
  for (const m of [1, 2, 5, 10]) {
    if (m * mag >= raw) return m * mag;
  }
  return 10 * mag;
}

/** Latency axes are read in seconds once past a second — 6.3s beats 6300ms for a value an
 * operator compares against a watermark expressed in seconds. */
function formatMs(ms) {
  if (ms >= 1000) {
    const s = ms / 1000;
    return `${s >= 10 ? s.toFixed(0) : s.toFixed(1)}s`;
  }
  return `${Math.round(ms)}ms`;
}

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
  return dpr;
}

function drawLine(ctx, points, color, plot, maxMs) {
  if (points.length < 2) return;
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((ms, i) => {
    const x = plot.x + (i / (points.length - 1)) * plot.w;
    const y = plot.y + plot.h - (ms / maxMs) * plot.h;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

export function renderLatencyChart(canvas, captionEl, statsHistory) {
  const dpr = sizeToDisplay(canvas);
  const ctx = canvas.getContext('2d');
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (statsHistory.length === 0) {
    captionEl.textContent = 'no samples yet';
    return;
  }

  ctx.save();
  ctx.scale(dpr, dpr); // draw in CSS pixels from here on
  const cw = w / dpr;
  const ch = h / dpr;
  const plot = {
    x: PAD.left,
    y: PAD.top,
    w: Math.max(10, cw - PAD.left - PAD.right),
    h: Math.max(10, ch - PAD.top - PAD.bottom),
  };

  const p50 = statsHistory.map((s) => s.p50_ms);
  const p95 = statsHistory.map((s) => s.p95_ms);
  // Round the top of the scale up to a tick boundary so the highest gridline is the frame's top
  // edge rather than an arbitrary 1.1x of the peak.
  const peak = Math.max(...p95, ...p50, 1);
  const step = niceStep(peak, 4);
  const maxMs = Math.ceil(peak / step) * step;

  const gridColor = cssVar('--md-sys-color-outline-variant');
  const labelColor = cssVar('--md-sys-color-on-surface-variant');
  ctx.font = '10px system-ui, sans-serif';
  ctx.textBaseline = 'middle';

  // y axis: a gridline and a label per tick, in latency units.
  ctx.strokeStyle = gridColor;
  ctx.lineWidth = 1;
  for (let v = 0; v <= maxMs + 1e-9; v += step) {
    const y = plot.y + plot.h - (v / maxMs) * plot.h;
    ctx.beginPath();
    ctx.moveTo(plot.x, y);
    ctx.lineTo(plot.x + plot.w, y);
    ctx.stroke();
    ctx.fillStyle = labelColor;
    ctx.textAlign = 'right';
    ctx.fillText(formatMs(v), plot.x - 5, y);
  }

  // x axis: elapsed time, oldest sample on the left. Only the ends are labelled — the axis exists
  // to say "how far back does this window reach", not to locate individual samples.
  const spanSec = (statsHistory.length - 1) * SECONDS_PER_SAMPLE;
  ctx.fillStyle = labelColor;
  ctx.textAlign = 'left';
  ctx.fillText(`-${spanSec.toFixed(0)}s`, plot.x, plot.y + plot.h + 8);
  ctx.textAlign = 'right';
  ctx.fillText('now', plot.x + plot.w, plot.y + plot.h + 8);

  drawLine(ctx, p50, cssVar('--md-sys-color-primary'), plot, maxMs);
  drawLine(ctx, p95, cssVar('--md-sys-color-tertiary'), plot, maxMs);
  ctx.restore();

  const latest = statsHistory[statsHistory.length - 1];
  captionEl.textContent =
    `p50 ${formatMs(latest.p50_ms)} · p95 ${formatMs(latest.p95_ms)} ` +
    `(n=${latest.n}) — solid p50, second line p95 · y: latency, x: last ${spanSec.toFixed(0)}s`;
}
