// Round speedometer gauges for rows/s in and alerts/s out — dial, ticks, needle, and (for rows/s)
// a red target marker at the requested rate so a generator falling behind is visible as a needle
// short of the mark rather than as two numbers to compare.
//
// Drawn as SVG rather than canvas: the dial, ticks and labels are static, so they are built once
// and only the needle's transform changes per tick. That keeps a 4Hz update to one attribute
// write instead of a full repaint, and the text stays crisp at any zoom without devicePixelRatio
// bookkeeping.

import { formatInt } from '../format.js';

// A car-like sweep: 240° from lower-left, around through the top, to lower-right.
const START_DEG = 150;
const SWEEP_DEG = 240;
const R = 46; // dial radius in the 120x120 viewBox
const CX = 60;
const CY = 60;

/** Speedometers do not rescale continuously — a needle that means something different every
 * second is unreadable. This snaps the full-scale value to a 1/2/5 x 10^n ladder, and only when
 * the reading would otherwise leave the dial (or sits under a fifth of it), so the scale holds
 * still while the needle moves. */
export function niceScale(observed, requested = 0) {
  const need = Math.max(observed, requested, 1);
  const ladder = [1, 2, 5];
  let mag = 1;
  while (mag < 1e9) {
    for (const m of ladder) {
      const candidate = m * mag;
      if (candidate >= need * 1.05) return candidate;
    }
    mag *= 10;
  }
  return need;
}

function polar(deg, radius) {
  const rad = (deg * Math.PI) / 180;
  return [CX + radius * Math.cos(rad), CY + radius * Math.sin(rad)];
}

function fracToDeg(frac) {
  return START_DEG + Math.min(1, Math.max(0, frac)) * SWEEP_DEG;
}

const svgEl = (name, attrs) => {
  const el = document.createElementNS('http://www.w3.org/2000/svg', name);
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, String(v));
  return el;
};

/** The arc path from fraction `a` to fraction `b` of the sweep. */
function arcPath(a, b, radius) {
  const [x0, y0] = polar(fracToDeg(a), radius);
  const [x1, y1] = polar(fracToDeg(b), radius);
  const large = (b - a) * SWEEP_DEG > 180 ? 1 : 0;
  return `M ${x0} ${y0} A ${radius} ${radius} 0 ${large} 1 ${x1} ${y1}`;
}

/** Build the static dial once and hand back the live pieces to update per tick. */
function buildDial(host, { ticks = 6 } = {}) {
  host.textContent = '';
  const svg = svgEl('svg', {
    viewBox: '0 0 120 120',
    class: 'dial',
    role: 'img',
  });

  svg.append(svgEl('path', { d: arcPath(0, 1, R), class: 'dial__track' }));
  const value = svgEl('path', { d: arcPath(0, 0, R), class: 'dial__value' });
  svg.append(value);

  const tickGroup = svgEl('g', { class: 'dial__ticks' });
  const labelGroup = svgEl('g', { class: 'dial__labels' });
  svg.append(tickGroup, labelGroup);

  const target = svgEl('line', { class: 'dial__target', x1: 0, y1: 0, x2: 0, y2: 0 });
  svg.append(target);

  const needle = svgEl('g', { class: 'dial__needle' });
  needle.append(svgEl('line', { x1: CX, y1: CY, x2: CX, y2: CY - R + 6 }));
  needle.append(svgEl('circle', { cx: CX, cy: CY, r: 4 }));
  svg.append(needle);

  host.append(svg);
  return { svg, value, needle, target, tickGroup, labelGroup, ticks, scale: null };
}

/** Redraw ticks and their labels — only when the scale actually changed. */
function drawTicks(dial, scale) {
  if (dial.scale === scale) return;
  dial.scale = scale;
  dial.tickGroup.textContent = '';
  dial.labelGroup.textContent = '';
  for (let i = 0; i <= dial.ticks; i++) {
    const frac = i / dial.ticks;
    const deg = fracToDeg(frac);
    const [x0, y0] = polar(deg, R - 7);
    const [x1, y1] = polar(deg, R);
    dial.tickGroup.append(svgEl('line', { x1: x0, y1: y0, x2: x1, y2: y1 }));
    // Only the ends and the middle are labelled: six numbers around a 120px dial is noise.
    if (i === 0 || i === dial.ticks || i * 2 === dial.ticks) {
      const [lx, ly] = polar(deg, R - 16);
      const text = svgEl('text', { x: lx, y: ly + 3, 'text-anchor': 'middle' });
      text.textContent = formatInt(scale * frac);
      dial.labelGroup.append(text);
    }
  }
}

function updateNeedle(dial, frac) {
  const deg = fracToDeg(frac) + 90; // the needle path points "up" at 0°
  dial.needle.setAttribute('transform', `rotate(${deg - 90} ${CX} ${CY})`);
  dial.value.setAttribute('d', arcPath(0, frac, R));
}

function updateTarget(dial, frac) {
  if (frac == null) {
    dial.target.setAttribute('x2', dial.target.getAttribute('x1') ?? 0);
    return;
  }
  const deg = fracToDeg(frac);
  const [x0, y0] = polar(deg, R - 9);
  const [x1, y1] = polar(deg, R + 3);
  dial.target.setAttribute('x1', x0);
  dial.target.setAttribute('y1', y0);
  dial.target.setAttribute('x2', x1);
  dial.target.setAttribute('y2', y1);
}

// One dial per host element, built lazily and reused.
const dials = new WeakMap();
function dialFor(host, opts) {
  let d = dials.get(host);
  if (!d) {
    d = buildDial(host, opts);
    dials.set(host, d);
  }
  return d;
}

export function renderRowsGauge(els, rate) {
  const { valueEl, captionEl, dialEl } = els;
  const observed = rate.rowsPerSecIn;
  const requested = rate.rowsPerSecRequested;
  valueEl.textContent = formatInt(observed);
  captionEl.textContent = `requested ${formatInt(requested)}/s`;

  if (!dialEl) return;
  const dial = dialFor(dialEl, { ticks: 6 });
  const scale = niceScale(observed, requested);
  drawTicks(dial, scale);
  updateNeedle(dial, observed / scale);
  updateTarget(dial, requested > 0 ? requested / scale : null);
  dial.svg.setAttribute(
    'aria-label',
    `${formatInt(observed)} rows per second in, requested ${formatInt(requested)}, dial full scale ${formatInt(scale)}`,
  );
}

export function renderAlertsGauge(els, rate) {
  // Called with a bare element by older call sites; accept both.
  const { valueEl, dialEl } = els instanceof Element ? { valueEl: els, dialEl: null } : els;
  const observed = rate.alertsPerSecOut;
  valueEl.textContent = formatInt(observed);

  if (!dialEl) return;
  const dial = dialFor(dialEl, { ticks: 6 });
  const scale = niceScale(observed);
  drawTicks(dial, scale);
  updateNeedle(dial, observed / scale);
  dial.svg.setAttribute(
    'aria-label',
    `${formatInt(observed)} alerts per second out, dial full scale ${formatInt(scale)}`,
  );
}
