import { headLag } from "./store.js";
import { int } from "./util.js";

const TIP_WINDOW = 60;

function geometry(width, axis) {
  const gap = 18;
  const leftW = Math.max(40, Math.round(width * 0.64));
  const rightX0 = leftW + gap;
  const rightW = Math.max(20, width - rightX0 - 2);
  const span = Math.max(1, axis.best + 1 - axis.min);
  return {
    leftW,
    rightX0,
    rightW,
    gap,
    xL: (b) => clamp(((b - axis.min) / span) * leftW, 0, leftW),
    xLag: (lag) => clamp(rightX0 + (1 - lag / TIP_WINDOW) * rightW, rightX0, rightX0 + rightW),
  };
}

function clamp(v, lo, hi) {
  return Math.min(hi, Math.max(lo, v));
}

function f(n) {
  return Number.isFinite(n) ? n.toFixed(1) : "0";
}

function breakMark(g, y) {
  const x = g.leftW + g.gap / 2;
  return `<path class="brk" d="M${f(x - 5)} ${y + 5}L${f(x - 1)} ${y - 5}M${f(x + 1)} ${y + 5}L${f(x + 5)} ${y - 5}"/>`;
}

export function axisMarkup(width, axis) {
  const g = geometry(width, axis);
  const y = 20;
  const parts = [
    '<title>Last reported history range on the left; measured chain-head lag on the right</title>',
    `<line class="tick" x1="0" x2="${f(g.leftW)}" y1="${y}" y2="${y}"/>`,
    `<line class="tick" x1="${f(g.rightX0)}" x2="${f(g.rightX0 + g.rightW)}" y1="${y}" y2="${y}"/>`,
    `<line class="tick" x1="0.5" x2="0.5" y1="${y - 4}" y2="${y}"/>`,
    `<line class="tick" x1="${f(g.leftW - 0.5)}" x2="${f(g.leftW - 0.5)}" y1="${y - 4}" y2="${y}"/>`,
    `<line class="tick" x1="${f(g.rightX0 + 0.5)}" x2="${f(g.rightX0 + 0.5)}" y1="${y - 4}" y2="${y}"/>`,
    `<line class="tick" x1="${f(g.rightX0 + g.rightW - 0.5)}" x2="${f(g.rightX0 + g.rightW - 0.5)}" y1="${y - 4}" y2="${y}"/>`,
    breakMark(g, y - 4),
    `<text x="0" y="${y - 6}">#${int(axis.min)}</text>`,
  ];
  if (g.leftW > 150) parts.push(`<text x="${f(g.leftW)}" y="${y - 6}" text-anchor="end">#${int(axis.best)}</text>`);
  parts.push(`<text x="${f(g.rightX0)}" y="${y - 6}">−${TIP_WINDOW}</text>`);
  parts.push(`<text x="${f(g.rightX0 + g.rightW)}" y="${y - 6}" text-anchor="end">at head</text>`);
  return parts.join("");
}

const DRAW_STATES = new Set(["up", "stale", "quarantined", "down", "disabled", "failover"]);

export function trackMarkup(width, axis, u, rawStatus) {
  // Only known words reach the markup; the server's string is never inserted as-is.
  const status = DRAW_STATES.has(rawStatus) ? rawStatus : "down";
  const g = geometry(width, axis);
  const y = 12;
  const yf = 21;
  const parts = [
    `<title>${trackLabel(u, axis)}</title>`,
    `<line class="rail" x1="0" x2="${f(g.leftW)}" y1="${y}" y2="${y}"/>`,
    `<line class="rail" x1="${f(g.rightX0)}" x2="${f(g.rightX0 + g.rightW)}" y1="${y}" y2="${y}"/>`,
    breakMark(g, y),
    `<line class="headline" x1="${f(g.rightX0 + g.rightW)}" x2="${f(g.rightX0 + g.rightW)}" y1="2" y2="27"/>`,
  ];
  const t = u.trace;
  if (t && t.end > 0) {
    const last = t.end - 1;
    const x1 = g.xL(t.begin);
    const x2 = Math.max(x1 + 2, g.xL(last));
    parts.push(`<line class="range" data-status="${status}" x1="${f(x1)}" x2="${f(x2)}" y1="${y}" y2="${y}"/>`);
  }
  // The right-hand scale is relative lag, not another view of sampled range
  // ends. Using trace.end here can contradict the adjacent "at head" label
  // whenever the two monitors' status polls are out of phase.
  const lag = headLag(u);
  if (lag !== null && lag <= TIP_WINDOW) {
    const xr = g.xLag(lag);
    parts.push(`<line class="range" data-status="${status}" x1="${f(g.rightX0)}" x2="${f(xr)}" y1="${y}" y2="${y}"/>`);
    parts.push(`<line class="cap" data-status="${status}" x1="${f(xr)}" x2="${f(xr)}" y1="${y - 6}" y2="${y + 6}"/>`);
  } else if (lag !== null) {
    const x = g.rightX0;
    parts.push(`<path class="behind" d="M${f(x + 7)} ${y - 4}L${f(x + 1)} ${y}L${f(x + 7)} ${y + 4}Z"/>`);
  }
  const fin = u.finality;
  if (fin && fin.end > 0) {
    const last = fin.end - 1;
    const x1 = g.xL(fin.begin);
    parts.push(`<line class="fin" data-status="${status}" x1="${f(x1)}" x2="${f(Math.max(x1 + 2, g.xL(last)))}" y1="${yf}" y2="${yf}"/>`);
  }
  for (const q of u.quarantines || []) {
    const x = g.xL(q.block);
    parts.push(`<rect class="q" x="${f(x - 3)}" y="${y - 6}" width="6" height="12"/>`);
  }
  return parts.join("");
}

export function trackLabel(u, axis) {
  const bits = [];
  if (u.trace && u.trace.end > 0) bits.push(`last reported trace blocks ${int(u.trace.begin)} to ${int(u.trace.end - 1)}`);
  else bits.push("no trace range reported yet");
  if (u.finality && u.finality.end > 0) bits.push(`finality blocks ${int(u.finality.begin)} to ${int(u.finality.end - 1)}`);
  if (axis) {
    const lag = headLag(u);
    bits.push(lag === null ? "head lag unavailable" : lag === 0 ? "no lag observed at the last status check" : `at least ${int(lag)} blocks behind at the last status check`);
  }
  if (u.quarantines?.length) bits.push(`${u.quarantines.length} quarantined block range${u.quarantines.length === 1 ? "" : "s"}`);
  return bits.join(", ");
}
