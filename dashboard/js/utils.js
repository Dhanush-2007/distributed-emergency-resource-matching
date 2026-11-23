// Haversine distance with exact radius 6371.0088 km
export function haversineDistanceKm(lat1, lon1, lat2, lon2) {
  const R = 6371.0088;
  const toRad = (deg) => (deg * Math.PI) / 180.0;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLon / 2) * Math.sin(dLon / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

// Relative time formatter with ISO title
export function formatRelativeTime(isoString) {
  if (!isoString) return { text: "unknown", iso: "" };
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return { text: isoString, iso: isoString };
  const now = new Date();
  const diffSec = Math.floor((now.getTime() - d.getTime()) / 1000);
  let text = "";
  if (diffSec < 5) text = "just now";
  else if (diffSec < 60) text = `${diffSec} s ago`;
  else if (diffSec < 3600) text = `${Math.floor(diffSec / 60)} m ago`;
  else if (diffSec < 86400) text = `${Math.floor(diffSec / 3600)} h ago`;
  else text = `${Math.floor(diffSec / 86400)} d ago`;
  return { text, iso: d.toISOString() };
}

// Generate random UUID v4 for correlation ID
export function generateUUID() {
  if (typeof crypto !== "undefined" && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// Safe DOM element creation helper (ensures zero raw HTML string injection)
export function el(tag, attrs = {}, children = []) {
  const element = document.createElement(tag);
  for (const [key, val] of Object.entries(attrs)) {
    if (key === "className") element.className = val;
    else if (key === "textContent") element.textContent = val;
    else if (key === "title") element.title = val;
    else if (key === "disabled") element.disabled = Boolean(val);
    else if (key.startsWith("on") && typeof val === "function") {
      element.addEventListener(key.substring(2).toLowerCase(), val);
    } else {
      element.setAttribute(key, val);
    }
  }
  if (!Array.isArray(children)) children = [children];
  for (const child of children) {
    if (typeof child === "string" || typeof child === "number") {
      element.appendChild(document.createTextNode(String(child)));
    } else if (child instanceof Node) {
      element.appendChild(child);
    }
  }
  return element;
}

// Copy to clipboard helper
export function copyToClipboard(text, targetEl) {
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text);
  }
  if (targetEl) {
    const oldText = targetEl.textContent;
    targetEl.textContent = "Copied!";
    setTimeout(() => {
      targetEl.textContent = oldText;
    }, 1200);
  }
}

// Create an ID cell with 8 chars and click-to-copy
export function renderIdCell(uuid) {
  if (!uuid) return el("span", { textContent: "-" });
  const shortId = uuid.substring(0, 8);
  const btn = el(
    "button",
    {
      className: "btn code-mono",
      style: "padding: 2px 6px; font-size: 11px; cursor: pointer;",
      title: `Click to copy full ID: ${uuid}`,
      onclick: (e) => {
        e.stopPropagation();
        copyToClipboard(uuid, btn);
      },
    },
    shortId
  );
  return btn;
}

// Create a state badge
export function renderBadge(status) {
  const s = String(status || "").toUpperCase();
  const cls = `badge badge-${s.toLowerCase()}`;
  return el("span", { className: cls, textContent: s || "UNKNOWN" });
}

// Format ISO timestamp into human-readable string
export function formatTimestamp(isoString) {
  if (!isoString) return "-";
  const d = new Date(isoString);
  if (isNaN(d.getTime())) return String(isoString);
  return d.toLocaleString();
}
