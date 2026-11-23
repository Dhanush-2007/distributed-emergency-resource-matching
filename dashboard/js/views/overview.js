import { api } from "../api.js";
import { el, renderIdCell, renderBadge, formatRelativeTime } from "../utils.js";

let currentOverview = null;

export async function renderOverviewView(container) {
  currentOverview = renderOverview(container);
  return currentOverview;
}

export async function refreshOverview() {
  if (currentOverview && currentOverview.update) {
    await currentOverview.update();
  }
}

export function renderOverview(container) {
  while (container.firstChild) container.removeChild(container.firstChild);

  // Four plain figures
  const totalVal = el("div", { className: "figure-val", textContent: "-" });
  const matchedVal = el("div", { className: "figure-val", textContent: "-" });
  const pendingVal = el("div", { className: "figure-val", textContent: "-" });
  const availVal = el("div", { className: "figure-val", textContent: "-" });

  const figures = el("div", { className: "figures-grid" }, [
    el("div", { className: "figure-card" }, [el("div", { className: "label-caps", textContent: "Requests Total" }), totalVal]),
    el("div", { className: "figure-card" }, [el("div", { className: "label-caps", textContent: "Matched Requests" }), matchedVal]),
    el("div", { className: "figure-card" }, [el("div", { className: "label-caps", textContent: "Pending or No Match" }), pendingVal]),
    el("div", { className: "figure-card" }, [el("div", { className: "label-caps", textContent: "Resources Available" }), availVal]),
  ]);
  container.appendChild(figures);

  // Compact Shard Table
  const shardTbody = el("tbody");
  const shardSection = el("div", { style: "margin-bottom: 24px;" }, [
    el("h2", { className: "label-caps", style: "font-size: 12px; margin-bottom: 8px;", textContent: "Storage Shards" }),
    el("div", { className: "table-wrap" }, [
      el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { textContent: "Shard" }),
            el("th", { textContent: "State" }),
            el("th", { textContent: "Primary" }),
            el("th", { className: "num-cell", textContent: "Epoch" }),
            el("th", { className: "num-cell", textContent: "Records" }),
          ]),
        ]),
        shardTbody,
      ]),
    ]),
  ]);
  container.appendChild(shardSection);

  // 10 Newest Requests
  const reqTbody = el("tbody");
  const reqSection = el("div", {}, [
    el("h2", { className: "label-caps", style: "font-size: 12px; margin-bottom: 8px;", textContent: "Recent Requests" }),
    el("div", { className: "table-wrap" }, [
      el("table", {}, [
        el("thead", {}, [
          el("tr", {}, [
            el("th", { textContent: "Request ID" }),
            el("th", { textContent: "Type" }),
            el("th", { textContent: "Location" }),
            el("th", { textContent: "Created" }),
            el("th", { textContent: "Status" }),
          ]),
        ]),
        reqTbody,
      ]),
    ]),
  ]);
  container.appendChild(reqSection);

  async function update() {
    try {
      const [shardsRes, requests, resources] = await Promise.all([
        api.getShards().catch(() => ({ shards: [] })),
        api.getRequests().catch(() => []),
        api.getResources().catch(() => []),
      ]);

      const shards = Array.isArray(shardsRes) ? shardsRes : (shardsRes && shardsRes.shards ? shardsRes.shards : []);

      // Calculate figures
      const totalReq = requests.length;
      const matchedReq = requests.filter((r) => r.status === "MATCHED" || r.status === "COMPLETED" || r.status === "IN_PROGRESS").length;
      const pendingReq = requests.filter((r) => r.status === "PENDING" || r.status === "NO_MATCH").length;
      const availRes = resources.filter((r) => r.availability === true || r.status === "AVAILABLE").length;

      totalVal.textContent = String(totalReq);
      matchedVal.textContent = String(matchedReq);
      pendingVal.textContent = String(pendingReq);
      availVal.textContent = String(availRes);

      // Render Shards
      while (shardTbody.firstChild) shardTbody.removeChild(shardTbody.firstChild);
      if (shards.length === 0) {
        shardTbody.appendChild(el("tr", {}, el("td", { colSpan: 5, style: "text-align: center; color: var(--ink-muted);", textContent: "No shard diagnostics available." })));
      } else {
        shards.forEach((s) => {
          const sId = s.shardId !== undefined ? s.shardId : s.id;
          const recCount = s.records !== undefined ? s.records : (s.recordCount ? Object.values(s.recordCount).reduce((a, b) => a + b, 0) : 0);
          const tr = el("tr", {}, [
            el("td", { className: "code-mono", textContent: `shard-${sId}` }),
            el("td", {}, renderBadge(s.state || s.status)),
            el("td", { className: "code-mono", textContent: s.primary || "-" }),
            el("td", { className: "num-cell", textContent: String(s.epoch || 1) }),
            el("td", { className: "num-cell", textContent: String(recCount) }),
          ]);
          shardTbody.appendChild(tr);
        });
      }

      // Render 10 newest requests
      while (reqTbody.firstChild) reqTbody.removeChild(reqTbody.firstChild);
      const sorted = [...requests].sort((a, b) => new Date(b.createdAt || 0) - new Date(a.createdAt || 0)).slice(0, 10);
      if (sorted.length === 0) {
        reqTbody.appendChild(el("tr", {}, el("td", { colSpan: 5, style: "text-align: center; color: var(--ink-muted);", textContent: "No requests recorded yet." })));
      } else {
        sorted.forEach((r) => {
          const rel = formatRelativeTime(r.createdAt);
          const locStr = r.location ? `${Number(r.location.lat).toFixed(4)}, ${Number(r.location.lon).toFixed(4)}` : "-";
          const tr = el("tr", {}, [
            el("td", {}, renderIdCell(r.requestId)),
            el("td", { textContent: r.type || "-" }),
            el("td", { className: "code-mono", textContent: locStr }),
            el("td", { title: rel.iso, textContent: rel.text }),
            el("td", {}, renderBadge(r.status)),
          ]);
          reqTbody.appendChild(tr);
        });
      }
    } catch {
      // Handled by api layer
    }
  }

  update();
  return { update };
}
