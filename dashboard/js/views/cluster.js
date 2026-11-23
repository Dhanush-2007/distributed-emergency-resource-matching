import { api } from "../api.js";
import { el, formatTimestamp, renderBadge } from "../utils.js";

const MAX_EVENTS = 20;
const clusterEvents = [];
let prevClusterState = null;

export async function renderClusterView(container) {
  container.replaceChildren();

  const titleRow = el("div", { class: "view-header" }, [
    el("h2", { class: "view-title" }, "Cluster Diagnostics"),
    el("div", { class: "text-muted text-xs" }, "Storage shards, replication status and failover operations")
  ]);

  const shardsSection = el("div", { id: "cluster-shards-container", class: "card mb-3" });
  const eventLogSection = renderEventLogSection();
  const demoSection = renderFailoverDemoSection();

  container.append(titleRow, shardsSection, eventLogSection, demoSection);
  await refreshClusterView();
}

function renderFailoverDemoSection() {
  const card = el("div", { class: "card mt-3" });
  const head = el("div", { class: "card-header flex justify-between align-center" }, [
    el("div", { class: "card-title" }, "Failover Testing & Queue Management"),
    el("a", { href: "http://localhost:15672", target: "_blank", class: "text-xs link-accent" }, "Open RabbitMQ UI (guest/guest) ↗")
  ]);

  const desc = el("p", { class: "text-xs text-muted mb-2" },
    "Test high-availability failover by pausing or killing a primary node. Watch replica election and epoch bump in real time."
  );

  const pre = el("pre", { class: "code-block mono text-xs p-3" },
    `# 1. Stop primary node for shard-0:\n` +
    `docker compose stop storage-node-0a\n\n` +
    `# 2. Watch coordinator promote storage-node-0b:\n` +
    `docker compose logs -f coordinator\n\n` +
    `# 3. Restore storage-node-0a as replica:\n` +
    `docker compose start storage-node-0a`
  );

  card.append(head, desc, pre);
  return card;
}

function renderEventLogSection() {
  const card = el("div", { class: "card mb-3" });
  const head = el("div", { class: "card-header flex justify-between align-center" }, [
    el("div", { class: "card-title" }, "Detected Cluster Events"),
    el("span", { class: "text-muted text-xs" }, "Last 20 state transitions")
  ]);

  const list = el("div", { id: "cluster-event-list", class: "event-list" });
  card.append(head, list);
  return card;
}

export async function refreshClusterView() {
  const container = document.getElementById("cluster-shards-container");
  const eventList = document.getElementById("cluster-event-list");
  if (!container) return;

  try {
    const [shardsData, nodesData] = await Promise.all([
      api.getShardsDiagnostics(),
      api.getNodesDiagnostics()
    ]);

    const shards = (shardsData && shardsData.shards) || [];
    const nodes = (nodesData && nodesData.nodes) || [];

    detectClusterEvents(shards);
    updateEventListUI(eventList);

    const table = el("table", { class: "table" });
    const thead = el("thead", {}, [
      el("tr", {}, [
        el("th", {}, "Shard ID"),
        el("th", {}, "Primary Node"),
        el("th", {}, "Epoch"),
        el("th", {}, "Status"),
        el("th", {}, "Replicas & Sequences"),
        el("th", {}, "Keys / Records")
      ])
    ]);

    const tbody = el("tbody");
    shards.forEach((sh) => {
      const isDegraded = sh.degraded;
      const shardNodes = nodes.filter((n) => n.shardId === sh.shardId);

      const seqs = [];
      let seqMismatch = false;
      let primarySeq = null;

      shardNodes.forEach((n) => {
        const isPrimary = n.role === "PRIMARY";
        if (isPrimary) primarySeq = n.lastAppliedSeq !== undefined ? n.lastAppliedSeq : n.lastSeq;
      });

      shardNodes.forEach((n) => {
        const seq = n.lastAppliedSeq !== undefined ? n.lastAppliedSeq : (n.lastSeq !== undefined ? n.lastSeq : 0);
        const mismatch = primarySeq !== null && seq !== primarySeq;
        if (mismatch) seqMismatch = true;
        seqs.push(
          el("div", { class: `text-xs mono ${mismatch ? "text-danger font-bold" : ""}` }, [
            el("span", { class: "text-muted" }, `${n.nodeId} (${n.role}): `),
            el("span", {}, `seq=${seq} [${n.status}]`)
          ])
        );
      });

      const tr = el("tr", { class: isDegraded ? "bg-warning-light" : "" }, [
        el("td", { class: "mono font-bold" }, sh.shardId),
        el("td", { class: "mono" }, sh.primaryNodeId || "NONE"),
        el("td", { class: "mono" }, String(sh.epoch !== undefined ? sh.epoch : "-")),
        el("td", {}, [
          renderBadge(isDegraded ? "DEGRADED" : "HEALTHY"),
          seqMismatch ? el("span", { class: "badge badge-fail ml-1" }, "SYNC LAG") : ""
        ]),
        el("td", {}, seqs.length ? seqs : el("span", { class: "text-muted text-xs" }, "No node metrics")),
        el("td", { class: "mono text-xs" }, String(sh.recordCount !== undefined ? sh.recordCount : (sh.totalKeys !== undefined ? sh.totalKeys : "-")))
      ]);
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    container.replaceChildren(table);
  } catch (err) {
    container.replaceChildren(el("div", { class: "error-banner" }, `Diagnostics error: ${err.message}`));
  }
}

function detectClusterEvents(currentShards) {
  if (!prevClusterState) {
    prevClusterState = currentShards;
    recordEvent("INIT", "Initial cluster baseline captured");
    return;
  }

  currentShards.forEach((cur) => {
    const prev = prevClusterState.find((s) => s.shardId === cur.shardId);
    if (!prev) return;

    if (prev.primaryNodeId !== cur.primaryNodeId) {
      recordEvent("FAILOVER", `Shard ${cur.shardId} primary switched: ${prev.primaryNodeId} -> ${cur.primaryNodeId}`);
    }
    if (prev.epoch !== cur.epoch) {
      recordEvent("EPOCH", `Shard ${cur.shardId} epoch bumped: ${prev.epoch} -> ${cur.epoch}`);
    }
    if (prev.degraded !== cur.degraded) {
      recordEvent("HEALTH", `Shard ${cur.shardId} degraded state: ${prev.degraded} -> ${cur.degraded}`);
    }
  });

  prevClusterState = currentShards;
}

function recordEvent(type, text) {
  clusterEvents.unshift({ type, text, time: new Date().toISOString() });
  if (clusterEvents.length > MAX_EVENTS) clusterEvents.pop();
}

function updateEventListUI(container) {
  if (!container) return;
  if (clusterEvents.length === 0) {
    container.replaceChildren(el("div", { class: "p-3 text-muted text-xs" }, "No cluster transitions recorded yet."));
    return;
  }

  const items = clusterEvents.map((ev) =>
    el("div", { class: "event-item flex justify-between align-center p-2 border-b text-xs" }, [
      el("div", { class: "flex gap-2 align-center" }, [
        el("span", { class: `badge badge-${ev.type === "FAILOVER" ? "fail" : "neutral"} mono` }, ev.type),
        el("span", {}, ev.text)
      ]),
      el("span", { class: "mono text-muted text-xs" }, formatTimestamp(ev.time))
    ])
  );
  container.replaceChildren(...items);
}
