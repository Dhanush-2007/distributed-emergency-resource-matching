import { api } from "./api.js";
import { el } from "./utils.js";
import { renderOverviewView, refreshOverview } from "./views/overview.js";
import { renderRequestsView, refreshRequestsTable } from "./views/requests.js";
import { renderResourcesView, refreshResourcesTable } from "./views/resources.js";
import { renderClusterView, refreshClusterView } from "./views/cluster.js";

// Global uncaught error handler to prevent silent failures
window.addEventListener("error", (event) => {
  const globalAlert = document.getElementById("global-alert");
  if (globalAlert) {
    const msg = (event && (event.message || (event.error && event.error.message))) || "Script error";
    globalAlert.textContent = `Client Error: ${msg}`;
    globalAlert.style.display = "block";
  }
});

window.addEventListener("unhandledrejection", (event) => {
  const globalAlert = document.getElementById("global-alert");
  if (globalAlert) {
    const reason = event.reason;
    const msg = (reason && (reason.message || (reason.errorEnvelope && reason.errorEnvelope.message))) || String(reason || "Unhandled Promise Rejection");
    globalAlert.textContent = `Client Rejection: ${msg}`;
    globalAlert.style.display = "block";
  }
});

const routes = {
  "#overview": { render: renderOverviewView, refresh: refreshOverview, navId: "nav-overview" },
  "#requests": { render: renderRequestsView, refresh: refreshRequestsTable, navId: "nav-requests" },
  "#resources": { render: renderResourcesView, refresh: refreshResourcesTable, navId: "nav-resources" },
  "#cluster": { render: renderClusterView, refresh: refreshClusterView, navId: "nav-cluster" }
};

let currentRoute = "#overview";
let pollInterval = null;
let gKeyPressed = false;
let gKeyTimer = null;

export function initApp() {
  window.addEventListener("hashchange", handleRouting);
  setupKeyboardShortcuts();
  setupApiListener();

  handleRouting();
  startPolling();
}

function handleRouting() {
  const hash = window.location.hash || "#overview";
  const route = routes[hash] || routes["#overview"];
  currentRoute = hash in routes ? hash : "#overview";

  document.querySelectorAll(".nav-item").forEach((el) => el.classList.remove("active"));
  const navEl = document.getElementById(route.navId);
  if (navEl) navEl.classList.add("active");

  const main = document.getElementById("main-content");
  if (main) {
    route.render(main);
  }
}

function startPolling() {
  if (pollInterval) clearInterval(pollInterval);
  pollInterval = setInterval(async () => {
    updateStatusStrip();
    const route = routes[currentRoute];
    if (route && route.refresh) {
      await route.refresh();
    }
  }, 3000);
  updateStatusStrip();
}

async function updateStatusStrip() {
  const coordStatusEl = document.getElementById("strip-coord");
  const shardsStatusEl = document.getElementById("strip-shards");
  const timeEl = document.getElementById("strip-time");

  try {
    const shardsData = await api.getShardsDiagnostics();
    const shards = (shardsData && shardsData.shards) || [];
    const degraded = shards.filter((s) => s.degraded).length;

    if (coordStatusEl) {
      coordStatusEl.textContent = "COORDINATOR: OK";
      coordStatusEl.className = "status-badge status-healthy";
    }
    if (shardsStatusEl) {
      shardsStatusEl.textContent = degraded > 0 ? `SHARDS: ${degraded} DEGRADED` : "SHARDS: 3/3 HEALTHY";
      shardsStatusEl.className = degraded > 0 ? "status-badge status-degraded" : "status-badge status-healthy";
    }
  } catch {
    if (coordStatusEl) {
      coordStatusEl.textContent = "COORDINATOR: DOWN";
      coordStatusEl.className = "status-badge status-down";
    }
    if (shardsStatusEl) {
      shardsStatusEl.textContent = "SHARDS: UNKNOWN";
      shardsStatusEl.className = "status-badge status-down";
    }
  }

  if (timeEl) {
    timeEl.textContent = new Date().toLocaleTimeString();
  }
}

function setupApiListener() {
  const globalAlert = document.getElementById("global-alert");
  api.onStatusChange((status, err) => {
    if (!globalAlert) return;
    if (status === "down" || status === "error") {
      const errStatus = (err && err.status) || 503;
      const errMsg = (err && err.message) || "Service unavailable";
      globalAlert.textContent = `Backend connection failure (${errStatus}): ${errMsg}. Retrying...`;
      globalAlert.style.display = "block";
    } else {
      globalAlert.style.display = "none";
    }
  });
}

function setupKeyboardShortcuts() {
  window.addEventListener("keydown", (e) => {
    const targetTag = e.target.tagName;
    if (targetTag === "INPUT" || targetTag === "TEXTAREA" || targetTag === "SELECT") {
      if (e.key === "Escape") e.target.blur();
      return;
    }

    if (e.key === "/") {
      const search = document.getElementById("lookup-input");
      if (search) {
        e.preventDefault();
        search.focus();
        search.select();
      }
      return;
    }

    if (e.key === "g" || e.key === "G") {
      gKeyPressed = true;
      clearTimeout(gKeyTimer);
      gKeyTimer = setTimeout(() => { gKeyPressed = false; }, 1000);
      return;
    }

    if (gKeyPressed) {
      gKeyPressed = false;
      clearTimeout(gKeyTimer);
      if (e.key === "o") window.location.hash = "#overview";
      if (e.key === "r") window.location.hash = "#requests";
      if (e.key === "s") window.location.hash = "#resources";
      if (e.key === "c") window.location.hash = "#cluster";
    }
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initApp);
} else {
  initApp();
}
