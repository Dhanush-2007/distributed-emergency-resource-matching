import { generateUUID } from "./utils.js";

let onServiceStatusChange = null;

export function setServiceStatusHandler(handler) {
  onServiceStatusChange = handler;
}

// Low-level fetch wrapper with correlation ID and envelope parsing
async function apiFetch(endpoint, options = {}) {
  const correlationId = generateUUID();
  const headers = {
    "X-Correlation-Id": correlationId,
    ...(options.headers || {}),
  };
  if (options.body && typeof options.body === "object" && !(options.body instanceof FormData)) {
    headers["Content-Type"] = "application/json";
    options.body = JSON.stringify(options.body);
  }

  try {
    const res = await fetch(endpoint, { ...options, headers });
    const retryAfter = res.headers.get("Retry-After");
    const resCorrelationId = res.headers.get("X-Correlation-Id") || correlationId;

    let data = null;
    const contentType = res.headers.get("Content-Type") || "";
    if (contentType.includes("application/json")) {
      try {
        data = await res.json();
      } catch {
        data = null;
      }
    } else {
      const text = await res.text();
      data = { text };
    }

    if (!res.ok) {
      const errMsg = (data && data.error && data.error.message) || `HTTP ${res.status}`;
      if (onServiceStatusChange) onServiceStatusChange("down", { status: res.status, message: errMsg });
      const err = new Error(errMsg);
      err.status = res.status;
      err.data = data;
      err.correlationId = resCorrelationId;
      err.retryAfter = retryAfter;
      err.errorEnvelope = (data && data.error) || {
        code: `HTTP_${res.status}`,
        message: err.message,
        details: [],
      };
      throw err;
    }

    if (onServiceStatusChange) onServiceStatusChange("ok");
    return { data, status: res.status, correlationId: resCorrelationId };
  } catch (err) {
    if (err.status === undefined) {
      if (onServiceStatusChange) onServiceStatusChange("down", { status: 0, message: err.message || "Failed to reach service" });
      err.status = 0;
      err.correlationId = correlationId;
      err.errorEnvelope = {
        code: "NETWORK_ERROR",
        message: err.message || "Failed to reach service",
        details: [],
      };
    }
    throw err;
  }
}

function normalizeItem(item) {
  if (!item || typeof item !== "object") return item;
  if (item.location && typeof item.location === "object") {
    if (item.lat === undefined) item.lat = item.location.lat;
    if (item.lon === undefined) item.lon = item.location.lon;
  }
  if (item.type && typeof item.type === "string") {
    item.type = item.type.toUpperCase();
  }
  return item;
}

// API methods
export const api = {
  onStatusChange: setServiceStatusHandler,

  // Coordinator
  getShards: async () => {
    const res = await apiFetch("/api/coord/diagnostics/shards");
    const arr = Array.isArray(res.data) ? res.data : ((res.data && res.data.shards) || (res.data && res.data.items) || []);
    return { shards: arr };
  },
  getShardsDiagnostics: async () => {
    return api.getShards();
  },
  getNodes: async () => {
    const res = await apiFetch("/api/coord/diagnostics/nodes");
    const arr = Array.isArray(res.data) ? res.data : ((res.data && res.data.nodes) || (res.data && res.data.items) || []);
    return { nodes: arr };
  },
  getNodesDiagnostics: async () => {
    return api.getNodes();
  },

  // Requests
  getRequests: async (params = {}) => {
    const query = new URLSearchParams();
    if (params.type) query.set("type", params.type.toLowerCase());
    if (params.status) query.set("status", params.status);
    const qs = query.toString() ? `?${query.toString()}` : "";
    const res = await apiFetch(`/api/req/requests${qs}`);
    const items = Array.isArray(res.data) ? res.data : ((res.data && res.data.items) || []);
    return items.map(normalizeItem);
  },
  listRequests: async (params) => {
    return api.getRequests(params);
  },
  getRequestById: async (id) => {
    const res = await apiFetch(`/api/req/requests/${id}`);
    return normalizeItem(res.data);
  },
  getRequest: async (id) => {
    return api.getRequestById(id);
  },
  createRequest: async (payload) => {
    const body = {
      type: (payload.type || "").toLowerCase(),
      location: payload.location || { lat: Number(payload.lat), lon: Number(payload.lon) },
      details: payload.details || `Emergency dispatch for ${(payload.type || "incident").toLowerCase()}`
    };
    const res = await apiFetch("/api/req/requests", {
      method: "POST",
      body,
    });
    return normalizeItem(res.data);
  },
  updateRequestStatus: async (id, status) => {
    const res = await apiFetch(`/api/req/requests/${id}/status`, {
      method: "PUT",
      body: { status },
    });
    return normalizeItem(res.data);
  },
  requeueMatch: async (id) => {
    const res = await apiFetch(`/api/req/match/${id}`, {
      method: "POST",
    });
    return res.data;
  },
  triggerMatch: async (id) => {
    return api.requeueMatch(id);
  },

  // Resources
  getResources: async (params = {}) => {
    const query = new URLSearchParams();
    if (params.type) query.set("type", params.type.toLowerCase());
    if (params.status) query.set("status", params.status);
    if (params.availability !== undefined) query.set("availability", String(params.availability));
    const qs = query.toString() ? `?${query.toString()}` : "";
    const res = await apiFetch(`/api/res/resources${qs}`);
    const items = Array.isArray(res.data) ? res.data : ((res.data && res.data.items) || []);
    return items.map(normalizeItem);
  },
  listResources: async (params) => {
    return api.getResources(params);
  },
  getResourceById: async (id) => {
    const res = await apiFetch(`/api/res/resources/${id}`);
    return normalizeItem(res.data);
  },
  getResource: async (id) => {
    return api.getResourceById(id);
  },
  createResource: async (payload) => {
    const body = {
      type: (payload.type || "").toLowerCase(),
      location: payload.location || { lat: Number(payload.lat), lon: Number(payload.lon) },
      availability: Boolean(payload.availability)
    };
    if (payload.name && payload.name.trim()) {
      body.name = payload.name.trim();
    }
    const res = await apiFetch("/api/res/resources", {
      method: "POST",
      body,
    });
    return normalizeItem(res.data);
  },
  updateResourceStatus: async (id, status) => {
    const res = await apiFetch(`/api/res/resources/${id}/status`, {
      method: "PUT",
      body: { status },
    });
    return normalizeItem(res.data);
  },
};
