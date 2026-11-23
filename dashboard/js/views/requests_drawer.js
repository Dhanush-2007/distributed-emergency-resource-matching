import { api } from "../api.js";
import { el, renderBadge, haversineDistanceKm, copyToClipboard } from "../utils.js";

export function createRequestsDrawer(onUpdateSuccess) {
  const drawer = document.getElementById("side-drawer");
  let currentRequest = null;

  function close() {
    drawer.classList.remove("open");
    const backdrop = document.getElementById("drawer-backdrop");
    if (backdrop) {
      backdrop.style.display = "none";
      backdrop.onclick = null;
    }
  }

  async function open(request) {
    currentRequest = request;
    const backdrop = document.getElementById("drawer-backdrop");
    if (backdrop) {
      backdrop.style.display = "block";
      backdrop.onclick = close;
    }
    while (drawer.firstChild) drawer.removeChild(drawer.firstChild);

    // Header
    const closeBtn = el("button", { className: "btn", textContent: "Close", onclick: close });
    const header = el("div", { className: "drawer-header" }, [
      el("div", {}, [
        el("div", { className: "label-caps", textContent: "Request Details" }),
        el("div", { className: "code-mono", style: "font-size: 15px; font-weight: 600;", textContent: request.requestId.substring(0, 8) }),
      ]),
      closeBtn,
    ]);
    drawer.appendChild(header);

    // Metadata
    const copyIdBtn = el("button", {
      className: "btn code-mono",
      style: "font-size: 11px; margin-left: 6px;",
      textContent: "Copy Full UUID",
      onclick: () => copyToClipboard(request.requestId, copyIdBtn),
    });
    const meta = el("div", { style: "display: flex; flex-direction: column; gap: 8px; margin-bottom: 16px;" }, [
      el("div", {}, [el("span", { className: "label-caps", textContent: "UUID: " }), el("span", { className: "code-mono", textContent: request.requestId }), copyIdBtn]),
      el("div", {}, [el("span", { className: "label-caps", textContent: "Status: " }), renderBadge(request.status)]),
      el("div", {}, [el("span", { className: "label-caps", textContent: "Type: " }), el("span", { textContent: request.type || "-" })]),
      el("div", {}, [el("span", { className: "label-caps", textContent: "Details: " }), el("span", { textContent: request.details || "-" })]),
      el("div", {}, [el("span", { className: "label-caps", textContent: "Version: " }), el("span", { className: "code-mono", textContent: String(request.version || 1) })]),
    ]);
    drawer.appendChild(meta);

    // Matched Resource Box
    const resBox = el("div", { style: "background: var(--bg-subtle); border: 1px solid var(--border); padding: 12px; border-radius: 4px; margin-bottom: 16px;" }, [
      el("div", { className: "label-caps", textContent: "Matched Resource" }),
    ]);
    if (request.matchedResourceId) {
      resBox.appendChild(el("div", { className: "code-mono", style: "margin-top: 4px;", textContent: `Loading resource ${request.matchedResourceId}...` }));
      api.getResourceById(request.matchedResourceId).then((res) => {
        while (resBox.children.length > 1) resBox.removeChild(resBox.lastChild);
        if (res) {
          let distStr = "";
          if (request.location && res.location) {
            const d = haversineDistanceKm(request.location.lat, request.location.lon, res.location.lat, res.location.lon);
            distStr = ` (Distance: ${d.toFixed(3)} km)`;
          }
          resBox.appendChild(el("div", { style: "margin-top: 6px;" }, [
            el("div", { style: "font-weight: 600;", textContent: `${res.name || "Unnamed Resource"}${distStr}` }),
            el("div", { className: "code-mono", style: "font-size: 11px;", textContent: `ID: ${res.resourceId} | Status: ${res.status}` }),
          ]));
        }
      }).catch(() => {
        while (resBox.children.length > 1) resBox.removeChild(resBox.lastChild);
        resBox.appendChild(el("div", { style: "color: var(--status-down); font-size: 11px; margin-top: 4px;", textContent: "Failed to load resource details." }));
      });
    } else {
      resBox.appendChild(el("div", { style: "font-size: 12px; color: var(--ink-muted); margin-top: 4px;", textContent: "No resource matched." }));
    }
    drawer.appendChild(resBox);

    // Actions under SPEC 5.4
    const st = request.status;
    const canInProgress = st === "MATCHED";
    const canCompleted = st === "MATCHED" || st === "IN_PROGRESS";
    const canCancelled = st === "PENDING" || st === "MATCHED" || st === "IN_PROGRESS" || st === "NO_MATCH";
    const canRequeue = st === "NO_MATCH" || st === "PENDING";

    const errBanner = el("div", { className: "error-banner", style: "margin-bottom: 12px;" });

    async function handleAction(fn) {
      errBanner.style.display = "none";
      try {
        const updated = await fn();
        if (onUpdateSuccess) onUpdateSuccess(updated);
        open(updated);
      } catch (err) {
        errBanner.style.display = "block";
        while (errBanner.firstChild) errBanner.removeChild(errBanner.firstChild);
        const env = err.errorEnvelope || {};
        errBanner.appendChild(el("div", { className: "err-code", textContent: env.code || "ERROR" }));
        errBanner.appendChild(el("div", { textContent: env.message || err.message }));
      }
    }

    const inProgressBtn = el("button", {
      className: "btn",
      disabled: !canInProgress,
      title: canInProgress ? "Mark request IN_PROGRESS" : "Only MATCHED requests can move to IN_PROGRESS",
      textContent: "In progress",
      onclick: () => handleAction(() => api.updateRequestStatus(request.requestId, "IN_PROGRESS")),
    });
    const completedBtn = el("button", {
      className: "btn",
      disabled: !canCompleted,
      title: canCompleted ? "Mark request COMPLETED" : "Only MATCHED or IN_PROGRESS requests can be COMPLETED",
      textContent: "Completed",
      onclick: () => handleAction(() => api.updateRequestStatus(request.requestId, "COMPLETED")),
    });
    const cancelledBtn = el("button", {
      className: "btn",
      disabled: !canCancelled,
      title: canCancelled ? "Cancel this request" : "Completed or already cancelled requests cannot be cancelled",
      textContent: "Cancelled",
      onclick: () => handleAction(() => api.updateRequestStatus(request.requestId, "CANCELLED")),
    });
    const requeueBtn = el("button", {
      className: "btn btn-primary",
      disabled: !canRequeue,
      title: canRequeue ? "Re-trigger matching event" : "Re-queue match is only allowed for PENDING or NO_MATCH requests",
      textContent: "Re-queue match",
      onclick: () => handleAction(() => api.requeueMatch(request.requestId)),
    });

    const actions = el("div", { className: "drawer-actions" }, [inProgressBtn, completedBtn, cancelledBtn, requeueBtn]);
    drawer.appendChild(actions);
    drawer.appendChild(errBanner);

    // Full JSON view
    const jsonPre = el("pre", {
      className: "code-mono",
      style: "background: var(--bg-subtle); padding: 12px; border: 1px solid var(--border); border-radius: 4px; overflow-x: auto; font-size: 11px;",
      textContent: JSON.stringify(request, null, 2),
    });
    drawer.appendChild(el("div", { className: "label-caps", style: "margin: 16px 0 6px 0;", textContent: "Raw JSON Record" }));
    drawer.appendChild(jsonPre);

    drawer.classList.add("open");
  }

  return { open, close };
}

let defaultDrawerInstance = null;

export async function openRequestDrawer(requestOrId, onUpdate) {
  defaultDrawerInstance = createRequestsDrawer(onUpdate);
  if (typeof requestOrId === "string") {
    try {
      const req = await api.getRequestById(requestOrId);
      if (req) defaultDrawerInstance.open(req);
    } catch (err) {
      console.error("Failed to load request for drawer", err);
    }
  } else if (requestOrId) {
    defaultDrawerInstance.open(requestOrId);
  }
}
