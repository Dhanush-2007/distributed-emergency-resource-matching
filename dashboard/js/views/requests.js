import { api } from "../api.js";
import { el, formatTimestamp, renderBadge, renderIdCell } from "../utils.js";
import { openRequestDrawer } from "./requests_drawer.js";

let pollTimer = null;
let activeFilter = "ALL";
let lookupId = "";

export async function renderRequestsView(container) {
  container.replaceChildren();

  const titleRow = el("div", { class: "view-header" }, [
    el("h2", { class: "view-title" }, "Emergency Requests"),
    el("div", { class: "text-muted text-xs" }, "Manage and dispatch citizen emergency requests")
  ]);

  const formSection = renderRequestForm(() => refreshRequestsTable());
  const filterRow = renderFilterBar();
  const tableContainer = el("div", { id: "requests-table-container", class: "card mt-3" });

  container.append(titleRow, formSection, filterRow, tableContainer);
  await refreshRequestsTable();
}

function renderRequestForm(onCreated) {
  const card = el("div", { class: "card mb-3" });
  const head = el("div", { class: "card-header flex justify-between align-center" }, [
    el("div", { class: "card-title" }, "New Emergency Request"),
    el("span", { class: "text-muted text-xs" }, "POST /api/req/requests")
  ]);

  const errorBanner = el("div", { class: "error-banner mb-2", style: "display:none" });
  const stepper = el("div", { class: "step-indicator mb-3", style: "display:none" });

  const typeSelect = el("select", { class: "form-control" }, [
    el("option", { value: "WATER" }, "WATER"),
    el("option", { value: "FOOD" }, "FOOD"),
    el("option", { value: "MEDICINE" }, "MEDICINE"),
    el("option", { value: "SHELTER" }, "SHELTER"),
    el("option", { value: "TRANSPORTATION" }, "TRANSPORTATION")
  ]);

  const latInput = el("input", { type: "number", step: "any", min: "-90", max: "90", placeholder: "e.g. 12.9716", class: "form-control" });
  const lonInput = el("input", { type: "number", step: "any", min: "-180", max: "180", placeholder: "e.g. 77.5946", class: "form-control" });
  const detailsInput = el("textarea", { maxlength: "1000", rows: "2", placeholder: "Details / notes (required, 1-1000 chars)", class: "form-control" });
  const charCounter = el("span", { class: "text-muted text-xs" }, "0/1000");

  detailsInput.addEventListener("input", () => {
    charCounter.textContent = `${detailsInput.value.length}/1000`;
  });

  const submitBtn = el("button", { class: "btn btn-primary" }, "Submit Request");

  submitBtn.addEventListener("click", async () => {
    errorBanner.style.display = "none";
    stepper.style.display = "none";

    const lat = parseFloat(latInput.value);
    const lon = parseFloat(lonInput.value);
    if (isNaN(lat) || lat < -90 || lat > 90) {
      showError(errorBanner, "Latitude must be between -90 and 90");
      return;
    }
    if (isNaN(lon) || lon < -180 || lon > 180) {
      showError(errorBanner, "Longitude must be between -180 and 180");
      return;
    }

    const details = detailsInput.value.trim() || `Emergency request for ${typeSelect.value}`;

    submitBtn.disabled = true;
    submitBtn.textContent = "Submitting...";

    try {
      const payload = {
        type: typeSelect.value,
        lat: lat,
        lon: lon,
        details: details
      };
      const res = await api.createRequest(payload);
      detailsInput.value = "";
      charCounter.textContent = "0/1000";
      onCreated();
      trackRequestSteps(res.requestId, stepper);
    } catch (err) {
      const errMsg = (err.data && err.data.error && err.data.error.message) || err.message || "Failed to create request";
      showError(errorBanner, errMsg);
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Submit Request";
    }
  });

  const formGrid = el("div", { class: "form-grid" }, [
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Emergency Type"), typeSelect]),
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Latitude (-90..90)"), latInput]),
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Longitude (-180..180)"), lonInput]),
    el("div", { class: "form-group col-span-2" }, [
      el("div", { class: "flex justify-between" }, [el("label", { class: "label" }, "Details"), charCounter]),
      detailsInput
    ])
  ]);

  card.append(head, errorBanner, stepper, formGrid, el("div", { class: "mt-2 flex justify-end" }, [submitBtn]));
  return card;
}

function showError(banner, message) {
  banner.textContent = message;
  banner.style.display = "block";
}

function trackRequestSteps(id, stepper) {
  stepper.replaceChildren();
  stepper.style.display = "flex";

  const s1 = el("span", { class: "step done" }, "1. Created");
  const s2 = el("span", { class: "step active" }, "2. Queued");
  const s3 = el("span", { class: "step pending" }, "3. Outcome...");
  stepper.append(s1, s2, s3);

  let elapsed = 0;
  if (pollTimer) clearInterval(pollTimer);

  pollTimer = setInterval(async () => {
    elapsed += 1;
    try {
      const req = await api.getRequest(id);
      if (req.status === "MATCHED") {
        s2.className = "step done";
        s3.className = "step done";
        s3.textContent = `3. Matched (${req.matchedResourceId ? req.matchedResourceId.slice(0, 8) : "assigned"})`;
        clearInterval(pollTimer);
        refreshRequestsTable();
      } else if (req.status === "NO_MATCH") {
        s2.className = "step done";
        s3.className = "step failed";
        s3.textContent = "3. No match available";
        clearInterval(pollTimer);
        refreshRequestsTable();
      } else if (elapsed > 20) {
        s3.textContent = `3. Still ${req.status}`;
        clearInterval(pollTimer);
      }
    } catch {
      clearInterval(pollTimer);
    }
  }, 1000);
}

function renderFilterBar() {
  const bar = el("div", { class: "flex justify-between align-center mt-3 mb-2 flex-wrap gap-2" });
  const chips = el("div", { class: "filter-chips flex gap-1" });
  const statuses = ["ALL", "PENDING", "MATCHED", "IN_PROGRESS", "COMPLETED", "CANCELLED", "NO_MATCH"];

  statuses.forEach((st) => {
    const chip = el("button", {
      class: `chip ${activeFilter === st ? "active" : ""}`,
      onclick: () => {
        activeFilter = st;
        bar.querySelectorAll(".chip").forEach((c) => c.classList.remove("active"));
        chip.classList.add("active");
        refreshRequestsTable();
      }
    }, st);
    chips.appendChild(chip);
  });

  const searchBox = el("input", {
    type: "text",
    id: "lookup-input",
    placeholder: "Filter / Lookup ID (/ to focus)",
    class: "form-control search-input",
    value: lookupId,
    oninput: (e) => {
      lookupId = e.target.value.trim().toLowerCase();
      refreshRequestsTable();
    }
  });

  bar.append(chips, searchBox);
  return bar;
}

export async function refreshRequestsTable() {
  const container = document.getElementById("requests-table-container");
  if (!container) return;

  try {
    const items = await api.listRequests();
    let filtered = items;
    if (activeFilter !== "ALL") {
      filtered = filtered.filter((r) => r.status === activeFilter);
    }
    if (lookupId) {
      filtered = filtered.filter((r) => (r.requestId && r.requestId.toLowerCase().includes(lookupId)) || (r.type && r.type.toLowerCase().includes(lookupId)));
    }

    if (filtered.length === 0) {
      container.replaceChildren(el("div", { class: "p-4 text-center text-muted" }, "No emergency requests found."));
      return;
    }

    const table = el("table", { class: "table" });
    const thead = el("thead", {}, [
      el("tr", {}, [
        el("th", {}, "Request ID"),
        el("th", {}, "Type"),
        el("th", {}, "Coordinates"),
        el("th", {}, "Status"),
        el("th", {}, "Created"),
        el("th", {}, "Action")
      ])
    ]);

    const tbody = el("tbody");
    filtered.forEach((r) => {
      const latVal = r.lat !== undefined ? r.lat : (r.location && r.location.lat);
      const lonVal = r.lon !== undefined ? r.lon : (r.location && r.location.lon);
      const locStr = (latVal !== undefined && lonVal !== undefined && !isNaN(latVal) && !isNaN(lonVal))
        ? `${Number(latVal).toFixed(4)}, ${Number(lonVal).toFixed(4)}`
        : "-";

      const tr = el("tr", {
        class: "cursor-pointer",
        onclick: (e) => {
          if (e.target.tagName !== "BUTTON") openRequestDrawer(r.requestId, () => refreshRequestsTable());
        }
      }, [
        renderIdCell(r.requestId),
        el("td", {}, r.type || "-"),
        el("td", { class: "mono text-xs" }, locStr),
        el("td", {}, [renderBadge(r.status)]),
        el("td", { class: "text-xs text-muted" }, formatTimestamp(r.createdAt)),
        el("td", {}, [
          el("button", {
            class: "btn btn-sm btn-ghost",
            onclick: (e) => {
              e.stopPropagation();
              openRequestDrawer(r.requestId, () => refreshRequestsTable());
            }
          }, "Inspect")
        ])
      ]);
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    container.replaceChildren(table);
  } catch (err) {
    container.replaceChildren(el("div", { class: "error-banner" }, `Error loading requests: ${err.message}`));
  }
}
