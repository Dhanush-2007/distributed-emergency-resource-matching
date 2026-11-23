import { api } from "../api.js";
import { el, formatTimestamp, renderBadge, renderIdCell } from "../utils.js";

let activeStatusFilter = "ALL";
let activeTypeFilter = "ALL";

export async function renderResourcesView(container) {
  container.replaceChildren();

  const titleRow = el("div", { class: "view-header" }, [
    el("h2", { class: "view-title" }, "Emergency Resources"),
    el("div", { class: "text-muted text-xs" }, "Inventory of deployable emergency assets")
  ]);

  const formSection = renderResourceForm(() => refreshResourcesTable());
  const filterRow = renderFilterBar();
  const tableContainer = el("div", { id: "resources-table-container", class: "card mt-3" });

  container.append(titleRow, formSection, filterRow, tableContainer);
  await refreshResourcesTable();
}

function renderResourceForm(onCreated) {
  const card = el("div", { class: "card mb-3" });
  const head = el("div", { class: "card-header flex justify-between align-center" }, [
    el("div", { class: "card-title" }, "Register New Resource"),
    el("span", { class: "text-muted text-xs" }, "POST /api/res/resources")
  ]);

  const errorBanner = el("div", { class: "error-banner mb-2", style: "display:none" });

  const nameInput = el("input", { type: "text", maxlength: "100", placeholder: "e.g. Tanker Alpha (optional)", class: "form-control" });
  const typeSelect = el("select", { class: "form-control" }, [
    el("option", { value: "WATER" }, "WATER"),
    el("option", { value: "FOOD" }, "FOOD"),
    el("option", { value: "MEDICINE" }, "MEDICINE"),
    el("option", { value: "SHELTER" }, "SHELTER"),
    el("option", { value: "TRANSPORTATION" }, "TRANSPORTATION")
  ]);
  const latInput = el("input", { type: "number", step: "any", min: "-90", max: "90", placeholder: "e.g. 12.9720", class: "form-control" });
  const lonInput = el("input", { type: "number", step: "any", min: "-180", max: "180", placeholder: "e.g. 77.5950", class: "form-control" });

  const availCheck = el("input", { type: "checkbox", checked: true, class: "checkbox-input" });
  const availLabel = el("label", { class: "flex align-center gap-1 text-xs" }, [availCheck, el("span", {}, "Available for immediate dispatch")]);

  const submitBtn = el("button", { class: "btn btn-primary" }, "Register Resource");

  submitBtn.addEventListener("click", async () => {
    errorBanner.style.display = "none";
    const lat = parseFloat(latInput.value);
    const lon = parseFloat(lonInput.value);
    if (isNaN(lat) || lat < -90 || lat > 90) {
      errorBanner.textContent = "Latitude must be between -90 and 90";
      errorBanner.style.display = "block";
      return;
    }
    if (isNaN(lon) || lon < -180 || lon > 180) {
      errorBanner.textContent = "Longitude must be between -180 and 180";
      errorBanner.style.display = "block";
      return;
    }

    submitBtn.disabled = true;
    submitBtn.textContent = "Registering...";

    try {
      const payload = {
        name: nameInput.value.trim() || undefined,
        type: typeSelect.value,
        lat: lat,
        lon: lon,
        availability: availCheck.checked
      };
      await api.createResource(payload);
      nameInput.value = "";
      onCreated();
    } catch (err) {
      const errMsg = (err.data && err.data.error && err.data.error.message) || err.message || "Failed to register resource";
      errorBanner.textContent = errMsg;
      errorBanner.style.display = "block";
    } finally {
      submitBtn.disabled = false;
      submitBtn.textContent = "Register Resource";
    }
  });

  const formGrid = el("div", { class: "form-grid" }, [
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Name (Optional)"), nameInput]),
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Type"), typeSelect]),
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Latitude (-90..90)"), latInput]),
    el("div", { class: "form-group" }, [el("label", { class: "label" }, "Longitude (-180..180)"), lonInput]),
    el("div", { class: "form-group col-span-2 pt-1" }, [availLabel])
  ]);

  card.append(head, errorBanner, formGrid, el("div", { class: "mt-2 flex justify-end" }, [submitBtn]));
  return card;
}

function renderFilterBar() {
  const bar = el("div", { class: "flex justify-between align-center mt-3 mb-2 flex-wrap gap-2" });
  const statuses = ["ALL", "AVAILABLE", "ASSIGNED", "OFFLINE"];
  const types = ["ALL", "WATER", "FOOD", "MEDICINE", "SHELTER", "TRANSPORTATION"];

  const statusChips = el("div", { class: "flex gap-1" });
  statuses.forEach((st) => {
    const chip = el("button", {
      class: `chip ${activeStatusFilter === st ? "active" : ""}`,
      onclick: () => {
        activeStatusFilter = st;
        statusChips.querySelectorAll(".chip").forEach((c) => c.classList.remove("active"));
        chip.classList.add("active");
        refreshResourcesTable();
      }
    }, st);
    statusChips.appendChild(chip);
  });

  const typeSelect = el("select", {
    class: "form-control text-xs",
    style: "width: auto;",
    onchange: (e) => {
      activeTypeFilter = e.target.value;
      refreshResourcesTable();
    }
  }, types.map((t) => el("option", { value: t }, t === "ALL" ? "All Types" : t)));

  bar.append(statusChips, typeSelect);
  return bar;
}

export async function refreshResourcesTable() {
  const container = document.getElementById("resources-table-container");
  if (!container) return;

  try {
    const items = await api.listResources();
    let filtered = items;
    if (activeStatusFilter !== "ALL") filtered = filtered.filter((r) => r.status === activeStatusFilter);
    if (activeTypeFilter !== "ALL") filtered = filtered.filter((r) => r.type === activeTypeFilter);

    if (filtered.length === 0) {
      container.replaceChildren(el("div", { class: "p-4 text-center text-muted" }, "No resources found."));
      return;
    }

    const table = el("table", { class: "table" });
    const thead = el("thead", {}, [
      el("tr", {}, [
        el("th", {}, "Resource ID"),
        el("th", {}, "Name"),
        el("th", {}, "Type"),
        el("th", {}, "Location"),
        el("th", {}, "Status"),
        el("th", {}, "Registered"),
        el("th", {}, "Actions")
      ])
    ]);

    const tbody = el("tbody");
    filtered.forEach((r) => {
      const isAssigned = r.status === "ASSIGNED";
      const isAvail = r.status === "AVAILABLE";

      const latVal = r.lat !== undefined ? r.lat : (r.location && r.location.lat);
      const lonVal = r.lon !== undefined ? r.lon : (r.location && r.location.lon);
      const locStr = (latVal !== undefined && lonVal !== undefined && !isNaN(latVal) && !isNaN(lonVal))
        ? `${Number(latVal).toFixed(4)}, ${Number(lonVal).toFixed(4)}`
        : "-";

      const toggleBtn = el("button", {
        class: "btn btn-xs btn-ghost",
        disabled: isAssigned,
        title: isAssigned ? "Cannot toggle while assigned to a request" : "",
        onclick: async () => {
          const nextStatus = isAvail ? "OFFLINE" : "AVAILABLE";
          toggleBtn.disabled = true;
          try {
            await api.updateResourceStatus(r.resourceId, nextStatus);
            await refreshResourcesTable();
          } catch (err) {
            const errMsg = (err.data && err.data.error && err.data.error.message) || err.message || "Failed to update status";
            alert(errMsg);
            toggleBtn.disabled = false;
          }
        }
      }, isAvail ? "Set Offline" : "Set Available");

      const tr = el("tr", {}, [
        renderIdCell(r.resourceId),
        el("td", {}, r.name || "-"),
        el("td", {}, r.type || "-"),
        el("td", { class: "mono text-xs" }, locStr),
        el("td", {}, [renderBadge(r.status)]),
        el("td", { class: "text-xs text-muted" }, formatTimestamp(r.createdAt)),
        el("td", {}, [toggleBtn])
      ]);
      tbody.appendChild(tr);
    });

    table.append(thead, tbody);
    container.replaceChildren(table);
  } catch (err) {
    container.replaceChildren(el("div", { class: "error-banner" }, `Error loading resources: ${err.message}`));
  }
}
