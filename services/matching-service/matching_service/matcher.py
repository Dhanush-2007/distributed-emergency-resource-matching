"""Matching algorithm implementing SPEC 8.5."""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from ems_common.haversine import haversine_km

from matching_service.clients import (
    RequestServiceClient,
    ResourceServiceClient,
    Transient,
)

logger = logging.getLogger("matching-service")


def _check_fault_injection(retry_count: int) -> None:
    """Check test fault injection environment switches."""
    delay_ms = int(os.getenv("FAULT_MATCH_DELAY_MS", "0"))
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)

    fault_fail = os.getenv("FAULT_MATCH_FAIL", "off").strip().lower()
    if fault_fail == "always":
        raise Transient("FAULT_MATCH_FAIL=always injected failure")
    if fault_fail not in ("off", "false", "0"):
        try:
            n = int(fault_fail)
            if retry_count < n:
                raise Transient(f"FAULT_MATCH_FAIL={n} injected failure on attempt {retry_count}")
        except ValueError:
            pass


def match_request(
    request_id: str,
    req_client: RequestServiceClient,
    res_client: ResourceServiceClient,
    retry_count: int = 0,
) -> str:
    """Execute match_request algorithm. Returns outcome string.

    Outcomes: "MATCHED", "NO_MATCH", "DUPLICATE", "DROPPED".
    """
    # 1. Load the request
    st, req_data = req_client.get_request(request_id)
    if st == 404:
        return "DROPPED"

    _check_fault_injection(retry_count)

    # 2. Check status
    if req_data.get("status") != "PENDING":
        return "DUPLICATE"

    # 3. By-request lookup (crash recovery / idempotency)
    existing = res_client.by_request(request_id)
    chosen_id: str | None = None
    candidate_log: list[dict[str, Any]] = []

    if existing is not None:
        chosen_id = existing.get("resourceId")
    else:
        # 5. Candidate search
        req_type = req_data.get("type", "")
        cands, partial = res_client.candidates(req_type)
        latest_partial = partial
        if not cands:
            if latest_partial:
                raise Transient(f"Candidates list empty but scan partial for type {req_type}")
            req_client.match_result(request_id, outcome="NO_MATCH")
            return "NO_MATCH"

        # 7. Sort candidates by distance and resourceId
        req_loc = req_data.get("location", {})
        r_lat, r_lon = float(req_loc.get("lat", 0.0)), float(req_loc.get("lon", 0.0))

        max_dist_str = os.getenv("MAX_MATCH_DISTANCE_KM")
        max_dist = float(max_dist_str) if max_dist_str else None

        def distance_for(cand: dict[str, Any]) -> float:
            c_loc = cand.get("location", {})
            return haversine_km(r_lat, r_lon, float(c_loc.get("lat", 0.0)), float(c_loc.get("lon", 0.0)))

        cands.sort(key=lambda c: (distance_for(c), c.get("resourceId", "")))

        chosen_cand: dict[str, Any] | None = None
        for c in cands:
            cid = c.get("resourceId", "")
            d_km = distance_for(c)
            if max_dist is not None and d_km > max_dist:
                candidate_log.append({"resourceId": cid, "distanceKm": round(d_km, 3), "decision": "TOO_FAR"})
                continue

            r_code = res_client.reserve(cid, request_id, c.get("version"))
            if r_code == 200:
                chosen_cand = c
                candidate_log.append({"resourceId": cid, "distanceKm": round(d_km, 3), "decision": "RESERVED"})
                break
            if r_code in (404, 409):
                candidate_log.append({"resourceId": cid, "distanceKm": round(d_km, 3), "decision": "CONFLICT"})
                continue
            raise Transient(f"Unexpected reserve status {r_code} on resource {cid}")

        # If none chosen, re-fetch candidates ONCE and retry once
        if chosen_cand is None:
            cands2, partial2 = res_client.candidates(req_type)
            latest_partial = partial2
            if not cands2:
                if latest_partial:
                    raise Transient(f"Re-fetch candidates empty and partial for type {req_type}")
                req_client.match_result(request_id, outcome="NO_MATCH")
                return "NO_MATCH"

            cands2.sort(key=lambda c: (distance_for(c), c.get("resourceId", "")))
            for c in cands2:
                cid = c.get("resourceId", "")
                d_km = distance_for(c)
                if max_dist is not None and d_km > max_dist:
                    continue
                r_code = res_client.reserve(cid, request_id, c.get("version"))
                if r_code == 200:
                    chosen_cand = c
                    candidate_log.append({"resourceId": cid, "distanceKm": round(d_km, 3), "decision": "RESERVED_RETRY"})
                    break
                if r_code in (404, 409):
                    continue
                raise Transient(f"Unexpected reserve status {r_code} on retry for resource {cid}")

        if chosen_cand is None:
            if latest_partial:
                raise Transient(f"Could not reserve candidate and candidates were partial for type {req_type}")
            req_client.match_result(request_id, outcome="NO_MATCH")
            return "NO_MATCH"

        chosen_id = chosen_cand.get("resourceId")

    assert chosen_id is not None
    table_str = " | ".join([f"{c['resourceId'][:8]} ({c['distanceKm']}km -> {c['decision']})" for c in candidate_log])
    logger.info("Candidate table for request %s: %s | Selected: %s", request_id, table_str, chosen_id)
    logger.info(
        "Match candidate evaluation",
        extra={
            "event": "match.candidates",
            "requestId": request_id,
            "chosenId": chosen_id,
            "candidates": candidate_log,
        },
    )

    # 8. Record match result (up to 3 tries on transient network failures)
    last_transient_exc: Exception | None = None
    for attempt in range(3):
        try:
            mr_code, _ = req_client.match_result(request_id, outcome="MATCHED", resource_id=chosen_id)
            if mr_code == 200:
                return "MATCHED"
            if mr_code == 409:
                # Request changed meanwhile: release and treat as duplicate
                res_client.release(chosen_id, request_id)
                return "DUPLICATE"
        except Transient as exc:
            last_transient_exc = exc
            if attempt < 2:
                time.sleep(0.1)

    # Failure after 3 tries: compensate by releasing resource and raise Transient
    res_client.release(chosen_id, request_id)
    raise Transient(f"Failed to record MATCHED outcome after 3 tries: {last_transient_exc}")
