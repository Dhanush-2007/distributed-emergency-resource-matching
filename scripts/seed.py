#!/usr/bin/env python3
"""Seed script for Distributed Emergency Resource Matching."""

from __future__ import annotations

import argparse
import os
import random
import sys

import httpx
from ems_common.haversine import haversine_km

REQ_URL = os.environ.get("REQ_URL", "http://localhost:8001").rstrip("/")
RES_URL = os.environ.get("RES_URL", "http://localhost:8002").rstrip("/")

ALL_TYPES = ["water", "food", "medicine", "transportation", "shelter"]


def seed_fixed(client: httpx.Client) -> None:
    center = {"lat": 12.9716, "lon": 77.5946}
    demo_resources = [
        {"name": "Water-Near-0.6km", "type": "water", "location": {"lat": 12.9770, "lon": 77.5946}, "availability": True},
        {"name": "Water-Mid-2km", "type": "water", "location": {"lat": 12.9896, "lon": 77.5946}, "availability": True},
        {"name": "Water-Far-6km", "type": "water", "location": {"lat": 13.0256, "lon": 77.5946}, "availability": True},
        {"name": "Food-VeryClose", "type": "food", "location": {"lat": 12.9718, "lon": 77.5948}, "availability": True},
    ]

    registered = []
    for r_data in demo_resources:
        resp = client.post(f"{RES_URL}/resources", json=r_data)
        if resp.status_code != 201:
            print(f"Error registering resource {r_data['name']}: {resp.status_code} {resp.text}", file=sys.stderr)
            sys.exit(1)
        body = resp.json()
        dist = haversine_km(center["lat"], center["lon"], r_data["location"]["lat"], r_data["location"]["lon"])
        registered.append({
            "resourceId": body["resourceId"],
            "name": r_data["name"],
            "type": r_data["type"],
            "distKm": dist,
        })
        print(f"Registered {r_data['name']} ({body['resourceId']}) - {r_data['type']} at {dist:.2f} km")

    water_res = [r for r in registered if r["type"] == "water"]
    water_res.sort(key=lambda x: x["distKm"])
    nearest_water = water_res[0]
    print(f"\nNearest water resource: {nearest_water['name']} ({nearest_water['resourceId']}) at {nearest_water['distKm']:.2f} km")


def seed_random(client: httpx.Client, n_req: int, n_res: int) -> None:
    rng = random.Random(42)
    center_lat, center_lon = 12.9716, 77.5946

    res_counts: dict[str, int] = {t: 0 for t in ALL_TYPES}
    req_counts: dict[str, int] = {t: 0 for t in ALL_TYPES}

    if n_res > 0:
        print(f"\n--- Seeding {n_res} Resources ---")
        for i in range(n_res):
            t = rng.choice(ALL_TYPES)
            lat = round(center_lat + rng.uniform(-0.05, 0.05), 6)
            lon = round(center_lon + rng.uniform(-0.05, 0.05), 6)
            payload = {
                "name": f"Resource-{t}-{i}",
                "type": t,
                "location": {"lat": lat, "lon": lon},
                "availability": True,
            }
            resp = client.post(f"{RES_URL}/resources", json=payload)
            if resp.status_code != 201:
                print(f"Error creating resource {i}: {resp.status_code} {resp.text}", file=sys.stderr)
                sys.exit(1)
            res_counts[t] += 1

    if n_req > 0:
        print(f"\n--- Seeding {n_req} Requests ---")
        for i in range(n_req):
            t = rng.choice(ALL_TYPES)
            lat = round(center_lat + rng.uniform(-0.05, 0.05), 6)
            lon = round(center_lon + rng.uniform(-0.05, 0.05), 6)
            payload = {
                "type": t,
                "location": {"lat": lat, "lon": lon},
                "details": f"Urgent relief needed for {t} incident #{i}",
            }
            resp = client.post(f"{REQ_URL}/requests", json=payload)
            if resp.status_code != 201:
                print(f"Error creating request {i}: {resp.status_code} {resp.text}", file=sys.stderr)
                sys.exit(1)
            req_counts[t] += 1

    print("\n=== Seeding Summary ===")
    print("Type            Resources    Requests")
    print("-------------------------------------")
    for t in ALL_TYPES:
        print(f"{t:<15} {res_counts[t]:<12} {req_counts[t]:<12}")
    print("-------------------------------------")
    print(f"Total:          {sum(res_counts.values()):<12} {sum(req_counts.values()):<12}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed emergency resources and requests")
    parser.add_argument("--fixed", action="store_true", help="Register deterministic demo set")
    parser.add_argument("--requests", type=int, default=0, help="Number of random requests to create")
    parser.add_argument("--resources", type=int, default=0, help="Number of random resources to create")
    args = parser.parse_args()

    if not args.fixed and args.requests == 0 and args.resources == 0:
        args.fixed = True

    with httpx.Client(timeout=10.0) as client:
        if args.fixed:
            seed_fixed(client)
        if args.requests > 0 or args.resources > 0:
            seed_random(client, args.requests, args.resources)


if __name__ == "__main__":
    main()
