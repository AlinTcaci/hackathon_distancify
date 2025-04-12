#!/usr/bin/env python3
import math
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:5000"

SIMULATION_CONFIG = {
    "seed": "default",
    "targetDispatches": 10000,
    "maxActiveCalls": 1000
}

MAX_CONSECUTIVE_EMPTY_CALLS = 1


def call_api(endpoint, method="GET", payload=None, params=None):
    url = f"{BASE_URL}{endpoint}"
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, json=payload, params=params)
        else:
            raise ValueError("Unsupported HTTP method")
        response.raise_for_status()
        return response.json() if response.text.strip() else ""
    except requests.RequestException:
        return None


def euclidean_distance(coord1, coord2):
    return math.hypot(coord1[0] - coord2[0], coord1[1] - coord2[1])


def get_location_details():
    data = call_api("/locations")
    locations = {
        loc.get("city") or loc.get("name"): {
            "county": loc.get("county") or "unknown",
            "city": loc.get("city") or loc.get("name"),
            "latitude": loc.get("latitude") or loc.get("lat"),
            "longitude": loc.get("longitude") or loc.get("long")
        }
        for loc in data or []
        if (loc.get("latitude") or loc.get("lat")) is not None and (loc.get("longitude") or loc.get("long")) is not None
    }
    return locations


def get_available(service):
    endpoint = f"/{service}/search"
    data = call_api(endpoint)
    return {
        item.get("city") or item.get("name") or "unknown": item.get("available", item.get("quantity", 0))
        for item in data or []
    }


def dispatch(service, source_city, target_city, count, location_details):
    src = location_details.get(source_city)
    tgt = location_details.get(target_city)
    if not src or not tgt:
        return None
    payload = {
        "sourceCounty": src["county"],
        "sourceCity": src["city"],
        "targetCounty": tgt["county"],
        "targetCity": tgt["city"],
        "quantity": count
    }
    return call_api(f"/{service}/dispatch", method="POST", payload=payload)


def process_call(call, location_details):
    target_city = call.get("city")
    if not target_city or target_city not in location_details:
        return False

    requests_list = call.get("requests", [])
    if not requests_list:
        return False

    with ThreadPoolExecutor() as executor:
        futures = {
            service: executor.submit(get_available, service.lower())
            for service in ["medical", "fire", "police"]
        }
        available_resources = {k.capitalize(): f.result() for k, f in futures.items()}

    target_coord = (location_details[target_city]["latitude"], location_details[target_city]["longitude"])
    all_success = True

    for req in requests_list:
        service = req.get("Type")
        qty = req.get("Quantity", 0)
        if qty <= 0 or service not in available_resources:
            continue

        candidates = [
            (city, euclidean_distance((location_details[city]["latitude"], location_details[city]["longitude"]), target_coord), count)
            for city, count in available_resources[service].items()
            if count > 0 and city in location_details
        ]
        candidates.sort(key=lambda c: c[1])

        remaining = qty
        for city, _, avail in candidates:
            if remaining <= 0:
                break
            dispatch_count = min(avail, remaining)
            dispatch(service.lower(), city, target_city, dispatch_count, location_details)
            available_resources[service][city] -= dispatch_count
            remaining -= dispatch_count

        if remaining > 0:
            all_success = False

    return all_success


def get_pending_calls():
    return call_api("/calls/queue", params={"limit": SIMULATION_CONFIG["maxActiveCalls"]}) or []


def request_next_call():
    return call_api("/calls/next") or []


def main():
    if call_api("/control/reset", method="POST", params=SIMULATION_CONFIG) is None:
        return

    location_details = get_location_details()
    total_calls = 0
    empty_count = 0

    while total_calls < SIMULATION_CONFIG["targetDispatches"]:
        calls = get_pending_calls()
        if not calls:
            call = request_next_call()
            if not call:
                empty_count += 1
                if empty_count >= MAX_CONSECUTIVE_EMPTY_CALLS:
                    break
            else:
                empty_count = 0
                if process_call(call, location_details):
                    total_calls += 1
        else:
            empty_count = 0
            for call in calls:
                if process_call(call, location_details):
                    total_calls += 1
                if total_calls >= SIMULATION_CONFIG["targetDispatches"]:
                    break

    stop_result = call_api("/control/stop", method="POST")
    print(stop_result)


if __name__ == "__main__":
    main()
