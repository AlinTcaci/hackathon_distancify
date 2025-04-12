#!/usr/bin/env python3
import math
import asyncio
import aiohttp

BASE_URL = "http://localhost:5000"
SIMULATION_CONFIG = {
    "seed": "default",
    "targetDispatches": 10000,  # Total emergencies to be generated
    "maxActiveCalls": 1000     # Maximum number of concurrent emergencies to process in one batch
}
MAX_CONSECUTIVE_EMPTY_CALLS = 1

# Global availability cache and lock
availability_cache = {
    "Medical": {},
    "Fire": {},
    "Police": {}
}
cache_lock = asyncio.Lock()

async def call_api(session, endpoint, method="GET", payload=None, params=None):
    url = f"{BASE_URL}{endpoint}"
    try:
        if method.upper() == "GET":
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                if not text.strip():
                    return ""
                try:
                    return await response.json()
                except Exception:
                    return text
        elif method.upper() == "POST":
            async with session.post(url, json=payload, params=params) as response:
                response.raise_for_status()
                text = await response.text()
                if not text.strip():
                    return ""
                try:
                    return await response.json()
                except Exception:
                    return text
        else:
            raise ValueError("Unsupported HTTP method")
    except Exception:
        return None

def euclidean_distance(coord1, coord2):
    return math.hypot(coord1[0] - coord2[0], coord1[1] - coord2[1])

async def get_location_details(session):
    data = await call_api(session, "/locations")
    if not data:
        raise RuntimeError("Failed to retrieve locations")
    locations = {}
    for loc in data:
        city = loc.get("city") or loc.get("name")
        if not city:
            raise ValueError(f"Missing city name for location: {loc}")
        county = loc.get("county") or "unknown"
        latitude = loc.get("latitude") or loc.get("lat")
        longitude = loc.get("longitude") or loc.get("long")
        if latitude is None or longitude is None:
            raise ValueError(f"Missing coordinates for location: {loc}")
        locations[city] = {
            "county": county,
            "city": city,
            "latitude": latitude,
            "longitude": longitude
        }
    return locations

async def get_available(session, endpoint):
    data = await call_api(session, endpoint)
    if not data:
        raise RuntimeError(f"Failed to retrieve available resources from {endpoint}")
    available = {}
    for item in data:
        city = item.get("city") or item.get("name") or "unknown"
        count = item.get("available", item.get("quantity", 0))
        available[city] = count
    return available

async def dispatch(session, endpoint, source_city, target_city, count, location_details):
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
    result = await call_api(session, endpoint, method="POST", payload=payload)
    return result

async def update_availability_cache(session, interval=5):
    """Background task to update the global availability cache every 'interval' seconds."""
    global availability_cache
    while True:
        try:
            med, fire, police = await asyncio.gather(
                get_available(session, "/medical/search"),
                get_available(session, "/fire/search"),
                get_available(session, "/police/search")
            )
            async with cache_lock:
                availability_cache["Medical"] = med
                availability_cache["Fire"] = fire
                availability_cache["Police"] = police
        except Exception:
            # In a production system, add error logging here.
            pass
        await asyncio.sleep(interval)

async def process_multi_service_emergency_shared(session, call, location_details):
    """
    Process an emergency call using the shared (cached) availability info.
    Uses cache_lock to read/update the availability_cache.
    """
    target_city = call.get("city")
    if not target_city or target_city not in location_details:
        return False

    requests_list = call.get("requests", [])
    if not requests_list:
        return False

    target_coord = (
        location_details[target_city]["latitude"],
        location_details[target_city]["longitude"]
    )
    all_success = True

    for req in requests_list:
        service_type = req.get("Type")
        quantity_needed = req.get("Quantity", 0)
        if quantity_needed <= 0:
            continue

        async with cache_lock:
            available = availability_cache.get(service_type)
            if available is None:
                continue
            # Build candidate list from cached availability
            candidates = []
            for city, count in available.items():
                if count > 0 and city in location_details:
                    src_coord = (
                        location_details[city]["latitude"],
                        location_details[city]["longitude"]
                    )
                    distance = euclidean_distance(src_coord, target_coord)
                    candidates.append((city, distance, count))
            candidates.sort(key=lambda candidate: candidate[1])

        remaining = quantity_needed
        for city, _, avail_count in candidates:
            if remaining <= 0:
                break
            dispatch_count = min(avail_count, remaining)
            endpoint = None
            if service_type == "Medical":
                endpoint = "/medical/dispatch"
            elif service_type == "Fire":
                endpoint = "/fire/dispatch"
            elif service_type == "Police":
                endpoint = "/police/dispatch"
            else:
                continue

            # Dispatch asynchronously (POST calls are still made individually)
            await dispatch(session, endpoint, city, target_city, dispatch_count, location_details)

            # Update the shared cache immediately
            async with cache_lock:
                availability_cache[service_type][city] -= dispatch_count
            remaining -= dispatch_count

        if remaining > 0:
            all_success = False
    return all_success

async def get_pending_calls(session):
    params = {"limit": SIMULATION_CONFIG["maxActiveCalls"]}
    data = await call_api(session, "/calls/queue", params=params)
    if not data:
        return []
    return data

async def request_next_call(session):
    data = await call_api(session, "/calls/next")
    if not data:
        return []
    return data

async def main():
    async with aiohttp.ClientSession() as session:
        # Start the background cache updater
        cache_task = asyncio.create_task(update_availability_cache(session, interval=5))

        # Reset simulation
        reset_result = await call_api(session, "/control/reset", method="POST", params=SIMULATION_CONFIG)
        if reset_result is None:
            cache_task.cancel()
            return

        location_details = await get_location_details(session)
        total_calls_processed = 0
        consecutive_empty_calls = 0

        while total_calls_processed < SIMULATION_CONFIG["targetDispatches"]:
            pending_calls = await get_pending_calls(session)
            if pending_calls:
                consecutive_empty_calls = 0
                coros = [
                    process_multi_service_emergency_shared(session, call, location_details)
                    for call in pending_calls if call
                ]
                results = await asyncio.gather(*coros, return_exceptions=True)
                for result in results:
                    if result is True:
                        total_calls_processed += 1
                    if total_calls_processed >= SIMULATION_CONFIG["targetDispatches"]:
                        break
            else:
                next_call = await request_next_call(session)
                if not next_call:
                    consecutive_empty_calls += 1
                    if consecutive_empty_calls >= MAX_CONSECUTIVE_EMPTY_CALLS:
                        break
                else:
                    consecutive_empty_calls = 0
                    if await process_multi_service_emergency_shared(session, next_call, location_details):
                        total_calls_processed += 1

        stop_result = await call_api(session, "/control/stop", method="POST")
        print(stop_result)
        cache_task.cancel()

if __name__ == '__main__':
    asyncio.run(main())
