#!/usr/bin/env python3
import math
import asyncio
import aiohttp

BASE_URL = "http://localhost:5000"
SIMULATION_CONFIG = {
    "seed": "jollyroom",
    "targetDispatches": 100000,  # Total emergencies to be generated
    "maxActiveCalls": 1000    # Maximum number of concurrent emergencies to process in one batch
}
POLL_TIMEOUT = 2  # seconds to wait for a new call before exiting

# Global availability cache for all five services and a shared lock.
availability_cache = {
    "Medical": {},
    "Fire": {},
    "Police": {},
    "Rescue": {},
    "Utility": {}
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
    """Background task that updates the global availability cache for all five services every 'interval' seconds."""
    global availability_cache
    while True:
        try:
            # Use asyncio.gather to fetch availability for each service concurrently.
            med, fire, police, rescue, utility = await asyncio.gather(
                get_available(session, "/medical/search"),
                get_available(session, "/fire/search"),
                get_available(session, "/police/search"),
                get_available(session, "/rescue/search"),
                get_available(session, "/utility/search")
            )
            async with cache_lock:
                availability_cache["Medical"] = med
                availability_cache["Fire"] = fire
                availability_cache["Police"] = police
                availability_cache["Rescue"] = rescue
                availability_cache["Utility"] = utility
        except Exception:
            # In production, log errors appropriately.
            pass
        await asyncio.sleep(interval)

async def process_multi_service_emergency_shared(session, call, location_details):
    """
    Process a single emergency call that may request any of the five emergency services.
    Uses the shared availability cache and updates it atomically to avoid overdispatch.
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

        # Build candidate list using cached data.
        async with cache_lock:
            available = availability_cache.get(service_type)
            if available is None:
                continue
            candidates = [
                (city, euclidean_distance(
                    (location_details[city]["latitude"], location_details[city]["longitude"]),
                    target_coord
                ))
                for city, count in available.items() if count > 0 and city in location_details
            ]
        candidates.sort(key=lambda candidate: candidate[1])

        remaining = quantity_needed
        for city, _ in candidates:
            if remaining <= 0:
                break
            dispatch_count = 0
            async with cache_lock:
                current_avail = availability_cache.get(service_type, {}).get(city, 0)
                if current_avail > 0:
                    dispatch_count = min(current_avail, remaining)
                    availability_cache[service_type][city] = current_avail - dispatch_count
            if dispatch_count > 0:
                endpoint = {
                    "Medical": "/medical/dispatch",
                    "Fire": "/fire/dispatch",
                    "Police": "/police/dispatch",
                    "Rescue": "/rescue/dispatch",
                    "Utility": "/utility/dispatch"
                }.get(service_type)
                if endpoint:
                    await dispatch(session, endpoint, city, target_city, dispatch_count, location_details)
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
        # Start the background updater for the availability cache.
        cache_task = asyncio.create_task(update_availability_cache(session, interval=5))

        # Reset the simulation.
        reset_result = await call_api(session, "/control/reset", method="POST", params=SIMULATION_CONFIG)
        if reset_result is None:
            cache_task.cancel()
            return

        location_details = await get_location_details(session)
        total_calls_processed = 0

        while total_calls_processed < SIMULATION_CONFIG["targetDispatches"]:
            pending_calls = await get_pending_calls(session)
            if not pending_calls:
                try:
                    next_call = await asyncio.wait_for(request_next_call(session), timeout=POLL_TIMEOUT)
                except asyncio.TimeoutError:
                    break
                if not next_call:
                    break
                pending_calls = [next_call]
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

        stop_result = await call_api(session, "/control/stop", method="POST")
        print(stop_result)
        cache_task.cancel()

if __name__ == '__main__':
    asyncio.run(main())
