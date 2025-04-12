#!/usr/bin/env python3
import math
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

BASE_URL = "http://localhost:5000"
SIMULATION_CONFIG = {
    "seed": "default",
    "targetDispatches": 1000,  # Total emergencies to be generated
    "maxActiveCalls": 100     # Maximum number of concurrent emergencies to process in one batch
}

# Maximum consecutive polls for emptiness before we decide there are no further calls.
MAX_CONSECUTIVE_EMPTY_CALLS = 1

# Create a global session for connection reuse.
session = requests.Session()

def call_api(endpoint, method="GET", payload=None, params=None):
    url = f"{BASE_URL}{endpoint}"
    try:
        if method.upper() == "GET":
            response = session.get(url, params=params)
        elif method.upper() == "POST":
            response = session.post(url, json=payload, params=params)
        else:
            raise ValueError("Unsupported HTTP method")
        response.raise_for_status()

        if response.text.strip() == "":
            return ""
        try:
            return response.json()
        except Exception:
            return response.text
    except requests.RequestException as e:
        print(f"Error during API call to {endpoint}: {e}")
        return None

def euclidean_distance(coord1, coord2):
    """Compute the Euclidean (linear) distance between two 2D points."""
    return math.hypot(coord1[0] - coord2[0], coord1[1] - coord2[1])

def get_location_details():
    data = call_api("/locations")
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

def get_available_ambulances():
    data = call_api("/medical/search")
    if not data:
        raise RuntimeError("Failed to retrieve available ambulances")
    available = {}
    for item in data:
        city = item.get("city") or item.get("name") or "unknown"
        count = item.get("available", item.get("quantity", 0))
        available[city] = count
    return available

def get_available_firefighters():
    data = call_api("/fire/search")
    if not data:
        raise RuntimeError("Failed to retrieve available firefighters")
    available = {}
    for item in data:
        city = item.get("city") or item.get("name") or "unknown"
        count = item.get("available", item.get("quantity", 0))
        available[city] = count
    return available

def get_available_police():
    data = call_api("/police/search")
    if not data:
        raise RuntimeError("Failed to retrieve available police vehicles")
    available = {}
    for item in data:
        city = item.get("city") or item.get("name") or "unknown"
        count = item.get("available", item.get("quantity", 0))
        available[city] = count
    return available

def dispatch_ambulances(source_city, target_city, count, location_details):
    source_detail = location_details.get(source_city)
    target_detail = location_details.get(target_city)
    if source_detail is None or target_detail is None:
        print("Error: Missing location details for ambulance dispatch.")
        return None
    payload = {
        "sourceCounty": source_detail.get("county"),
        "sourceCity": source_detail.get("city"),
        "targetCounty": target_detail.get("county"),
        "targetCity": target_detail.get("city"),
        "quantity": count
    }
    result = call_api("/medical/dispatch", method="POST", payload=payload)
    if result is None:
        print(f"Dispatch error: Unable to dispatch {count} ambulance(s) from {source_city} to {target_city}.")
    else:
        print(f"Dispatched {count} ambulance(s) from {source_city} to {target_city}.")
    return result

def dispatch_firefighters(source_city, target_city, count, location_details):
    source_detail = location_details.get(source_city)
    target_detail = location_details.get(target_city)
    if source_detail is None or target_detail is None:
        print("Error: Missing location details for firefighter dispatch.")
        return None
    payload = {
        "sourceCounty": source_detail.get("county"),
        "sourceCity": source_detail.get("city"),
        "targetCounty": target_detail.get("county"),
        "targetCity": target_detail.get("city"),
        "quantity": count
    }
    result = call_api("/fire/dispatch", method="POST", payload=payload)
    if result is None:
        print(f"Dispatch error: Unable to dispatch {count} firefighter unit(s) from {source_city} to {target_city}.")
    else:
        print(f"Dispatched {count} firefighter unit(s) from {source_city} to {target_city}.")
    return result

def dispatch_police(source_city, target_city, count, location_details):
    source_detail = location_details.get(source_city)
    target_detail = location_details.get(target_city)
    if source_detail is None or target_detail is None:
        print("Error: Missing location details for police dispatch.")
        return None
    payload = {
        "sourceCounty": source_detail.get("county"),
        "sourceCity": source_detail.get("city"),
        "targetCounty": target_detail.get("county"),
        "targetCity": target_detail.get("city"),
        "quantity": count
    }
    result = call_api("/police/dispatch", method="POST", payload=payload)
    if result is None:
        print(f"Dispatch error: Unable to dispatch {count} police unit(s) from {source_city} to {target_city}.")
    else:
        print(f"Dispatched {count} police unit(s) from {source_city} to {target_city}.")
    return result

def process_multi_service_emergency(call, location_details):
    """
    Process an emergency call that may request multiple service types.
    This version executes the three resource availability API calls concurrently.
    """
    target_city = call.get("city")
    if not target_city:
        print("Error: Emergency call missing 'city' field.")
        return False

    requests_list = call.get("requests", [])
    if not requests_list:
        print("Error: No service requests provided in the call.")
        return False

    # Fetch available resources concurrently.
    with ThreadPoolExecutor(max_workers=3) as local_executor:
        future_med = local_executor.submit(get_available_ambulances)
        future_fire = local_executor.submit(get_available_firefighters)
        future_police = local_executor.submit(get_available_police)
        available_resources = {
            "Medical": future_med.result(),
            "Fire": future_fire.result(),
            "Police": future_police.result()
        }

    all_success = True  # Track if all dispatches were fulfilled
    for req in requests_list:
        service_type = req.get("Type")
        quantity_needed = req.get("Quantity", 0)
        if quantity_needed <= 0:
            print(f"Skipping service '{service_type}' with non-positive quantity {quantity_needed}.")
            continue

        print(f"\nEmergency call: {quantity_needed} unit(s) of {service_type} required at {target_city}.")
        if target_city not in location_details:
            print(f"Error: Target city '{target_city}' not found in location details.")
            continue

        target_coord = (location_details[target_city]["latitude"], location_details[target_city]["longitude"])
        available = available_resources.get(service_type)
        if available is None:
            print(f"Error: Unknown service type '{service_type}'.")
            continue

        # Gather candidate cities with available units, sorted by Euclidean distance.
        candidates = []
        for city, count in available.items():
            if count > 0 and city in location_details:
                source_detail = location_details[city]
                source_coord = (source_detail["latitude"], source_detail["longitude"])
                distance = euclidean_distance(source_coord, target_coord)
                candidates.append((city, distance, count))
        candidates.sort(key=lambda candidate: candidate[1])

        remaining = quantity_needed
        for city, distance, avail_count in candidates:
            if remaining <= 0:
                break
            dispatch_count = min(avail_count, remaining)
            if service_type == "Medical":
                dispatch_ambulances(city, target_city, dispatch_count, location_details)
            elif service_type == "Fire":
                dispatch_firefighters(city, target_city, dispatch_count, location_details)
            elif service_type == "Police":
                dispatch_police(city, target_city, dispatch_count, location_details)
            else:
                print(f"Unknown service type: {service_type}")
                continue
            available[city] -= dispatch_count
            remaining -= dispatch_count

        if remaining > 0:
            print(f"Warning: Unable to dispatch {remaining} unit(s) of {service_type} for {target_city}!")
            all_success = False
        else:
            print(f"Emergency resolved: all required {service_type} units dispatched successfully.")
    return all_success

def get_pending_calls():
    """
    Retrieve emergencies from the /calls/queue endpoint.
    """
    params = {"limit": SIMULATION_CONFIG["maxActiveCalls"]}
    data = call_api("/calls/queue", params=params)
    if not data or (isinstance(data, str) and data.strip() == ""):
        return []
    return data

def request_next_call():
    """
    Request the next emergency call using the /calls/next endpoint.
    """
    data = call_api("/calls/next")
    if not data or (isinstance(data, str) and data.strip() == ""):
        return []
    return data

def main():
    print("Starting emergency service simulation (optimized)...")

    # Reset the simulation.
    reset_result = call_api("/control/reset", method="POST", payload=None, params=SIMULATION_CONFIG)
    if reset_result is None:
        print("Failed to reset simulation.")
        return
    print("Simulation reset successful. Configuration:")
    print(SIMULATION_CONFIG)

    print("\nFetching locations...")
    try:
        location_details = get_location_details()
    except Exception as e:
        print("Error getting location details:", e)
        return

    print(f"Retrieved {len(location_details)} locations.")

    total_calls_processed = 0
    consecutive_empty_calls = 0

    # A lock for synchronizing updates to total_calls_processed.
    counter_lock = threading.Lock()

    # Create a thread pool for processing emergency calls concurrently.
    with ThreadPoolExecutor(max_workers=50) as executor:
        while total_calls_processed < SIMULATION_CONFIG["targetDispatches"]:
            pending_calls = get_pending_calls()
            if pending_calls:
                consecutive_empty_calls = 0
                print(f"Processing batch of {len(pending_calls)} call(s)...")
                futures = []
                for call in pending_calls:
                    if isinstance(call, str) and call.strip() == "":
                        continue
                    # Submit the processing task concurrently.
                    future = executor.submit(process_multi_service_emergency, call, location_details)
                    futures.append(future)
                # As futures complete, update the processed count.
                for future in as_completed(futures):
                    try:
                        success = future.result()
                        if success:
                            with counter_lock:
                                total_calls_processed += 1
                    except Exception as e:
                        print("Error processing call:", e)
                    if total_calls_processed >= SIMULATION_CONFIG["targetDispatches"]:
                        break
                # (Optional) Remove or reduce the delay if needed.
                # time.sleep(0.1)
            else:
                next_call = request_next_call()
                if not next_call:
                    consecutive_empty_calls += 1
                    print("Queue is empty, requesting next call... "
                          f"(consecutive empty calls: {consecutive_empty_calls})")
                    if consecutive_empty_calls >= MAX_CONSECUTIVE_EMPTY_CALLS:
                        print("No further calls available; stopping simulation.")
                        break
                else:
                    consecutive_empty_calls = 0
            # Reduced sleep delay for faster looping.
            # time.sleep(0.1)

    print("\nAll emergencies processed or no further calls available. Stopping simulation...")
    stop_result = call_api("/control/stop", method="POST")
    print("\nSimulation finished. Final results:")
    print(stop_result)
    print(f"Total emergency calls processed: {total_calls_processed}")

if __name__ == '__main__':
    main()
