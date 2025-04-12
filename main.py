import math
import time
import requests
import json

BASE_URL = "http://localhost:5000"

SIMULATION_CONFIG = {
    "seed": "default",
    "targetDispatches": 10000,  # Total emergencies to be generated
    "maxActiveCalls": 100  # Maximum number of concurrent emergencies
}


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

        # If the response text is empty, return an empty string
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
    """
    Compute the Euclidean (linear) distance between two 2D points.
    """
    return math.hypot(coord1[0] - coord2[0], coord1[1] - coord2[1])


def get_location_details():
    """
    Retrieve locations from the /locations endpoint.

    Expected JSON objects include keys such as:
      - "county": the county name.
      - "city" (or "name"): the city name.
      - "latitude"/"lat": the latitude.
      - "longitude"/"long": the longitude.

    Returns a dictionary mapping city names to a dictionary of details.
    """
    data = call_api("/locations")
    if not data:
        raise RuntimeError("Failed to retrieve locations")

    locations = {}
    for loc in data:
        # Get the city name from 'city' or 'name'
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


def dispatch_ambulances(source_city, target_city, count, location_details):
    """
    Dispatch a number of ambulances from a source city to a target city.

    The payload is constructed with the following parameters:
      - "sourceCounty": County name of the source.
      - "sourceCity": Source city name.
      - "targetCounty": County name of the target.
      - "targetCity": Target city name.
      - "quantity": The number of ambulances to dispatch.
    """
    source_detail = location_details.get(source_city)
    target_detail = location_details.get(target_city)

    if source_detail is None or target_detail is None:
        print("Error: Missing location details for dispatch.")
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
        print(f"Dispatch error: Unable to dispatch from {source_city} to {target_city} for {count} units.")
    else:
        print(f"Dispatched {count} ambulance(s) from {source_city} to {target_city}.")
    return result


def process_emergency(call, location_details, available):
    """
    Process a single emergency call by dispatching the required number of ambulances.

    The emergency call must include:
      - "city": the target city where ambulances are needed.
      - One of the keys for the required count: "needed", "required", or "count", or alternatively,
        a "requests" array with objects having a "Quantity" field.

    This function selects candidate source cities (with available ambulances), calculates their
    Euclidean distance (based on latitude and longitude) to the target city, and dispatches from
    the nearest cities until the requirement is met.

    Returns True if the dispatch fulfills the requirement; otherwise, False.
    """
    # If call is a string, try to convert it to a dictionary.
    if isinstance(call, str):
        try:
            call = json.loads(call)
        except Exception as e:
            print(f"Error parsing call data: {e}")
            return False

    target_city = call.get("city")
    if not target_city:
        print("Error: Emergency call missing 'city' field.")
        return False

    ambulances_needed = call.get("needed") or call.get("required") or call.get("count")
    if ambulances_needed is None:
        if "requests" in call and isinstance(call["requests"], list):
            ambulances_needed = sum(req.get("Quantity", 0) for req in call["requests"])
        else:
            print(f"Error: Emergency call for {target_city} missing ambulance count: {call}")
            return False

    if not ambulances_needed or ambulances_needed <= 0:
        print(f"Error: Emergency call for {target_city} has non-positive ambulance count: {ambulances_needed}")
        return False

    print(f"\nEmergency call: {ambulances_needed} ambulance(s) required at {target_city}")

    if target_city not in location_details:
        print(f"Error: Target city '{target_city}' not found in location details.")
        return False

    target_coord = (location_details[target_city]["latitude"], location_details[target_city]["longitude"])

    candidates = []
    for city, count in available.items():
        if count > 0 and city in location_details:
            source_detail = location_details[city]
            source_coord = (source_detail["latitude"], source_detail["longitude"])
            distance = euclidean_distance(source_coord, target_coord)
            candidates.append((city, distance, count))

    candidates.sort(key=lambda candidate: candidate[1])

    remaining = ambulances_needed
    for city, distance, avail_count in candidates:
        if remaining <= 0:
            break
        dispatch_count = min(avail_count, remaining)
        dispatch_ambulances(city, target_city, dispatch_count, location_details)
        available[city] -= dispatch_count
        remaining -= dispatch_count

    if remaining > 0:
        print(f"Warning: Unable to dispatch {remaining} ambulance(s) for {target_city}!")
        return False

    print("Emergency resolved: all required ambulances dispatched successfully.")
    return True


def get_pending_calls():
    """
    Get all pending emergencies from the /calls/queue endpoint.
    If the call returns an empty response, return an empty list.
    """
    data = call_api("/calls/queue")
    if not data or (isinstance(data, str) and data.strip() == ""):
        return []
    return data


def request_next_call():
    """
    Request the next emergency call using the /calls/next endpoint.
    If the call returns an empty response, return an empty list.
    """
    data = call_api("/calls/next")
    if not data or (isinstance(data, str) and data.strip() == ""):
        return []
    return data


def main():
    print("Starting emergency service simulation...")

    # Make the reset call using POST with query parameters.
    reset_result = call_api("/control/reset", method="POST", params=SIMULATION_CONFIG)
    if reset_result is None:
        print("Failed to reset simulation.")
        return
    print("Simulation reset successful. Configuration:")
    print(SIMULATION_CONFIG)

    print("\nFetching locations and ambulance availability...")
    try:
        location_details = get_location_details()
    except Exception as e:
        print("Error getting location details:", e)
        return

    try:
        available = get_available_ambulances()
        print("Available ambulances:", available)
    except Exception as e:
        print("Error getting available ambulances:", e)
        return

    print(f"Retrieved {len(location_details)} locations and ambulance availability for {len(available)} cities.")

    total_calls_processed = 0
    # Counter for consecutive polls with no valid calls.
    no_call_counter = 0
    max_no_call_polls = 2  # adjust as needed

    # Continuous polling loop to process emergency calls until no new calls appear
    while True:
        pending_calls = get_pending_calls()
        if pending_calls:
            no_call_counter = 0  # Reset counter if we receive calls
            for call in pending_calls:
                # Skip if the call is an empty string.
                if isinstance(call, str) and call.strip() == "":
                    continue
                success = process_emergency(call, location_details, available)
                if success:
                    total_calls_processed += 1
                time.sleep(0.1)  # Brief pause to avoid flooding the API
                # Refresh available ambulance data after each call.
                available = get_available_ambulances()
        else:
            no_call_counter += 1
            print("Queue is empty, polling for emergency calls...")
            # Trigger a new call (if applicable)
            _ = request_next_call()
            time.sleep(1)

        # If we have polled many times with no new calls, assume simulation is finished.
        if no_call_counter >= max_no_call_polls:
            break

    print("\nAll emergencies processed. Stopping simulation...")
    stop_result = call_api("/control/stop", method="POST")
    print("\nSimulation finished. Final results:")
    print(stop_result)
    print(f"Total emergency calls processed: {total_calls_processed}")


if __name__ == '__main__':
    main()
