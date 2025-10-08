import requests
import pandas as pd

# --- FlixBus public API endpoints ---
API_URL = "https://global.api.flixbus.com/search/service/v4/search"
LOCATIONS_URL = "https://global.api.flixbus.com/locations?country=all&locale=en"

def fetch_routes():
    print("Fetching FlixBus routes...")

    # Get all available stops (cities)
    resp = requests.get(LOCATIONS_URL, timeout=60)
    data = resp.json()

    # Sometimes API returns {"data": [...]}, sometimes a list
    if isinstance(data, dict) and "data" in data:
        stops = data["data"]
    elif isinstance(data, list):
        stops = data
    else:
        raise ValueError("Unexpected FlixBus API format for /locations")

    # Normalize structure
    city_list = []
    for s in stops:
        if isinstance(s, dict):
            city_list.append({
                "id": s.get("id"),
                "name": s.get("name"),
                "country": {"name": s.get("country_name") or s.get("country") or "Unknown"}
            })

    rows = []
    for origin in city_list[:300]:  # limit for speed (adjust later)
        origin_id = origin["id"]
        origin_city = origin["name"]
        origin_country = origin["country"]["name"]

        for dest in city_list:
            if dest["id"] == origin_id:
                continue
            dest_city = dest["name"]
            dest_country = dest["country"]["name"]

            params = {
                "from_id": origin_id,
                "to_id": dest["id"],
                "adult": 1,
                "currency": "EUR",
                "locale": "en",
                "products": "bus"
            }
            try:
                r = requests.get(API_URL, params=params, timeout=10)
                if r.status_code != 200:
                    continue
                data = r.json()
                trips = data.get("trips", [])
                if not trips:
                    continue

                # Take the first trip for that pair
                trip = trips[0]
                duration_minutes = trip.get("duration", 0)
                h, m = divmod(duration_minutes, 60)
                duration_str = f"{h:02d}:{m:02d}"

                # Frequency bucket (approximation based on trip count)
                freq = len(trips)
                if freq <= 5:
                    freq_bucket = "Very Low"
                elif freq <= 15:
                    freq_bucket = "Low"
                elif freq <= 25:
                    freq_bucket = "Average"
                elif freq <= 35:
                    freq_bucket = "High"
                else:
                    freq_bucket = "Very High"

                rows.append([
                    origin_city, origin_country,
                    dest_city, dest_country,
                    "FlixBus",
                    duration_str,
                    freq,
                    freq_bucket
                ])
            except Exception:
                continue

    df = pd.DataFrame(rows, columns=[
        "origin_city","origin_country",
        "destination_city","destination_country",
        "operator_name","duration","frequency","frequency_bucket"
    ])
    print(f"Fetched {len(df)} routes from FlixBus.")
    return df
