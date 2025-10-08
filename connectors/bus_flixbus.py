import requests
import pandas as pd
from datetime import timedelta

# --- FlixBus public API endpoint ---
API_URL = "https://global.api.flixbus.com/search/service/v4/search"

def fetch_routes():
    print("Fetching FlixBus routes...")

    # Get all available stops (cities)
    stops_url = "https://global.api.flixbus.com/locations?country=all&locale=en"
    stops = requests.get(stops_url, timeout=60).json()
    city_list = [s for s in stops if s.get("country") and s.get("name")]

    rows = []
    for origin in city_list[:300]:  # limit for speed; can increase later
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
import requests
import pandas as pd
from datetime import timedelta

# --- FlixBus public API endpoint ---
API_URL = "https://global.api.flixbus.com/search/service/v4/search"

def fetch_routes():
    print("Fetching FlixBus routes...")

    # Get all available stops (cities)
    stops_url = "https://global.api.flixbus.com/locations?country=all&locale=en"
    stops = requests.get(stops_url, timeout=60).json()
    city_list = [s for s in stops if s.get("country") and s.get("name")]

    rows = []
    for origin in city_list[:300]:  # limit for speed; can increase later
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
