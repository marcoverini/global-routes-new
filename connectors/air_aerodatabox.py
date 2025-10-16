# connectors/air_aerodatabox.py
import os
import requests
import pandas as pd
import time

API_KEY = os.getenv("AERODATABOX_API_KEY", "YOUR_API_KEY_HERE")
API_HOST = os.getenv("AERODATABOX_API_HOST", "aerodatabox.p.rapidapi.com")
BASE_URL = f"https://{API_HOST}"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

def fetch_routes():
    """
    Pulls direct flight routes (origin–destination) from AeroDataBox airport endpoints.
    """
    print("Connecting to AeroDataBox API...")

    # A representative sample of key airports (expand later)
    airports = [
        "LHR", "LGW", "CDG", "ORY", "FRA", "MUC", "AMS", "MAD", "BCN",
        "DUB", "MXP", "FCO", "ZRH", "VIE", "LIS", "IST", "ATH",
        "JFK", "LAX", "ORD", "ATL", "DFW", "YYZ", "YUL", "YVR"
    ]

    all_routes = []

    for origin in airports:
        try:
            print(f"✈ Fetching routes from {origin} ...")
            url = f"{BASE_URL}/airports/{origin}/routes"
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                print(f"⚠️  {origin}: {r.status_code} {r.text[:120]}")
                continue

            data = r.json()

            destinations = data.get("routes", [])
            for dest in destinations:
                dest_code = dest.get("arrival", {}).get("iata", "")
                dest_city = dest.get("arrival", {}).get("municipalityName", "")
                dest_country = dest.get("arrival", {}).get("countryName", "")
                dest_name = dest.get("arrival", {}).get("name", "")

                origin_city = data.get("airport", {}).get("municipalityName", "")
                origin_country = data.get("airport", {}).get("countryName", "")
                origin_name = data.get("airport", {}).get("name", "")

                if dest_code:
                    all_routes.append({
                        "transport_type": "air",
                        "operator_name": None,
                        "origin_city": origin_city,
                        "origin_country": origin_country,
                        "origin_station": origin_name,
                        "destination_city": dest_city,
                        "destination_country": dest_country,
                        "destination_station": dest_name,
                        "duration": None,
                        "frequency_daily": None,
                        "frequency_label": None
                    })

            time.sleep(1)  # gentle delay for free-tier rate limits

        except Exception as e:
            print(f"❌ Error fetching {origin}: {e}")

    df = pd.DataFrame(all_routes).drop_duplicates()
    print(f"✅ Fetched {len(df)} unique routes from AeroDataBox.")
    return df
