# connectors/air_aerodatabox.py
import os
import requests
import pandas as pd

API_KEY = os.getenv("AERODATABOX_API_KEY", "YOUR_API_KEY_HERE")
API_HOST = os.getenv("AERODATABOX_API_HOST", "aerodatabox.p.rapidapi.com")
BASE_URL = f"https://{API_HOST}"

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST
}

def fetch_routes():
    """
    Fetches a simplified list of direct air routes (origin-destination pairs)
    from AeroDataBox’s open “routes by airline” endpoints.
    Only includes unique city pairs — no timetable data.
    """

    print("Connecting to AeroDataBox API...")

    # --- Example airline sample set (expand later) ---
    airlines = [
        "BAW",  # British Airways
        "AFR",  # Air France
        "DLH",  # Lufthansa
        "UAE",  # Emirates
        "AAL",  # American Airlines
        "RYR",  # Ryanair
        "EZY",  # easyJet
        "IBE",  # Iberia
        "KLM",  # KLM Royal Dutch Airlines
        "TAP",  # TAP Air Portugal
    ]

    all_routes = []

    for code in airlines:
        try:
            print(f"✈ Fetching routes for {code} ...")
            url = f"{BASE_URL}/airlines/{code}/routes"
            r = requests.get(url, headers=HEADERS, timeout=60)
            if r.status_code != 200:
                print(f"⚠️  {code}: {r.status_code} {r.text[:100]}")
                continue

            data = r.json()

            for item in data.get("routes", []):
                origin = item.get("departure", {}).get("iata", "")
                destination = item.get("arrival", {}).get("iata", "")
                if not origin or not destination:
                    continue

                all_routes.append({
                    "transport_type": "air",
                    "operator_name": code,
                    "origin_city": item.get("departure", {}).get("municipalityName", ""),
                    "origin_country": item.get("departure", {}).get("countryName", ""),
                    "origin_station": item.get("departure", {}).get("name", ""),
                    "destination_city": item.get("arrival", {}).get("municipalityName", ""),
                    "destination_country": item.get("arrival", {}).get("countryName", ""),
                    "destination_station": item.get("arrival", {}).get("name", ""),
                    "duration": None,
                    "frequency_daily": None,
                    "frequency_label": None,
                })

        except Exception as e:
            print(f"❌ Failed to fetch {code}: {e}")

    df = pd.DataFrame(all_routes)
    print(f"✅ Fetched {len(df)} routes from AeroDataBox.")
    return df

