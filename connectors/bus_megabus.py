# connectors/bus_megabus.py
import requests
import pandas as pd
from time import sleep
from connectors.bus_flixbus import _sec_to_hhmm, _infer_country

BASE_URL = "https://uk.megabus.com/journey-planner/api"
OPERATOR_NAME = "Megabus"
TRANSPORT_TYPE = "bus"

def fetch_routes():
    print("Fetching Megabus routes via public API...")

    # Get all available locations (cities)
    loc_url = f"{BASE_URL}/locations"
    try:
        res = requests.get(loc_url, timeout=60)
        res.raise_for_status()
        locations = res.json()
    except Exception as e:
        raise RuntimeError(f"Could not fetch Megabus locations: {e}")

    locs = pd.DataFrame(locations)
    locs = locs[["id", "name", "country"]].rename(
        columns={"id": "city_id", "name": "city_name"}
    )

    routes = []
    count = 0

    # We'll test pairs (to avoid hitting API limits, we sample every 3rd destination)
    for i, row_o in locs.iterrows():
        for j, row_d in locs.iterrows():
            if row_o.city_id == row_d.city_id:
                continue
            if j % 3 != 0:
                continue  # reduce API load

            payload = {
                "originId": int(row_o.city_id),
                "destinationId": int(row_d.city_id),
                "departureDate": "2025-10-12"
            }

            try:
                r = requests.post(f"{BASE_URL}/journeys", json=payload, timeout=30)
                if r.status_code != 200:
                    continue
                data = r.json()
                if not data or "journeys" not in data:
                    continue

                for jn in data["journeys"]:
                    dur_min = jn.get("durationInMinutes")
                    if dur_min is None:
                        continue

                    routes.append({
                        "transport_type": TRANSPORT_TYPE,
                        "operator_name": OPERATOR_NAME,
                        "duration": _sec_to_hhmm(dur_min * 60),
                        "frequency_daily": 1,  # each unique journey counts as 1 service
                        "frequency_label": "Low (6-15)" if dur_min < 600 else "Very Low (0-5)",
                        "origin_station": row_o.city_name,
                        "destination_station": row_d.city_name,
                        "origin_city": row_o.city_name,
                        "destination_city": row_d.city_name,
                        "origin_country": row_o.country,
                        "destination_country": row_d.country,
                    })
                    count += 1

            except Exception as e:
                print(f"⚠️ API error between {row_o.city_name} and {row_d.city_name}: {e}")
                continue

            # Gentle rate limit (Megabus API blocks bursts)
            sleep(0.5)

    if not routes:
        print("⚠️ No routes fetched from Megabus API.")
        return pd.DataFrame()

    df = pd.DataFrame(routes)
    print(f"✅ Retrieved {len(df):,} Megabus routes from API")
    return df
