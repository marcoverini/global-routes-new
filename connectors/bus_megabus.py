import requests
import pandas as pd
import time

def fetch_routes():
    print("Fetching Megabus UK/Europe routes...")

    # Step 1: Fetch list of all Megabus stops
    LOC_URL = "https://uk.megabus.com/journey-planner/api/locations"
    r = requests.get(LOC_URL, timeout=60)
    r.raise_for_status()
    stops = r.json()

    # Step 2: Build mapping {id: name, country, city}
    cities = {}
    for s in stops:
        name = s["name"]
        city = name.split("(")[0].replace("Coach Station", "").replace("Bus Station", "").strip()
        country = s.get("country", "United Kingdom")
        cities[s["id"]] = {
            "id": s["id"],
            "name": name,
            "city": city,
            "country": country
        }

    # Step 3: Query some intercity pairs (we’ll limit to top 40 to stay under rate limits)
    pairs = []
    city_ids = list(cities.keys())[:40]

    for i, o_id in enumerate(city_ids):
        for d_id in city_ids[i+1:]:
            url = f"https://uk.megabus.com/journey-planner/api/journeys?originId={o_id}&destinationId={d_id}&outboundDate=2025-10-15"
            try:
                r = requests.get(url, timeout=20)
                if r.status_code == 200 and r.json().get("journeys"):
                    data = r.json()["journeys"][0]
                    origin = cities[o_id]
                    dest = cities[d_id]

                    duration = data.get("duration", "")
                    freq_label = "Unknown"
                    freq_daily = "Unknown"

                    pairs.append({
                        "transport_type": "bus",
                        "operator_name": "Megabus",
                        "duration": duration,
                        "frequency_daily": freq_daily,
                        "frequency_label": freq_label,
                        "origin_station": origin["name"],
                        "destination_station": dest["name"],
                        "origin_city": origin["city"],
                        "destination_city": dest["city"],
                        "origin_country": origin["country"],
                        "destination_country": dest["country"],
                    })

                time.sleep(0.5)  # avoid rate limit
            except Exception as e:
                print("Error:", e)
                continue

    # Step 4: Build DataFrame
    df = pd.DataFrame(pairs)
    print(f"✅ Found {len(df)} Megabus routes.")
    return df
