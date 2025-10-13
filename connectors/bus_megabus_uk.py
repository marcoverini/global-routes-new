# connectors/bus_megabus_uk.py
import requests, time
import pandas as pd

BASE = "https://uk.megabus.com"
LOC = f"{BASE}/journey-planner/api/locations"
JNY = f"{BASE}/journey-planner/api/journeys"

def _hhmm_from_minutes(m):
    try:
        m = int(m)
        h, r = divmod(m, 60)
        return f"{h:02d}:{r:02d}"
    except Exception:
        return None

def fetch_routes():
    print("Fetching Megabus UK routes (live API)...")
    # 1) locations
    r = requests.get(LOC, timeout=60)
    r.raise_for_status()
    locs = r.json()  # list of dicts: {id, name, country}
    loc_df = pd.DataFrame(locs)
    if loc_df.empty:
        return pd.DataFrame()

    # Derive cities from stop names (strip common suffixes)
    def city_from(name: str):
        if not isinstance(name, str):
            return None
        x = name.replace("Coach Station", "").replace("Bus Station", "").strip()
        x = x.split("(")[0].strip()
        return x

    loc_df["city"] = loc_df["name"].map(city_from)
    loc_df["country"] = loc_df["country"].fillna("United Kingdom")

    # 2) sample OD pairs to stay under rate limits
    ids = loc_df["id"].tolist()
    ids = ids[:60]  # cap to first 60 locations to be polite
    pairs = []
    results = []

    for i, o in enumerate(ids):
        for d in ids[i+1:]:
            payload = {"originId": int(o), "destinationId": int(d), "departureDate": pd.Timestamp.today().strftime("%Y-%m-%d")}
            try:
                rr = requests.post(JNY, json=payload, timeout=35)
                if rr.status_code != 200:
                    continue
                data = rr.json()
                journeys = data.get("journeys") or []
                if not journeys:
                    continue
                # Take first journey as representative
                j0 = journeys[0]
                dur_min = j0.get("durationInMinutes")
                if dur_min is None:
                    continue

                orec = loc_df.loc[loc_df["id"] == o].iloc[0]
                drec = loc_df.loc[loc_df["id"] == d].iloc[0]

                results.append({
                    "transport_type": "bus",
                    "operator_name": "Megabus UK",
                    "duration": _hhmm_from_minutes(dur_min),
                    "frequency_daily": max(1, len(journeys)),  # crude lower bound
                    "frequency_label": None,  # build_monthly will fill if needed
                    "origin_station": orec["name"],
                    "destination_station": drec["name"],
                    "origin_city": orec["city"],
                    "destination_city": drec["city"],
                    "origin_country": orec["country"],
                    "destination_country": drec["country"],
                })
            except Exception:
                pass
            time.sleep(0.4)  # gentle rate limit

    df = pd.DataFrame(results)
    print(f"✅ Megabus UK: {len(df)} live routes")
    return df
