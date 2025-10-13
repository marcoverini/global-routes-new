# connectors/bus_megabus_na.py
import requests, time
import pandas as pd

BASES = ["https://us.megabus.com", "https://ca.megabus.com"]
LOC_SUFFIX = "/journey-planner/api/locations"
JNY_SUFFIX = "/journey-planner/api/journeys"

def _hhmm_from_minutes(m):
    try:
        m = int(m)
        h, r = divmod(m, 60)
        return f"{h:02d}:{r:02d}"
    except Exception:
        return None

def _fetch_locations():
    frames = []
    for base in BASES:
        try:
            r = requests.get(base + LOC_SUFFIX, timeout=60)
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            if not df.empty:
                df["source_base"] = base
                frames.append(df)
        except Exception:
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def fetch_routes():
    print("Fetching Megabus North America routes (live API)…")
    loc_df = _fetch_locations()
    if loc_df.empty:
        print("⚠️ No NA locations returned")
        return pd.DataFrame()

    # Normalize
    def city_from(name: str):
        if not isinstance(name, str):
            return None
        x = name.replace("Coach Station", "").replace("Bus Station", "").strip()
        x = x.split("(")[0].strip()
        return x

    loc_df["city"] = loc_df["name"].map(city_from)
    loc_df["country"] = loc_df["country"].fillna(
        loc_df["source_base"].map(lambda b: "United States" if "us." in b else "Canada")
    )

    # modest sampling to avoid bans
    ids = loc_df["id"].tolist()
    ids = ids[:80]

    results = []
    for i, o in enumerate(ids):
        for d in ids[i+1:]:
            orec = loc_df.loc[loc_df["id"] == o].iloc[0]
            drec = loc_df.loc[loc_df["id"] == d].iloc[0]
            base = orec["source_base"]  # use the origin’s domain

            payload = {"originId": int(o), "destinationId": int(d), "departureDate": pd.Timestamp.today().strftime("%Y-%m-%d")}
            try:
                rr = requests.post(base + JNY_SUFFIX, json=payload, timeout=35)
                if rr.status_code != 200:
                    continue
                data = rr.json()
                journeys = data.get("journeys") or []
                if not journeys:
                    continue
                j0 = journeys[0]
                dur_min = j0.get("durationInMinutes")
                if dur_min is None:
                    continue

                results.append({
                    "transport_type": "bus",
                    "operator_name": "Megabus North America",
                    "duration": _hhmm_from_minutes(dur_min),
                    "frequency_daily": max(1, len(journeys)),
                    "frequency_label": None,
                    "origin_station": orec["name"],
                    "destination_station": drec["name"],
                    "origin_city": orec["city"],
                    "destination_city": drec["city"],
                    "origin_country": orec["country"],
                    "destination_country": drec["country"],
                })
            except Exception:
                pass
            time.sleep(0.4)

    df = pd.DataFrame(results)
    print(f"✅ Megabus North America: {len(df)} live routes")
    return df
