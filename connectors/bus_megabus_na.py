# connectors/bus_megabus_na.py
import zipfile, io, os, requests, pandas as pd
from ftfy import fix_text

# GTFS feed URLs (hosted by MobilityData)
GTFS_FEEDS = {
    "Megabus US": "https://storage.googleapis.com/mdb-csv/gtfs/megabus-us.zip",
    "Megabus Canada": "https://storage.googleapis.com/mdb-csv/gtfs/megabus-canada.zip",
}

def _hhmm_from_minutes(m):
    try:
        m = int(m)
        h, r = divmod(m, 60)
        return f"{h:02d}:{r:02d}"
    except Exception:
        return None

def fetch_routes():
    print("Fetching Megabus North America GTFS feeds (US + Canada)...")
    frames = []

    for name, url in GTFS_FEEDS.items():
        print(f"→ Downloading {name} feed from {url}")
        try:
            r = requests.get(url, timeout=90)
            r.raise_for_status()
        except Exception as e:
            print(f"❌ Failed to fetch {name}: {e}")
            continue

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            if "stops.txt" not in z.namelist() or "trips.txt" not in z.namelist() or "stop_times.txt" not in z.namelist():
                print(f"⚠️ Missing GTFS tables in {name}")
                continue

            stops = pd.read_csv(z.open("stops.txt"))
            trips = pd.read_csv(z.open("trips.txt"))
            stop_times = pd.read_csv(z.open("stop_times.txt"))

        # Fix text encoding issues
        stops["stop_name"] = stops["stop_name"].astype(str).map(fix_text)

        # Merge minimal fields
        first_last = stop_times.groupby("trip_id").agg(
            first_stop=("stop_id", "first"),
            last_stop=("stop_id", "last"),
            duration=("arrival_time", lambda x: len(x))
        ).reset_index()

        df = first_last.merge(trips[["trip_id", "route_id"]], on="trip_id", how="left")
        df = df.merge(
            stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]],
            left_on="first_stop", right_on="stop_id", how="left"
        ).rename(columns={
            "stop_name": "origin_station",
            "stop_lat": "origin_lat",
            "stop_lon": "origin_lon"
        }).drop(columns=["stop_id"])

        df = df.merge(
            stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]],
            left_on="last_stop", right_on="stop_id", how="left"
        ).rename(columns={
            "stop_name": "destination_station",
            "stop_lat": "destination_lat",
            "stop_lon": "destination_lon"
        }).drop(columns=["stop_id"])

        # Derive city names (basic cleanup)
        def city_from_station(s):
            if not isinstance(s, str):
                return None
            s = s.replace("Station", "").replace("Stop", "").replace("Megabus Stop", "")
            s = s.replace("FlixBus Stop", "").split("(")[0].split("-")[0]
            return s.strip()

        df["origin_city"] = df["origin_station"].map(city_from_station)
        df["destination_city"] = df["destination_station"].map(city_from_station)
        df["origin_country"] = "United States" if "US" in name else "Canada"
        df["destination_country"] = "United States" if "US" in name else "Canada"

        # Add metadata
        df["operator_name"] = name
        df["transport_type"] = "bus"
        df["duration"] = df["duration"].map(_hhmm_from_minutes)
        df["frequency_daily"] = 1
        df["frequency_label"] = "Unknown"

        frames.append(df[
            ["origin_city","origin_country","origin_station",
             "destination_city","destination_country","destination_station",
             "operator_name","duration","frequency_daily","frequency_label","transport_type"]
        ])

        print(f"✅ {name}: {len(df)} routes extracted")

    if not frames:
        print("⚠️ No Megabus North America feeds loaded")
        return pd.DataFrame()

    all_df = pd.concat(frames, ignore_index=True)
    print(f"✅ Megabus North America combined: {len(all_df)} routes total")
    return all_df
    
