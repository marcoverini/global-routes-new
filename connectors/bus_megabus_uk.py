# connectors/bus_megabus_uk.py
import zipfile, io, os, requests, pandas as pd
from ftfy import fix_text

# Public Megabus UK GTFS feed (via MobilityData cloud)
GTFS_URL = "https://storage.googleapis.com/mdb-csv/gtfs/megabus-uk.zip"

def _hhmm_from_minutes(m):
    try:
        m = int(m)
        h, r = divmod(m, 60)
        return f"{h:02d}:{r:02d}"
    except Exception:
        return None

def fetch_routes():
    print("Fetching Megabus UK GTFS feed (official)...")
    try:
        r = requests.get(GTFS_URL, timeout=90)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ Failed to download Megabus UK GTFS: {e}")
        return pd.DataFrame()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        files = z.namelist()
        needed = {"stops.txt", "trips.txt", "stop_times.txt"}
        if not needed.issubset(set(files)):
            print(f"⚠️ Missing required GTFS files in Megabus UK feed ({files})")
            return pd.DataFrame()

        stops = pd.read_csv(z.open("stops.txt"))
        trips = pd.read_csv(z.open("trips.txt"))
        stop_times = pd.read_csv(z.open("stop_times.txt"))

    # Fix encoding and normalize names
    stops["stop_name"] = stops["stop_name"].astype(str).map(fix_text)

    # Find first and last stops for each trip
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

    # Derive city names
    def city_from_station(s):
        if not isinstance(s, str):
            return None
        s = s.replace("Station", "").replace("Stop", "").replace("Coach Station", "")
        s = s.replace("Megabus Stop", "").replace("FlixBus Stop", "")
        s = s.split("(")[0].split("-")[0]
        return s.strip()

    df["origin_city"] = df["origin_station"].map(city_from_station)
    df["destination_city"] = df["destination_station"].map(city_from_station)
    df["origin_country"] = "United Kingdom"
    df["destination_country"] = "United Kingdom"

    # Add metadata
    df["operator_name"] = "Megabus UK"
    df["transport_type"] = "bus"
    df["duration"] = df["duration"].map(_hhmm_from_minutes)
    df["frequency_daily"] = 1
    df["frequency_label"] = "Unknown"

    final = df[
        ["origin_city","origin_country","origin_station",
         "destination_city","destination_country","destination_station",
         "operator_name","duration","frequency_daily","frequency_label","transport_type"]
    ]

    print(f"✅ Megabus UK: {len(final)} routes extracted")
    return final
