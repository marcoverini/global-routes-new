import pandas as pd
import zipfile
import io
import requests

GTFS_URL = "https://gtfs.global.flixbus.com/flixbus.zip"

def fetch_routes():
    print("Fetching FlixBus GTFS feed...")

    # Download GTFS zip
    r = requests.get(GTFS_URL, timeout=120)
    r.raise_for_status()

    z = zipfile.ZipFile(io.BytesIO(r.content))

    stops = pd.read_csv(z.open("stops.txt"))
    routes = pd.read_csv(z.open("routes.txt"))
    trips = pd.read_csv(z.open("trips.txt"))
    stop_times = pd.read_csv(z.open("stop_times.txt"))

    # Join to get start and end stops for each trip
    first_stops = stop_times.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").first().reset_index()
    last_stops = stop_times.sort_values(["trip_id", "stop_sequence"]).groupby("trip_id").last().reset_index()

    merged = trips.merge(routes, on="route_id", how="left")
    merged = merged.merge(first_stops[["trip_id", "stop_id"]].rename(columns={"stop_id": "origin_stop_id"}), on="trip_id", how="left")
    merged = merged.merge(last_stops[["trip_id", "stop_id"]].rename(columns={"stop_id": "dest_stop_id"}), on="trip_id", how="left")

    merged = merged.merge(stops[["stop_id", "stop_name"]].rename(columns={"stop_id": "origin_stop_id", "stop_name": "origin_stop_name"}), on="origin_stop_id", how="left")
    merged = merged.merge(stops[["stop_id", "stop_name"]].rename(columns={"stop_id": "dest_stop_id", "stop_name": "dest_stop_name"}), on="dest_stop_id", how="left")

    merged = merged.dropna(subset=["origin_stop_name", "dest_stop_name"]).drop_duplicates(subset=["origin_stop_name", "dest_stop_name"])

    df = merged[["origin_stop_name", "dest_stop_name", "route_long_name"]].copy()
    df = df.rename(columns={
        "origin_stop_name": "origin_city",
        "dest_stop_name": "destination_city",
        "route_long_name": "operator_name"
    })
    df["transport_type"] = "bus"
    df["duration"] = None
    df["frequency"] = None
    df["frequency_bucket"] = None

    print(f"Fetched {len(df)} direct FlixBus routes.")
    return df
