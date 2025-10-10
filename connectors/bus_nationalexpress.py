import io, zipfile, time, re
import requests
import pandas as pd
from ftfy import fix_text
from connectors.bus_flixbus import (
    _parse_time_to_sec, _sec_to_hhmm, _extract_city,
    _infer_country
)

def _get_with_retries(url, tries=3, timeout=120):
    err = None
    for i in range(tries):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.content
            err = f"HTTP {r.status_code}"
        except Exception as e:
            err = str(e)
        time.sleep(2)
    raise RuntimeError(f"Download failed for {url}: {err}")

def _parse_gtfs_zip(zip_bytes, feed_label):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def rd(name, usecols=None):
        try:
            df = pd.read_csv(z.open(name), dtype=str, usecols=usecols,
                             encoding="latin1", on_bad_lines="skip")
            for col in df.columns:
                df[col] = df[col].apply(lambda v: fix_text(v) if isinstance(v, str) else v)
            return df
        except KeyError:
            return pd.DataFrame()

    routes = rd("routes.txt", usecols=["route_id","route_type"])
    trips = rd("trips.txt", usecols=["route_id","trip_id","service_id"])
    stop_times = rd("stop_times.txt", usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    stops = rd("stops.txt", usecols=["stop_id","stop_name","stop_lat","stop_lon"])

    if routes.empty or trips.empty or stop_times.empty or stops.empty:
        return pd.DataFrame()

    routes = routes[routes["route_type"].astype(str) == "3"]
    trips = trips.merge(routes, on="route_id", how="inner")

    stop_times["stop_sequence"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce").fillna(0).astype(int)
    stop_times = stop_times.sort_values(["trip_id", "stop_sequence"])
    first = stop_times.groupby("trip_id").first().reset_index()[["trip_id","departure_time","stop_id"]]
    last = stop_times.groupby("trip_id").last().reset_index()[["trip_id","arrival_time","stop_id"]]
    first.columns = ["trip_id","t0","origin_stop"]
    last.columns = ["trip_id","t1","dest_stop"]

    merged = trips.merge(first, on="trip_id").merge(last, on="trip_id")
    merged = merged.merge(stops.rename(columns={"stop_id":"origin_stop","stop_name":"origin_station",
                                                "stop_lat":"origin_lat","stop_lon":"origin_lon"}), on="origin_stop", how="left")
    merged = merged.merge(stops.rename(columns={"stop_id":"dest_stop","stop_name":"destination_station",
                                                "stop_lat":"dest_lat","stop_lon":"dest_lon"}), on="dest_stop", how="left")

    merged["dur_sec"] = merged.apply(
        lambda r: (_parse_time_to_sec(r["t1"]) - _parse_time_to_sec(r["t0"]))
        if (_parse_time_to_sec(r["t1"]) and _parse_time_to_sec(r["t0"])) else None,
        axis=1
    )
    merged = merged[(merged["dur_sec"].notna()) & (merged["dur_sec"] > 0) & (merged["dur_sec"] < 48*3600)]

    merged["origin_city"] = merged["origin_station"].apply(_extract_city)
    merged["destination_city"] = merged["destination_station"].apply(_extract_city)
    merged["origin_country"] = merged.apply(lambda r: _infer_country(r["origin_lat"], r["origin_lon"]), axis=1)
    merged["destination_country"] = merged.apply(lambda r: _infer_country(r["dest_lat"], r["dest_lon"]), axis=1)

    freq = merged.groupby(["origin_city","destination_city"]).size().reset_index(name="trip_count")
    def freq_bucket(x):
        if x <= 5: return "Very Low"
        elif x <= 15: return "Low"
        elif x <= 25: return "Average"
        elif x <= 35: return "High"
        else: return "Very High"
    freq["frequency_bucket"] = freq["trip_count"].apply(freq_bucket)
    merged = merged.merge(freq, on=["origin_city","destination_city"], how="left")

    merged["duration"] = merged["dur_sec"].apply(_sec_to_hhmm)
    merged["operator_name"] = feed_label
    merged["transport_type"] = "bus"

    cols = [
        "origin_city","origin_country","origin_station",
        "destination_city","destination_country","destination_station",
        "operator_name","duration","trip_count","frequency_bucket"
    ]
    df = merged[cols].drop_duplicates(subset=["origin_city","destination_city"])
    return df

def fetch_routes():
    FEED_URL = "https://..."   # replace below per company
    LABEL = "Operator Name"
    print(f"Fetching {LABEL}...")
    try:
        content = _get_with_retries(FEED_URL)
        df = _parse_gtfs_zip(content, LABEL)
        print(f"  -> {len(df)} routes from {LABEL}")
        return df
    except Exception as e:
        print(f"Failed {LABEL}: {e}")
        return pd.DataFrame(columns=[
            "origin_city","origin_country","origin_station",
            "destination_city","destination_country","destination_station",
            "operator_name","duration","trip_count","frequency_bucket"
        ])
