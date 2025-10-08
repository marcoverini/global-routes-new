import io, zipfile, time, re
import requests
import pandas as pd

FEEDS = [
    "https://gtfs.gis.flix.tech/gtfs_generic_eu.zip",
    "https://gtfs.gis.flix.tech/gtfs_generic_us.zip",
    "http://gtfs.gis.flix.tech/gtfs_generic_us.zip",
]

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

def _parse_time_to_sec(t):
    if pd.isna(t): return None
    parts = str(t).split(":")
    if len(parts) == 2: parts.append("00")
    try: h, m, s = [int(x) for x in parts[:3]]
    except: return None
    return h*3600 + m*60 + s

def _sec_to_hhmm(s):
    if s is None or s < 0: return None
    h = s // 3600
    m = (s % 3600) // 60
    return f"{int(h):02d}:{int(m):02d}"

def _extract_city_country(name):
    """Extract city and country code if present, e.g. 'Berlin central bus station [DE]'"""
    if not isinstance(name, str):
        return None, None
    # Common suffix clean-up
    city = re.sub(r"\b(Bus( station| stop)?|Autostazione|ZOB|Gare routière|Terminal)\b.*", "", name, flags=re.I).strip()
    city = city.split(",")[0].strip()

    m = re.search(r"\[([A-Z]{2})\]", name)
    country = m.group(1) if m else None
    return city, country

def _parse_gtfs_zip(zip_bytes, feed_label="FlixBus"):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def rd(name, usecols=None):
        try:
            return pd.read_csv(z.open(name), dtype=str, usecols=usecols)
        except KeyError:
            return pd.DataFrame()

    routes = rd("routes.txt", usecols=["route_id","route_type"])
    trips = rd("trips.txt", usecols=["route_id","trip_id","service_id"])
    stop_times = rd("stop_times.txt", usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    stops = rd("stops.txt", usecols=["stop_id","stop_name"])

    if routes.empty or trips.empty or stop_times.empty or stops.empty:
        return pd.DataFrame()

    routes = routes[routes["route_type"].astype(str) == "3"]  # only buses
    trips = trips.merge(routes, on="route_id", how="inner")

    stop_times["stop_sequence"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce").fillna(0).astype(int)
    stop_times = stop_times.sort_values(["trip_id", "stop_sequence"])
    first = stop_times.groupby("trip_id").first().reset_index()[["trip_id","departure_time","stop_id"]]
    last = stop_times.groupby("trip_id").last().reset_index()[["trip_id","arrival_time","stop_id"]]
    first.columns = ["trip_id","t0","origin_stop"]
    last.columns = ["trip_id","t1","dest_stop"]

    merged = trips.merge(first, on="trip_id").merge(last, on="trip_id")
    merged = merged.merge(stops.rename(columns={"stop_id":"origin_stop","stop_name":"origin_station"}), on="origin_stop", how="left")
    merged = merged.merge(stops.rename(columns={"stop_id":"dest_stop","stop_name":"destination_station"}), on="dest_stop", how="left")

    # Duration
    merged["dur_sec"] = merged.apply(
        lambda r: (_parse_time_to_sec(r["t1"]) - _parse_time_to_sec(r["t0"]))
        if (_parse_time_to_sec(r["t1"]) and _parse_time_to_sec(r["t0"])) else None,
        axis=1
    )
    merged = merged[(merged["dur_sec"].notna()) & (merged["dur_sec"] > 0) & (merged["dur_sec"] < 48*3600)]

    # City & country
    merged[["origin_city","origin_country"]] = merged["origin_station"].apply(
        lambda x: pd.Series(_extract_city_country(x))
    )
    merged[["destination_city","destination_country"]] = merged["destination_station"].apply(
        lambda x: pd.Series(_extract_city_country(x))
    )

    # Frequency estimation
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
    print(f"Fetched {len(df)} routes from {feed_label}.")
    return df

def fetch_routes():
    print("Fetching FlixBus (GTFS) with city + country + frequency...")
    frames = []
    for url in FEEDS:
        try:
            print(f"Downloading {url}")
            content = _get_with_retries(url, tries=2, timeout=180)
            label = "FlixBus/US" if "us" in url else "FlixBus/EU"
            df = _parse_gtfs_zip(content, feed_label=label)
            if not df.empty:
                frames.append(df)
                print(f"  -> {len(df):,} rows from {label}")
        except Exception as e:
            print(f"  -> Skipped {url}: {e}")

    if not frames:
        return pd.DataFrame(columns=[
            "origin_city","origin_country","origin_station",
            "destination_city","destination_country","destination_station",
            "operator_name","duration","trip_count","frequency_bucket"
        ])

    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    print(f"Total FlixBus routes combined: {len(out):,}")
    return out
