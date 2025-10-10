import io, zipfile, time, re
import requests
import pandas as pd
from ftfy import fix_text  # <--- ensures perfect accent/character repair

FEEDS = [
    "https://gtfs.gis.flix.tech/gtfs_generic_eu.zip",
    "https://gtfs.gis.flix.tech/gtfs_generic_us.zip",
]

# ---- SIMPLE GEO COUNTRY BOUNDS ----
COUNTRY_BOUNDS = {
    "ES": [(36, -9.5), (43.8, 3.3)],
    "FR": [(41, -5.2), (51.3, 9.7)],
    "IT": [(36.4, 6.6), (47.1, 18.5)],
    "DE": [(47.0, 5.5), (55.2, 15.2)],
    "CH": [(45.7, 5.9), (47.9, 10.5)],
    "AT": [(46.3, 9.3), (49.1, 17.0)],
    "BE": [(49.5, 2.5), (51.6, 6.4)],
    "NL": [(50.6, 3.3), (53.7, 7.2)],
    "GB": [(49.9, -8.6), (59.4, 2.0)],
    "PT": [(36.9, -9.5), (42.2, -6.2)],
    "PL": [(49.0, 14.1), (54.9, 24.2)],
    "CZ": [(48.5, 12.1), (51.1, 18.9)],
    "US": [(24.5, -125.0), (49.4, -66.9)],
    "SE": [(55.0, 11.0), (69.0, 24.0)],
    "NO": [(58.0, 4.0), (71.2, 31.2)],
    "DK": [(54.5, 8.0), (57.8, 13.0)],
    "HU": [(45.7, 16.0), (48.7, 23.0)],
    "HR": [(42.0, 13.3), (46.7, 19.5)],
}

def _infer_country(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except:
        return None
    for code, ((minlat, minlon), (maxlat, maxlon)) in COUNTRY_BOUNDS.items():
        if minlat <= lat <= maxlat and minlon <= lon <= maxlon:
            return code
    return None

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

def _extract_city(name):
    """Cleaner city extraction from station name"""
    if not isinstance(name, str) or not name.strip():
        return None
    name = re.sub(r"\b(Bus( station| stop)?|Autostazione|ZOB|Gare routière|Terminal)\b", "", name, flags=re.I)
    name = re.sub(r"\s+", " ", name).strip(" ,;:-")
    name = re.sub(r"( central| station| Hbf| main)$", "", name, flags=re.I).strip(" ,;:-")
    if name.count("(") > name.count(")"):
        name += ")"
    return name.strip()

def _parse_gtfs_zip(zip_bytes, feed_label="FlixBus"):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    def rd(name, usecols=None):
        try:
            df = pd.read_csv(
                z.open(name),
                dtype=str,
                usecols=usecols,
                encoding="latin1",
                on_bad_lines="skip"
            )

            # --- Fix mixed encodings ---
            def _fix_encoding(val):
                if not isinstance(val, str):
                    return val
                try:
                    val = val.encode("latin1").decode("utf-8")
                except Exception:
                    pass
                return fix_text(val)

            for col in df.columns:
                df[col] = df[col].apply(_fix_encoding)
            return df
        except KeyError:
            return pd.DataFrame()

    routes = rd("routes.txt", usecols=["route_id","route_type"])
    trips = rd("trips.txt", usecols=["route_id","trip_id","service_id"])
    stop_times = rd("stop_times.txt", usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    stops = rd("stops.txt", usecols=["stop_id","stop_name","stop_lat","stop_lon"])

    if routes.empty or trips.empty or stop_times.empty or stops.empty:
        print("One of the GTFS files is empty — skipping feed.")
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
    merged = merged.merge(stops.rename(columns={"stop_id":"origin_stop","stop_name":"origin_station","stop_lat":"origin_lat","stop_lon":"origin_lon"}), on="origin_stop", how="left")
    merged = merged.merge(stops.rename(columns={"stop_id":"dest_stop","stop_name":"destination_station","stop_lat":"dest_lat","stop_lon":"dest_lon"}), on="dest_stop", how="left")

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
    print(f"Fetched {len(df)} routes from {feed_label}.")
    return df

def fetch_routes():
    print("Fetching FlixBus GTFS feeds with ftfy text repair...")
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
