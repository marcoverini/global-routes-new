import io, zipfile, time
import requests
import pandas as pd

# Stable public mirrors (EU + US). If HTTPS fails for US, it falls back to HTTP.
FEEDS = [
    "https://gtfs.gis.flix.tech/gtfs_generic_eu.zip",  # EU – linked from data.gouv.fr
    "https://gtfs.gis.flix.tech/gtfs_generic_us.zip",  # try HTTPS first
    "http://gtfs.gis.flix.tech/gtfs_generic_us.zip",   # fallback HTTP (Transitland shows this)
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
    # Accept HH:MM[:SS] with HH possibly > 24 (GTFS over-midnight)
    if pd.isna(t):
        return None
    parts = str(t).split(":")
    if len(parts) == 2:
        parts.append("00")
    try:
        h, m, s = [int(x) for x in parts[:3]]
    except:
        return None
    return h*3600 + m*60 + s

def _sec_to_hhmm(s):
    if s is None or s < 0:
        return None
    h = s // 3600
    m = (s % 3600) // 60
    return f"{int(h):02d}:{int(m):02d}"

def _parse_gtfs_zip(zip_bytes, feed_label="FlixBus"):
    z = zipfile.ZipFile(io.BytesIO(zip_bytes))

    # Read required tables (with minimal columns to save RAM)
    def rd(name, usecols=None):
        try:
            return pd.read_csv(z.open(name), dtype=str, usecols=usecols)
        except KeyError:
            return pd.DataFrame()

    agency = rd("agency.txt", usecols=["agency_id","agency_name"])
    routes = rd("routes.txt", usecols=["route_id","route_short_name","route_long_name","agency_id","route_type"])
    trips  = rd("trips.txt",  usecols=["route_id","trip_id","service_id"])
    st     = rd("stop_times.txt", usecols=["trip_id","arrival_time","departure_time","stop_id","stop_sequence"])
    stops  = rd("stops.txt",  usecols=["stop_id","stop_name"])

    if routes.empty or trips.empty or st.empty or stops.empty:
        return pd.DataFrame(columns=[
            "origin_station","destination_station","operator_name","duration","frequency","frequency_bucket"
        ])

    # Route type 3 is bus per GTFS
    if "route_type" in routes.columns:
        routes = routes[routes["route_type"].astype(str) == "3"]
        if routes.empty:
            return pd.DataFrame(columns=[
                "origin_station","destination_station","operator_name","duration","frequency","frequency_bucket"
            ])

    # First/last stops per trip
    st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce").fillna(0).astype(int)
    st = st.sort_values(["trip_id","stop_sequence"])
    first = st.groupby("trip_id").first(numeric_only=False).reset_index()[["trip_id","departure_time","stop_id"]]
    last  = st.groupby("trip_id").last(numeric_only=False).reset_index()[["trip_id","arrival_time","stop_id"]]
    first.columns = ["trip_id","t0","origin_stop"]
    last.columns  = ["trip_id","t1","dest_stop"]

    merged = (trips
              .merge(routes, on="route_id", how="inner")
              .merge(first,  on="trip_id", how="inner")
              .merge(last,   on="trip_id", how="inner"))

    # Attach operator from agency if available
    op = None
    if not agency.empty and "agency_id" in routes.columns:
        merged = merged.merge(agency, on="agency_id", how="left")
        op = "agency_name"
    # Fallback operator label
    if op is None:
        merged["agency_name"] = feed_label
        op = "agency_name"

    # Join stop names
    stops_slim = stops.rename(columns={"stop_id":"origin_stop","stop_name":"origin_station"})
    merged = merged.merge(stops_slim[["origin_stop","origin_station"]], on="origin_stop", how="left")
    stops_slim2 = stops.rename(columns={"stop_id":"dest_stop","stop_name":"destination_station"})
    merged = merged.merge(stops_slim2[["dest_stop","destination_station"]], on="dest_stop", how="left")

    # Compute durations
    merged["dur_sec"] = merged.apply(
        lambda r: (_parse_time_to_sec(r["t1"]) - _parse_time_to_sec(r["t0"]))
                  if (_parse_time_to_sec(r["t1"]) is not None and _parse_time_to_sec(r["t0"]) is not None) else None,
        axis=1
    )
    merged = merged[(merged["dur_sec"].notna()) & (merged["dur_sec"] > 0) & (merged["dur_sec"] < 48*3600)]
    if merged.empty:
        return pd.DataFrame(columns=[
            "origin_station","destination_station","operator_name","duration","frequency","frequency_bucket"
        ])

    # Collapse to OD with median duration; keep one operator name (agency_name)
    agg = (merged.groupby(["origin_station","destination_station", op], as_index=False)["dur_sec"]
                 .median()
                 .rename(columns={"dur_sec":"_dur_sec", op:"operator_name"}))
    agg["duration"] = agg["_dur_sec"].apply(_sec_to_hhmm)
    agg["frequency"] = "Unknown"
    agg["frequency_bucket"] = "Unknown"

    cols = ["origin_station","destination_station","operator_name","duration","frequency","frequency_bucket"]
    return agg[cols].dropna(subset=["origin_station","destination_station"])

def fetch_routes():
    print("Fetching FlixBus (GTFS) from EU + US mirrors…")
    frames = []
    for url in FEEDS:
        try:
            print(f"  - downloading {url}")
            content = _get_with_retries(url, tries=2, timeout=180)
            df = _parse_gtfs_zip(content, feed_label=("FlixBus/US" if "us" in url else "FlixBus/EU"))
            if not df.empty:
                frames.append(df)
                print(f"    -> {len(df):,} OD pairs")
        except Exception as e:
            print(f"    -> skipped: {e}")

    if not frames:
        # Return empty DataFrame with expected columns so the build doesn't crash
        return pd.DataFrame(columns=[
            "origin_station","destination_station","operator_name","duration","frequency","frequency_bucket"
        ])

    out = pd.concat(frames, ignore_index=True).drop_duplicates()
    print(f"Total FlixBus OD pairs: {len(out):,}")
    return out
