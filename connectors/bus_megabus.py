# connectors/bus_megabus.py
import io, zipfile
import pandas as pd
import requests
from connectors.bus_flixbus import (
    _parse_time_to_sec, _sec_to_hhmm, _extract_city, _infer_country
)

BODS_GTFS_ALL = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/all/"
OPERATOR_NAME = "Megabus UK"
import re
AGENCY_PATTERN = re.compile("mega", re.IGNORECASE)

def _http_get(url, tries=3, timeout=180):
    err = None
    for _ in range(tries):
        try:
            r = requests.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r.content
            err = f"HTTP {r.status_code}"
        except Exception as e:
            err = str(e)
    raise RuntimeError(f"Download failed: {err}")

def _read_csv(zf: zipfile.ZipFile, name: str, usecols=None):
    with zf.open(name) as f:
        raw = f.read()
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc, dtype=str, usecols=usecols, low_memory=False)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(raw), encoding="utf-8", dtype=str, usecols=usecols, low_memory=False, errors="ignore")

def _first_last_from_stop_times(zf: zipfile.ZipFile, keep_trip_ids: set, chunksize=500_000):
    cols = ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]
    first_parts, last_parts = [], []
    with zf.open("stop_times.txt") as fh:
        reader = pd.read_csv(fh, dtype=str, usecols=cols, chunksize=chunksize, low_memory=False, encoding="latin-1")
        for chunk in reader:
            if keep_trip_ids:
                chunk = chunk[chunk["trip_id"].isin(keep_trip_ids)]
                if chunk.empty:
                    continue
            chunk["stop_sequence"] = pd.to_numeric(chunk["stop_sequence"], errors="coerce")
            chunk = chunk.dropna(subset=["stop_sequence"])

            idx_min = chunk.groupby("trip_id")["stop_sequence"].idxmin()
            idx_max = chunk.groupby("trip_id")["stop_sequence"].idxmax()
            first_parts.append(chunk.loc[idx_min, ["trip_id","departure_time","stop_id","stop_sequence"]].rename(
                columns={"stop_id":"origin_stop_id","departure_time":"t0","stop_sequence":"seq0"}))
            last_parts.append(chunk.loc[idx_max, ["trip_id","arrival_time","stop_id","stop_sequence"]].rename(
                columns={"stop_id":"destination_stop_id","arrival_time":"t1","stop_sequence":"seq1"}))
    if not first_parts or not last_parts:
        return pd.DataFrame(), pd.DataFrame()
    first_all = pd.concat(first_parts, ignore_index=True)
    last_all  = pd.concat(last_parts, ignore_index=True)
    first_all = first_all.sort_values(["trip_id","seq0"]).groupby("trip_id", as_index=False).first()
    last_all  = last_all.sort_values(["trip_id","seq1"]).groupby("trip_id", as_index=False).last()
    return first_all, last_all

def _build_df(zbytes: bytes) -> pd.DataFrame:
    zf = zipfile.ZipFile(io.BytesIO(zbytes))

    agencies = _read_csv(zf, "agency.txt", usecols=["agency_id","agency_name"])
    agencies["__n"] = agencies["agency_name"].str.lower()
keep_agency_ids = agencies.loc[
    agencies["agency_name"].apply(lambda x: bool(AGENCY_PATTERN.search(str(x)))),
    "agency_id"
]
    ].dropna().astype(str).unique().tolist()

    routes = _read_csv(zf, "routes.txt", usecols=["route_id","agency_id"])
    if keep_agency_ids:
        routes = routes[(routes["agency_id"].astype(str).isin(keep_agency_ids)) | routes["agency_id"].isna()]

    trips = _read_csv(zf, "trips.txt", usecols=["trip_id","route_id","service_id"])
    trips = trips[trips["route_id"].isin(routes["route_id"])]
    keep_trip_ids = set(trips["trip_id"].tolist())

    first, last = _first_last_from_stop_times(zf, keep_trip_ids)
    if first.empty or last.empty:
        return pd.DataFrame()

    merged = trips.merge(first, on="trip_id", how="inner").merge(last, on="trip_id", how="inner")

    stops = _read_csv(zf, "stops.txt", usecols=["stop_id","stop_name","stop_lat","stop_lon"])
    o = merged.merge(stops.rename(columns={"stop_id":"origin_stop_id",
                                           "stop_name":"origin_station",
                                           "stop_lat":"origin_lat",
                                           "stop_lon":"origin_lon"}), on="origin_stop_id", how="left")
    o = o.merge(stops.rename(columns={"stop_id":"destination_stop_id",
                                      "stop_name":"destination_station",
                                      "stop_lat":"dest_lat",
                                      "stop_lon":"dest_lon"}), on="destination_stop_id", how="left")

    def t2s(x): 
        s = _parse_time_to_sec(x)
        return s if s is not None else -1
    o["dur_s"] = (o["t1"].map(t2s) - o["t0"].map(t2s))
    o = o[(o["dur_s"] > 0) & (o["dur_s"] < 48*3600)]

    o["origin_city"] = o["origin_station"].map(_extract_city)
    o["destination_city"] = o["destination_station"].map(_extract_city)
    o["origin_country"] = o.apply(lambda r: _infer_country(r["origin_lat"], r["origin_lon"]), axis=1)
    o["destination_country"] = o.apply(lambda r: _infer_country(r["dest_lat"], r["dest_lon"]), axis=1)

    freq = o.groupby(
        ["origin_station","destination_station","origin_city","destination_city","origin_country","destination_country"],
        dropna=False
    ).size().reset_index(name="frequency_daily")

    def label(n: int):
        if n <= 5: return "Very Low (0-5)"
        if n <=15: return "Low (6-15)"
        if n <=25: return "Average (16-25)"
        if n <=35: return "High (26-35)"
        return "Very High (36+)"

    freq["frequency_label"] = freq["frequency_daily"].astype(int).map(label)

    durs = o.groupby(
        ["origin_station","destination_station","origin_city","destination_city","origin_country","destination_country"],
        dropna=False
    )["dur_s"].mean().reset_index()
    durs["duration"] = durs["dur_s"].round().astype(int).map(_sec_to_hhmm)

    out = durs.merge(freq, on=["origin_station","destination_station","origin_city","destination_city","origin_country","destination_country"], how="left")
    out.insert(0, "operator_name", OPERATOR_NAME)
    out.insert(0, "transport_type", "bus")
    return out[[
        "transport_type","operator_name","duration","frequency_daily","frequency_label",
        "origin_station","destination_station","origin_city","destination_city",
        "origin_country","destination_country"
    ]]

def fetch_routes() -> pd.DataFrame:
    print("Fetching Megabus (BODS GTFS ALL, streamed)…")
    z = _http_get(BODS_GTFS_ALL)
    return _build_df(z)
