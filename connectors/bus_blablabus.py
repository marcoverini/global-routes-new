# connectors/bus_blablabus.py
import io, os, re, zipfile, pandas as pd, requests
from connectors.bus_flixbus import (
    _get_with_retries, _parse_time_to_sec, _sec_to_hhmm, _extract_city, _infer_country
)

# We fetch the resource page on transport.data.gouv.fr and grab the Drive URL.
RESOURCE_PAGE = "https://transport.data.gouv.fr/resources/52605?locale=en"
OPERATOR_NAME  = "BlaBlaCar Bus"
AGENCY_MATCHES = [r"blablacar\s*bus", r"ouibus", r"blablabus"]  # historic names too

def _find_drive_link(html: str) -> str:
    # find first Google Drive link on the page
    m = re.search(r"https://drive\.google\.com/[^\"]+", html)
    if not m:
        return ""
    url = m.group(0)
    # convert /file/d/<id>/view? to direct uc? download link
    m2 = re.search(r"/file/d/([^/]+)/", url)
    if m2:
        fid = m2.group(1)
        return f"https://drive.google.com/uc?export=download&id={fid}"
    return url

def _download_gtfs_bytes() -> bytes:
    # get page -> drive url -> zip
    page = _get_with_retries(RESOURCE_PAGE)
    drive = _find_drive_link(page.decode("utf-8", errors="ignore"))
    if not drive:
        raise RuntimeError("Could not find BlaBlaCar Bus GTFS download link on resource page.")
    return _get_with_retries(drive)

def _read_csv(zf: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with zf.open(name) as f:
        data = f.read()
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(data), encoding=enc)
        except Exception:
            continue
    # last resort
    return pd.read_csv(io.BytesIO(data), encoding="utf-8", errors="ignore")

def _build_df_from_gtfs(zbytes: bytes, operator_name: str, agency_regexes) -> pd.DataFrame:
    zf = zipfile.ZipFile(io.BytesIO(zbytes))
    # required files
    agencies = _read_csv(zf, "agency.txt")
    routes   = _read_csv(zf, "routes.txt")
    trips    = _read_csv(zf, "trips.txt")
    stop_times = _read_csv(zf, "stop_times.txt")
    stops    = _read_csv(zf, "stops.txt")
    cal = None
    if "calendar.txt" in zf.namelist():
        cal = _read_csv(zf, "calendar.txt")

    # normalize names for matching agencies
    agencies["__n"] = agencies["agency_name"].astype(str).str.lower()
    rx = re.compile("|".join(agency_regexes), flags=re.I)
    keep_agency_ids = agencies.loc[agencies["__n"].str.contains(rx), "agency_id"].astype(str).unique()
    if len(keep_agency_ids) == 0:
        # Some feeds omit agency_id, use all
        keep_agency_ids = agencies.get("agency_id", pd.Series([], dtype=str)).astype(str).unique()

    # filter to target operator
    routes = routes[routes.get("agency_id", "").astype(str).isin(keep_agency_ids) | routes.get("agency_id").isna()]
    trips  = trips[trips["route_id"].astype(str).isin(routes["route_id"].astype(str))]

    # compute trip first/last stops + duration
    stop_times["stop_sequence"] = pd.to_numeric(stop_times["stop_sequence"], errors="coerce")
    stop_times = stop_times.dropna(subset=["stop_sequence"])
    # first and last rows per trip
    idx_first = stop_times.sort_values(["trip_id","stop_sequence"]).groupby("trip_id").first().reset_index()
    idx_last  = stop_times.sort_values(["trip_id","stop_sequence"]).groupby("trip_id").last().reset_index()

    def _t2s(x): 
        try: return _parse_time_to_sec(str(x))
        except: return None

    idx_first["dep_s"] = idx_first["departure_time"].map(_t2s)
    idx_last["arr_s"]  = idx_last["arrival_time"].map(_t2s)
    trip_span = idx_first.merge(idx_last[["trip_id","stop_id","arr_s"]], on="trip_id", suffixes=("_orig","_dest"))
    trip_span.rename(columns={"stop_id_orig":"origin_stop_id","stop_id_dest":"destination_stop_id"}, inplace=True)
    trip_span = trip_span.merge(trips[["trip_id","route_id","service_id"]], on="trip_id", how="left")
    trip_span["duration_s"] = (trip_span["arr_s"] - trip_span["dep_s"]).clip(lower=0)

    # join stops (names + lat/lon)
    stops_min = stops[["stop_id","stop_name","stop_lat","stop_lon"]].copy()
    o = trip_span.merge(stops_min, left_on="origin_stop_id", right_on="stop_id", how="left")
    o = o.merge(stops_min, left_on="destination_stop_id", right_on="stop_id", how="left", suffixes=("_o","_d"))

    # city + country
    o["origin_station"]      = o["stop_name_o"].astype(str)
    o["destination_station"] = o["stop_name_d"].astype(str)
    o["origin_city"]         = o["origin_station"].map(_extract_city)
    o["destination_city"]    = o["destination_station"].map(_extract_city)
    o["origin_country"]      = _infer_country(o["stop_lat_o"], o["stop_lon_o"])
    o["destination_country"] = _infer_country(o["stop_lat_d"], o["stop_lon_d"])

    # frequency estimate: trips per typical weekday (Mon) or max-day fallback
    freq = pd.Series(1, index=o["trip_id"]).groupby(o["trip_id"]).sum().to_frame("trip_count").reset_index()
    o = o.merge(freq, on="trip_id", how="left")
    if cal is not None and "service_id" in o.columns and "monday" in cal.columns:
        cal_use = cal[["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday"]].copy()
        cal_use.iloc[:,1:] = cal_use.iloc[:,1:].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
        o = o.merge(cal_use, on="service_id", how="left")
        # trips per day = sum over services that run that day (approx)
        o["weekday_runs"] = o[["monday","tuesday","wednesday","thursday","friday"]].max(axis=1).fillna(1)
        o["freq_daily"]   = 1  # each trip counts once; we aggregate next
    else:
        o["freq_daily"] = 1

    # aggregate by origin/destination station
    grp = o.groupby(["origin_station","destination_station","origin_city","destination_city",
                     "origin_country","destination_country"], dropna=False)

    agg = grp.agg(
        duration_s=("duration_s","mean"),
        trips_day=("freq_daily","sum")
    ).reset_index()

    # label
    def _label(n):
        n = int(n or 0)
        if n <= 5: return "Very Low (0-5)"
        if n <=15: return "Low (6-15)"
        if n <=25: return "Average (16-25)"
        if n <=35: return "High (26-35)"
        return "Very High (36+)"
    agg["duration"] = agg["duration_s"].fillna(0).map(lambda s: _sec_to_hhmm(int(round(s))))
    agg["frequency_daily"] = agg["trips_day"].fillna(0).astype(int)
    agg["frequency_label"] = agg["frequency_daily"].map(_label)

    out = agg[[
        "duration","frequency_daily","frequency_label",
        "origin_station","destination_station",
        "origin_city","destination_city",
        "origin_country","destination_country"
    ]].copy()
    out.insert(0, "operator_name", operator_name)
    out.insert(0, "transport_type", "bus")
    return out

def fetch_routes() -> pd.DataFrame:
    print("Fetching BlaBlaCar Bus GTFS…")
    z = _download_gtfs_bytes()
    return _build_df_from_gtfs(z, OPERATOR_NAME, AGENCY_MATCHES)
