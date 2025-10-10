# connectors/bus_avanza.py
import io, os, zipfile, pandas as pd
from connectors.bus_flixbus import (
    _get_with_retries, _parse_time_to_sec, _sec_to_hhmm, _extract_city, _infer_country
)

# Avanza via Spain NAP (example Division Norte "Fichero" 1713 seen on Transitland).
# You can override with env var ES_NAP_AVANZA_FILE_ID if you have a better/all-operations file id.
NAP_BASE = "https://nap.transportes.gob.es/api/Fichero/download/"
NAP_FILE_ID = os.getenv("ES_NAP_AVANZA_FILE_ID", "1713")
ES_NAP_APIKEY = os.getenv("ES_NAP_APIKEY")
OPERATOR_NAME = "Avanza"
AGENCY_MATCH  = ["avanza"]

def _read_csv(zf: zipfile.ZipFile, name: str) -> pd.DataFrame:
    with zf.open(name) as f:
        raw = f.read()
    for enc in ("utf-8","utf-8-sig","latin-1"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(raw), encoding="utf-8", errors="ignore")

def fetch_routes() -> pd.DataFrame:
    if not ES_NAP_APIKEY:
        print("Avanza: ES_NAP_APIKEY not set — skipping Avanza for now.")
        return pd.DataFrame(columns=[
            "transport_type","operator_name","duration","frequency_daily","frequency_label",
            "origin_station","destination_station","origin_city","destination_city",
            "origin_country","destination_country"
        ])
    url = NAP_BASE + str(NAP_FILE_ID)
    print(f"Fetching Avanza from Spain NAP (file {NAP_FILE_ID})…")
    zbytes = _get_with_retries(url, headers={"ApiKey": ES_NAP_APIKEY})
    zf = zipfile.ZipFile(io.BytesIO(zbytes))

    agencies = _read_csv(zf, "agency.txt")
    routes   = _read_csv(zf, "routes.txt")
    trips    = _read_csv(zf, "trips.txt")
    st       = _read_csv(zf, "stop_times.txt")
    stops    = _read_csv(zf, "stops.txt")
    cal = _read_csv(zf, "calendar.txt") if "calendar.txt" in zf.namelist() else None

    agencies["__n"] = agencies["agency_name"].astype(str).str.lower()
    keep_ids = agencies.loc[agencies["__n"].str.contains("|".join(AGENCY_MATCH), na=False), "agency_id"].astype(str).unique().tolist()
    routes = routes[routes.get("agency_id","").astype(str).isin(keep_ids) | routes.get("agency_id").isna()]

    st["stop_sequence"] = pd.to_numeric(st["stop_sequence"], errors="coerce")
    st = st.dropna(subset=["stop_sequence"])
    st_sorted = st.sort_values(["trip_id","stop_sequence"])
    firsts = st_sorted.groupby("trip_id").first().reset_index()
    lasts  = st_sorted.groupby("trip_id").last().reset_index()

    def t2s(x):
        try: return _parse_time_to_sec(str(x))
        except: return None

    firsts["dep_s"] = firsts["departure_time"].map(t2s)
    lasts["arr_s"]  = lasts["arrival_time"].map(t2s)

    trip = firsts[["trip_id","stop_id","dep_s"]].merge(
        lasts[["trip_id","stop_id","arr_s"]],
        on="trip_id", suffixes=("_o","_d")
    )
    trips = trips[trips["route_id"].astype(str).isin(routes["route_id"].astype(str))]
    trip = trip.merge(trips[["trip_id","service_id","route_id"]], on="trip_id", how="left")
    trip["duration_s"] = (trip["arr_s"] - trip["dep_s"]).clip(lower=0)

    stops_min = stops[["stop_id","stop_name","stop_lat","stop_lon"]].copy()
    o = trip.merge(stops_min, left_on="stop_id_o", right_on="stop_id", how="left")
    o = o.merge(stops_min, left_on="stop_id_d", right_on="stop_id", how="left", suffixes=("_o","_d"))

    o["origin_station"]      = o["stop_name_o"].astype(str)
    o["destination_station"] = o["stop_name_d"].astype(str)
    o["origin_city"]         = o["origin_station"].map(_extract_city)
    o["destination_city"]    = o["destination_station"].map(_extract_city)
    o["origin_country"]      = _infer_country(o["stop_lat_o"], o["stop_lon_o"])
    o["destination_country"] = _infer_country(o["stop_lat_d"], o["stop_lon_d"])

    if cal is not None and "monday" in cal.columns:
        cal_use = cal[["service_id","monday","tuesday","wednesday","thursday","friday","saturday","sunday"]].copy()
        cal_use.iloc[:,1:] = cal_use.iloc[:,1:].apply(pd.to_numeric, errors="coerce").fillna(0).astype(int)
        o = o.merge(cal_use, on="service_id", how="left")
        o["freq_daily"] = 1
    else:
        o["freq_daily"] = 1

    grp = o.groupby([
        "origin_station","destination_station",
        "origin_city","destination_city",
        "origin_country","destination_country"
    ], dropna=False)

    agg = grp.agg(duration_s=("duration_s","mean"), trips_day=("freq_daily","sum")).reset_index()

    def lab(n):
        n = int(n or 0)
        if n <= 5: return "Very Low (0-5)"
        if n <=15: return "Low (6-15)"
        if n <=25: return "Average (16-25)"
        if n <=35: return "High (26-35)"
        return "Very High (36+)"

    agg["duration"] = agg["duration_s"].fillna(0).map(lambda s: _sec_to_hhmm(int(round(s))))
    agg["frequency_daily"] = agg["trips_day"].fillna(0).astype(int)
    agg["frequency_label"] = agg["frequency_daily"].map(lab)

    out = agg[[
        "duration","frequency_daily","frequency_label",
        "origin_station","destination_station",
        "origin_city","destination_city",
        "origin_country","destination_country"
    ]].copy()
    out.insert(0, "operator_name", OPERATOR_NAME)
    out.insert(0, "transport_type", "bus")
    return out
