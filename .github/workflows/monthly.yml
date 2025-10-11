import os
import pandas as pd

# --- Import your GTFS/JSON connectors (keep the ones you already have) ---
from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink,
    # optional: these may return empty for now; harmless to keep
    bus_alsa,
    bus_avanza,
    bus_blablabus,
    # NOTE: we are NOT calling the Megabus connector here anymore,
    # because we’ll ingest a vendor CSV instead (see load_vendor_megabus()).
)

# Optional helpers (only if you want fallback city/country inference later)
try:
    from connectors.bus_flixbus import _extract_city, _infer_country, _sec_to_hhmm  # noqa
except Exception:
    _extract_city = None
    _infer_country = None
    _sec_to_hhmm = None


def _bucket(n: int) -> str:
    n = int(n or 0)
    if n <= 5:  return "Very Low (0-5)"
    if n <= 15: return "Low (6-15)"
    if n <= 25: return "Average (16-25)"
    if n <= 35: return "High (26-35)"
    return "Very High (36+)"


def load_vendor_megabus(local_path="data/vendor/megabus.csv") -> pd.DataFrame:
    """Load a pre-generated Megabus CSV (no API calls). If not present, return empty DF."""
    cols = [
        "transport_type","operator_name","duration","frequency_daily","frequency_label",
        "origin_station","destination_station","origin_city","destination_city",
        "origin_country","destination_country"
    ]
    if not os.path.exists(local_path):
        print(f"Megabus vendor CSV not found at {local_path} — skipping.")
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(local_path, dtype=str, low_memory=False)

    # Normalize columns
    # Allow some flexibility if vendor file uses different names
    rename_map = {
        "freq_daily": "frequency_daily",
        "frequency": "frequency_daily",
        "operator": "operator_name",
        "from_station": "origin_station",
        "to_station": "destination_station",
        "from_city": "origin_city",
        "to_city": "destination_city",
        "from_country": "origin_country",
        "to_country": "destination_country",
        "duration_minutes": "duration"  # will convert below if numeric
    }
    for k, v in rename_map.items():
        if k in df.columns and v not in df.columns:
            df[v] = df[k]

    # Ensure required columns exist
    for c in cols:
        if c not in df.columns:
            df[c] = None

    # If duration is numeric minutes, convert to HH:MM
    if df["duration"].astype(str).str.fullmatch(r"\d+").any() and _sec_to_hhmm is not None:
        mins = pd.to_numeric(df["duration"], errors="coerce")
        df["duration"] = mins.fillna(0).astype(int).map(lambda m: _sec_to_hhmm(m * 60))
    # Otherwise assume duration is already "HH:MM"

    # Fill defaults
    df["transport_type"] = "bus"
    df["operator_name"] = df["operator_name"].fillna("Megabus")

    # Frequency label if missing
    if df["frequency_label"].isna().any():
        df["frequency_daily"] = pd.to_numeric(df["frequency_daily"], errors="coerce").fillna(0).astype(int)
        df.loc[df["frequency_label"].isna(), "frequency_label"] = df.loc[df["frequency_label"].isna(), "frequency_daily"].map(_bucket)

    # A little cleanup (strip spaces)
    for c in ["origin_station","destination_station","origin_city","destination_city","origin_country","destination_country"]:
        df[c] = df[c].astype(str).str.strip()

    # Keep schema & de-dup
    out = df[cols].drop_duplicates()
    print(f"✅ Megabus vendor rows loaded: {len(out):,}")
    return out


def main(out_dir="data/outputs"):
    print("🚌 Building global bus dataset...")

    frames = []

    # 1) Vendor Megabus (no API calls; just a CSV if present)
    frames.append(load_vendor_megabus("data/vendor/megabus.csv"))

    # 2) Live connectors that work well on GitHub Actions
    connectors = [
        ("FlixBus", bus_flixbus),
        ("National Express", bus_nationalexpress),
        ("Irish Citylink", bus_irishcitylink),
        # Optional: these may return empty for now
        ("ALSA", bus_alsa),
        ("Avanza", bus_avanza),
        ("BlaBlaBus", bus_blablabus),
    ]

    for name, module in connectors:
        try:
            print(f"\n▶ Fetching {name} routes...")
            df = module.fetch_routes()
            if df is not None and not df.empty:
                print(f"✅ {name}: {len(df):,} routes fetched")
                frames.append(df)
            else:
                print(f"⚠️ {name}: no routes returned")
        except Exception as e:
            print(f"❌ Error fetching {name}: {e}")

    if not any((f is not None and len(f) > 0) for f in frames):
        print("❌ No data fetched — aborting write.")
        return

    print("\n🧩 Merging all operators into one dataset...")
    combined = pd.concat([f for f in frames if f is not None and not f.empty], ignore_index=True).drop_duplicates()

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "world_bus.csv")
    combined.to_csv(out_path, index=False, encoding="utf-8")

    print(f"\n✅ Saved {len(combined):,} total routes to {out_path}")
    print("Included sources:")
    print("   • Vendor: Megabus (CSV)")
    for name, _ in connectors:
        print(f"   • {name}")

if __name__ == "__main__":
    main()
