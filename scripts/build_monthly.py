# scripts/build_monthly.py
import os
import glob
import pandas as pd
from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink
)

os.makedirs("data/outputs", exist_ok=True)

def load_vendor_csv(path):
    """Load vendor CSV safely, auto-detecting delimiters and fixing headers."""
    try:
        # Detect delimiter (, or tab)
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline()
        sep = '\t' if '\t' in first_line else ','

        df = pd.read_csv(path, sep=sep, encoding='utf-8', on_bad_lines='skip')

        # Normalize column names
        df.columns = (
            df.columns.str.strip()
                      .str.lower()
                      .str.replace('\t', '')
                      .str.replace(' ', '_')
        )

        # Drop empty columns
        df = df.dropna(how='all', axis=1)

        print(f"   ✅ Loaded {os.path.basename(path)} ({len(df)} rows, sep='{sep}')")
        return df
    except Exception as e:
        print(f"   ⚠️ Failed to load {path}: {e}")
        return pd.DataFrame()

def main(out_dir="data/outputs"):
    print("🚌 Building global bus dataset...\n")

    frames = []

    # --- 1. FlixBus ---
    print("▶ Fetching FlixBus routes…")
    try:
        df_flix = bus_flixbus.fetch_routes()
        print(f"✅ FlixBus: {len(df_flix)} rows")
        frames.append(df_flix)
    except Exception as e:
        print(f"❌ FlixBus failed: {e}")

    # --- 2. National Express ---
    print("\n▶ Fetching National Express routes…")
    try:
        df_ne = bus_nationalexpress.fetch_routes()
        print(f"✅ National Express: {len(df_ne)} rows")
        frames.append(df_ne)
    except Exception as e:
        print(f"❌ National Express failed: {e}")

    # --- 3. Irish Citylink ---
    print("\n▶ Fetching Irish Citylink routes…")
    try:
        df_citylink = bus_irishcitylink.fetch_routes()
        print(f"✅ Irish Citylink: {len(df_citylink)} rows")
        frames.append(df_citylink)
    except Exception as e:
        print(f"❌ Irish Citylink failed: {e}")

    # --- 4. Vendor CSVs ---
    print("\n▶ Including vendor datasets…")
    vendor_dir = os.path.join("data", "vendor")
    vendor_files = glob.glob(os.path.join(vendor_dir, "*.csv"))
    if not vendor_files:
        print("⚠️ No vendor CSV files found.")
    else:
        for vf in vendor_files:
            vdf = load_vendor_csv(vf)
            if not vdf.empty:
                frames.append(vdf)

    # --- 5. Combine everything ---
    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        print(f"\n✅ Total combined routes: {len(df_all)} rows")
    else:
        print("⚠️ No data combined.")
        df_all = pd.DataFrame()

    # --- 6. Save ---
    out_path = os.path.join(out_dir, "world_bus.csv")
    df_all.to_csv(out_path, index=False, encoding="utf-8")
    print(f"💾 Saved combined dataset to {out_path}")

if __name__ == "__main__":
    main("data/outputs")
