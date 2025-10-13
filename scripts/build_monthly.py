# scripts/build_monthly.py
import os
import glob
import pandas as pd
from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink
)

# Ensure output directory exists
os.makedirs("data/outputs", exist_ok=True)

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

    # --- 4. Vendor datasets (static .csv files like ALSA, Megabus, etc.) ---
    print("\n▶ Including vendor datasets…")
    vendor_dir = os.path.join("data", "vendor")

    if not os.path.exists(vendor_dir):
        print("⚠️ No vendor directory found, skipping.")
    else:
        vendor_files = glob.glob(os.path.join(vendor_dir, "*.csv"))
        if not vendor_files:
            print("⚠️ No vendor CSV files found in data/vendor/")
        else:
            for vf in vendor_files:
                try:
                    vdf = pd.read_csv(vf)
                    print(f"   → Added vendor dataset: {os.path.basename(vf)} ({len(vdf)} rows)")
                    frames.append(vdf)
                except Exception as e:
                    print(f"   ⚠️ Failed to load {vf}: {e}")

    # --- 5. Combine everything ---
    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        print(f"\n✅ Total combined routes: {len(df_all)} rows")
    else:
        print("⚠️ No data combined, output will be empty.")
        df_all = pd.DataFrame()

    # --- 6. Save final output ---
    out_path = os.path.join(out_dir, "world_bus.csv")
    df_all.to_csv(out_path, index=False, encoding="utf-8")
    print(f"💾 Saved combined dataset to {out_path}")

if __name__ == "__main__":
    main("data/outputs")
