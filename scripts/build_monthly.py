# scripts/build_monthly.py
import os
import pandas as pd
from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink
)

# Create output directory if not exists
os.makedirs("data/outputs", exist_ok=True)

def main(out_dir="data/outputs"):
    print("🚌 Building global bus dataset...")

    # List to collect all dataframes
    frames = []

    # --- 1. FlixBus ---
    print("\n▶ Fetching FlixBus routes…")
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

    # --- 4. Vendor (static) datasets like Megabus ---
    print("\n▶ Merging vendor datasets…")
    vendor_dir = os.path.join("data", "vendor")
    if os.path.exists(vendor_dir):
        for f in os.listdir(vendor_dir):
            if f.endswith(".csv"):
                path = os.path.join(vendor_dir, f)
                print(f"   → Including vendor dataset: {f}")
                try:
                    vdf = pd.read_csv(path)
                    frames.append(vdf)
                except Exception as e:
                    print(f"   ⚠️ Failed to load {f}: {e}")
    else:
        print("⚠️ No vendor directory found")

    # --- Combine all routes ---
    if frames:
        df_all = pd.concat(frames, ignore_index=True)
        print(f"\n✅ Total combined routes: {len(df_all)}")
    else:
        print("⚠️ No data to combine.")
        df_all = pd.DataFrame()

    # --- Save final dataset ---
    out_path = os.path.join(out_dir, "world_bus.csv")
    df_all.to_csv(out_path, index=False)
    print(f"\n💾 Saved to {out_path}")

if __name__ == "__main__":
    main("data/outputs")
