import os
import pandas as pd
from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink,
)

def main(out_dir: str):
    print("Building global bus dataset...")
    os.makedirs(out_dir, exist_ok=True)

    # 1️⃣ Fetch data from connectors
    frames = []
    for mod, name in [
        (bus_flixbus, "FlixBus"),
        (bus_nationalexpress, "National Express"),
        (bus_irishcitylink, "Irish Citylink"),
    ]:
        try:
            print(f"Fetching {name} routes...")
            df = mod.fetch_routes()
            df["operator_name"] = name
            frames.append(df)
            print(f"✅ {name}: {len(df)} routes")
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    # 2️⃣ Add vendor CSVs from data/vendor
    vendor_dir = os.path.join("data", "vendor")
    if os.path.exists(vendor_dir):
        for file in os.listdir(vendor_dir):
            if file.endswith(".csv"):
                path = os.path.join(vendor_dir, file)
                try:
                    print(f"Adding vendor file: {file}")
                    df_vendor = pd.read_csv(path)
                    frames.append(df_vendor)
                    print(f"✅ Loaded {len(df_vendor)} rows from {file}")
                except Exception as e:
                    print(f"❌ Failed to load {file}: {e}")
    else:
        print("⚠️ No vendor directory found.")

    # 3️⃣ Combine all data
    if frames:
        combined = pd.concat(frames, ignore_index=True)
        combined.to_csv(os.path.join(out_dir, "world_bus.csv"), index=False)
        print(f"✅ Combined total routes: {len(combined)}")
    else:
        print("❌ No data to combine.")

if __name__ == "__main__":
    main("data/outputs")
