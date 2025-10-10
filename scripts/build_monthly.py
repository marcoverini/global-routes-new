import os
import pandas as pd

# --- Import all connectors ---
from connectors import (
    bus_flixbus,
    bus_alsa,
    bus_avanza,
    bus_blablabus,
    bus_irishcitylink,
    bus_megabus,
    bus_nationalexpress,
)

def main(out_dir="data/outputs"):
    print("🚌 Building global bus dataset...")

    connectors = [
        ("FlixBus", bus_flixbus),
        ("ALSA", bus_alsa),
        ("Avanza", bus_avanza),
        ("BlaBlaBus", bus_blablabus),
        ("Irish Citylink", bus_irishcitylink),
        ("Megabus", bus_megabus),
        ("National Express", bus_nationalexpress),
    ]

    frames = []

    for name, module in connectors:
        print(f"\n▶ Fetching {name} routes...")
        try:
            df = module.fetch_routes()
            if df is not None and not df.empty:
                print(f"✅ {name}: {len(df):,} routes fetched")
                frames.append(df)
            else:
                print(f"⚠️ {name}: no routes returned")
        except Exception as e:
            print(f"❌ Error fetching {name}: {e}")

    if not frames:
        print("❌ No data fetched from any operator. Exiting.")
        return

    print("\n🧩 Merging all operators into one dataset...")
    combined = pd.concat(frames, ignore_index=True).drop_duplicates()

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "world_bus.csv")
    combined.to_csv(out_path, index=False, encoding="utf-8")

    print(f"\n✅ Saved {len(combined):,} total routes to {out_path}")
    print("Included operators:")
    for name, _ in connectors:
        print(f"   • {name}")

if __name__ == "__main__":
    main()
