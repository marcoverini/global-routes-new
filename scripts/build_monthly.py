import os
import pandas as pd

from connectors import (
    bus_flixbus,
    bus_nationalexpress,
    bus_irishcitylink,
    bus_megabus_uk,
    bus_megabus_na,
)

def _bucket(n: int) -> str:
    try:
        n = int(n or 0)
    except Exception:
        n = 0
    if n <= 5:  return "Very Low (0-5)"
    if n <= 15: return "Low (6-15)"
    if n <= 25: return "Average (16-25)"
    if n <= 35: return "High (26-35)"
    return "Very High (36+)"

def main(out_dir="data/outputs"):
    print("🚌 Building global bus dataset...")
    os.makedirs(out_dir, exist_ok=True)

    frames = []
    connectors = [
        ("FlixBus", bus_flixbus),
        ("National Express", bus_nationalexpress),
        ("Irish Citylink", bus_irishcitylink),
        ("Megabus UK", bus_megabus_uk),
        ("Megabus North America", bus_megabus_na),
    ]

    for name, module in connectors:
        try:
            print(f"\n▶ Fetching {name} routes…")
            df = module.fetch_routes()
            if df is None or df.empty:
                print(f"⚠️ {name}: 0 rows")
                continue

            # fill frequency label if missing
            if "frequency_label" in df.columns and df["frequency_label"].isna().any():
                if "frequency_daily" in df.columns:
                    df["frequency_daily"] = pd.to_numeric(df["frequency_daily"], errors="coerce").fillna(0).astype(int)
                    df.loc[df["frequency_label"].isna(), "frequency_label"] = df.loc[df["frequency_label"].isna(), "frequency_daily"].map(_bucket)

            frames.append(df)
            print(f"✅ {name}: {len(df)} rows")
        except Exception as e:
            print(f"❌ {name} failed: {e}")

    if not frames:
        print("❌ No data to combine.")
        return

    combined = pd.concat(frames, ignore_index=True).drop_duplicates()
    out_fp = os.path.join(out_dir, "world_bus.csv")
    combined.to_csv(out_fp, index=False, encoding="utf-8")
    print(f"\n✅ Saved {len(combined):,} rows to {out_fp}")

if __name__ == "__main__":
    main()
