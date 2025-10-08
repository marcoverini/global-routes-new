import os, pandas as pd
from connectors import bus_flixbus

def main(out_dir):
    os.makedirs(out_dir, exist_ok=True)

    df_flix = bus_flixbus.fetch_routes()
    out_path = os.path.join(out_dir, "world_bus.csv")
    df_flix.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved {len(df_flix)} routes to {out_path}")

if __name__ == "__main__":
    main("data/outputs")
