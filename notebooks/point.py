import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
import json
import os
import sys
import math

# TimezoneFinder instance
tf = TimezoneFinder()

def parse_datetime(dt_str):
    try:
        return pd.to_datetime(dt_str, utc=True)
    except Exception:
        return pd.NaT

def csv_to_geojson_with_geopandas(csv_path, output_path):
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(csv_path)

    # Capitalize all column names
    df.columns = [col.upper() for col in df.columns]

    # Parse datetime and drop invalid coordinates
    df["TIME"] = df["TIME"].apply(parse_datetime)
    df = df.dropna(subset=["LONGITUDE", "LATITUDE"])

    # Create geometry column using shapely Points
    df["geometry"] = df.apply(lambda row: Point(row["LONGITUDE"], row["LATITUDE"]), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    features = []

    for idx, row in gdf.iterrows():
        time = row["TIME"]
        longitude = row["geometry"].x
        latitude = row["geometry"].y

        # Convert to local time based on coordinates
        obsdatelocal = None
        if pd.notnull(time) and not math.isnan(longitude) and not math.isnan(latitude):
            tz_name = tf.timezone_at(lng=longitude, lat=latitude)
            if tz_name:
                obsdatelocal = time.astimezone(ZoneInfo(tz_name)).isoformat()

        obsdateutc = time.astimezone(ZoneInfo("UTC")).isoformat() if pd.notnull(time) else None

        feature = {
            "type": "Feature",
            "properties": {
                "obsdatelocal": obsdatelocal,
                "obsdateutc": obsdateutc,
                "teamname_targetid": row.get("TEAM_TARGETID", 0),
                "frp": row["FRP"] if "FRP" in row and pd.notnull(row["FRP"]) and not math.isnan(row["FRP"]) else None,
            },
            "geometry": {
                "type": "Point",
                "coordinates": [longitude, latitude],
            },
            "id": int(idx) + 1
        }

        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    # Save the GeoJSON to the specified output path
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"✅ GeoJSON saved to: {output_path}")

# Command-line interface
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python csv_to_geojson.py <input_csv_file> <output_geojson_file>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    output_path = sys.argv[2]

    csv_to_geojson_with_geopandas(csv_path, output_path)
