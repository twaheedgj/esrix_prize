import argparse
import pandas as pd
import geopandas as gpd
from shapely import wkb
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder
import json
import shapely.geometry
from shapely.wkt import loads

def fast_csv_perimeter_to_geojson(csv_path: str, output_path: str):
    # Read the CSV file
    df = pd.read_csv(csv_path)

    # Normalize column names to lowercase for consistency
    df.columns = [col.lower() for col in df.columns]

    # Convert geometry from WKB (hex), GeoJSON, or WKT to shapely objects
    if "perimeter" in df.columns:
        # Try to decode WKB hex, catch exceptions for invalid WKB
        def decode_wkb(x):
            try:
                return wkb.loads(x, hex=True)
            except Exception as e:
                print(f"Error decoding WKB: {e}")
                return None  # Return None for invalid WKB

        df["geometry"] = df["perimeter"].apply(decode_wkb)

    elif "geom" in df.columns:
        # If the 'geom' column contains GeoJSON, decode it
        df["geometry"] = df["geom"].apply(lambda g: shapely.geometry.shape(json.loads(g)))
    
    elif "perimeter_wkt" in df.columns:  # If you have WKT data (e.g., POLYGON)
        # If the 'perimeter_wkt' column contains WKT strings, decode it
        df["geometry"] = df["perimeter_wkt"].apply(lambda x: loads(x))
    
    else:
        raise ValueError("No geometry column ('perimeter', 'geom', or 'perimeter_wkt') found in the CSV.")

    # Drop rows where geometry is None (invalid WKB)
    df = df.dropna(subset=["geometry"])

    # Convert the DataFrame to a GeoDataFrame with the geometry column
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Parse datetime and localize to UTC
    gdf["start_time"] = pd.to_datetime(gdf["start_time"], errors="coerce", utc=True)

    # Initialize the TimezoneFinder
    tf = TimezoneFinder()

    # Use geometry centroid for timezone lookup
    gdf["centroid"] = gdf["geometry"].centroid
    longitudes = gdf["centroid"].x
    latitudes = gdf["centroid"].y

    # Find timezones based on the centroid's coordinates
    timezones = [tf.timezone_at(lng=lon, lat=lat) for lon, lat in zip(longitudes, latitudes)]

    # Localize 'start_time' to the correct timezone
    gdf["obsdatelocal"] = [
        st.astimezone(ZoneInfo(tz)).isoformat() if pd.notnull(st) and tz else None
        for st, tz in zip(gdf["start_time"], timezones)
    ]

    # Convert the 'start_time' to UTC for 'obsdateutc'
    gdf["obsdateutc"] = gdf["start_time"].dt.tz_convert("UTC").astype(str)

    # Optional cleanup: Rename columns and fill missing values
    gdf = gdf.rename(columns={"team_targetid": "teamname_targetid"})

    # List of columns to fill NaN values with 0
    for col in [
        "burnedarea", "obsdirection_n", "obsdirection_e", "obsdirection_s", "obsdirection_w",
        "rateofspread_n", "rateofspread_e", "rateofspread_s", "rateofspread_w", "intensity"
    ]:
        if col in gdf.columns:
            gdf[col] = gdf[col].fillna(0)

    # Drop the 'centroid' column (no longer needed)
    gdf = gdf.drop(columns=["centroid"], errors="ignore")

    # Save the GeoDataFrame to a GeoJSON file
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"✅ GeoJSON saved to {output_path}")

def main():
    # Set up the command-line argument parser
    parser = argparse.ArgumentParser(description="Convert CSV with perimeter data to GeoJSON format")
    parser.add_argument("csv_path", help="Path to the input CSV file")
    parser.add_argument("output_path", help="Path to save the output GeoJSON file")
    
    args = parser.parse_args()
    
    # Run the function to convert CSV to GeoJSON
    fast_csv_perimeter_to_geojson(args.csv_path, args.output_path)

if __name__ == "__main__":
    main()
