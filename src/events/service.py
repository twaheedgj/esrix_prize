import json
from zoneinfo import ZoneInfo
import pandas as pd
from shapely import Point
from sqlmodel.ext.asyncio.session import AsyncSession
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import select
from sqlmodel import text
import math
from shapely import wkb
import shapely.geometry
import geopandas as gpd
from timezonefinder import TimezoneFinder
from shapely import wkt
from shapely.geometry import mapping
tf = TimezoneFinder()
class Service:
    async def leona(self, session: AsyncSession) -> dict:
        statement = """
               SELECT * FROM xp.point WHERE geo = 0
        """
        leona = await session.exec(text(statement))
        leona = leona.all()
        response = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "obsdatelocal": (
                            row.time.astimezone(
                                ZoneInfo(
                                    tf.timezone_at(
                                        lng=row.longitude,
                                        lat=row.latitude
                                    )
                                )
                            ).replace(tzinfo=None).isoformat()
                            if row.time and row.longitude is not None and row.latitude is not None else None
                        ),
                        "obsdateutc": row.time.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat() if row.time else None,
                        "teamname_targetid": getattr(row, 'team_targetid', None),
                        "frp": row.frp if row.frp is not None and not math.isnan(row.frp) else None,
                    },
                    "geometry": {
                        "coordinates": [
                            row.longitude if row.longitude is not None and not math.isnan(row.longitude) else None,
                            row.latitude if row.latitude is not None and not math.isnan(row.latitude) else None,
                        ],
                        "type": "Point",    # Longitude first, then latitude
                    },
                    "id": idx  # Add unique ID
                }
                for idx, row in enumerate(leona, start=1)
            ],
        }
        return response
    
    async def geoevents(self, session: AsyncSession) -> dict:
        statement = """
               SELECT * FROM xp.point WHERE geo = 1
        """
        
        leona = await session.exec(text(statement))
        leona = leona.all()
        response = {
            "type": "FeatureCollection",
            "features": [
               {
                    "type": "Feature",
                    "properties": {
                        "obsdatelocal": (
                            row.time.astimezone(
                                ZoneInfo(
                                    tf.timezone_at(
                                        lng=row.longitude,
                                        lat=row.latitude
                                    )
                                )
                            ).replace(tzinfo=None).isoformat()
                            if row.time and row.longitude is not None and row.latitude is not None else None
                        ),
                        "obsdateutc": row.time.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat() if row.time else None,
                        "teamname_targetid": getattr(row, 'team_targetid', None),
                        "frp": row.frp if row.frp is not None and not math.isnan(row.frp) else None,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            row.longitude if row.longitude is not None and not math.isnan(row.longitude) else None,
                            row.latitude if row.latitude is not None and not math.isnan(row.latitude) else None,
                        ],  # Longitude first, then latitude
                    },
                    "id": idx  # Add unique ID
                }
                for idx, row in enumerate(leona, start=1)
            ],
        }
        return response
    
    async def perimeter(self, session: AsyncSession) -> dict:
        statement = """
               SELECT *, ST_AsGeoJSON(perimeter) AS geom FROM xp.perimeter_1
        """
        perimeter = await session.exec(text(statement))
        perimeter = perimeter.all()
        response = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "obsdatelocal": (
                            row.start_time.astimezone(
                                ZoneInfo(
                                    tf.timezone_at(
                                        lng=json.loads(row.geom)["coordinates"][0][0][0],
                                        lat=json.loads(row.geom)["coordinates"][0][0][1]
                                    )
                                )
                            ).replace(tzinfo=None).isoformat()
                            if row.start_time and row.geom else None
                        ),
                        "obsdateutc": row.start_time.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat() if row.start_time else None,
                        "teamname_targetid": getattr(row, 'team_targetid', None),  # Replace with dynamic value if needed
                        "burnedarea": getattr(row, "burnedarea", 0),
                        "obsdirection_n": getattr(row, "obsdirection_n", 0),
                        "obsdirection_e": getattr(row, "obsdirection_e", 0),
                        "obsdirection_s": getattr(row, "obsdirection_s", 0),
                        "obsdirection_w": getattr(row, "obsdirection_w", 0),
                        "rateofspread_n": getattr(row, "rateofspread_n", 0),
                        "rateofspread_e": getattr(row, "rateofspread_e", 0),
                        "rateofspread_s": getattr(row, "rateofspread_s", 0),
                        "rateofspread_w": getattr(row, "rateofspread_w", 0),
                        "intensity": getattr(row, "intensity", 0)
                    },
                    "geometry": json.loads(row.geom) if row.geom else None,
                    "id": idx,  # Replace with dynamic ID if needed
                }
                for idx, row in enumerate(perimeter, start=1)
            ],
        }
        return response
        
    async def combined_geojson(self, session: AsyncSession) -> dict:
        # Query for leona data
        statement = """
            SELECT * FROM xp.point
        """
        events = await session.exec(text(statement))
        events = events.all()
        response = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {
                        "obsdatelocal":  (
                                row.time.astimezone(
                                ZoneInfo(
                                    tf.timezone_at(
                                        lng=row.longitude,
                                        lat=row.latitude
                                    )
                                )
                            ).replace(tzinfo=None).isoformat()
                            if row.time and row.longitude is not None and row.latitude is not None else None
                        ),
                        "obsdateutc": (
                            row.time.astimezone(ZoneInfo("UTC")).replace(tzinfo=None).isoformat()
                            if row.time else None
                        ),
                        "teamname_targetid": getattr(row, 'team_targetid', 0), #if hasattr(row, "group_id") and row.group_id else None,
                        "frp": row.frp if row.frp is not None and not math.isnan(row.frp) else None,
                    },
                    "geometry": {
                        "coordinates": [
                            row.longitude if row.longitude is not None and not math.isnan(row.longitude) else None,
                            row.latitude if row.latitude is not None and not math.isnan(row.latitude) else None,
                        ],
                        "type": "Point",
                    },
                    "id": idx  # Add unique ID
                }
                for idx, row in enumerate(events, start=1)
            ]
        }
        return response
    def csv_to_geojson(self,df: pd.DataFrame) -> dict:
        df.columns = df.columns.str.upper()
        df["TIME"]= pd.to_datetime(df["TIME"], errors="coerce", utc=True)
        df["obsdatelocal"] = df.apply(
            lambda row: row["TIME"].astimezone(
                ZoneInfo(
                    tf.timezone_at(
                        lng=row["LONGITUDE"],
                        lat=row["LATITUDE"]
                    )
                )
            ).replace(tzinfo=None).isoformat() if pd.notnull(row["TIME"]) else None,
            axis=1
        )
        df["TIME"]=df["TIME"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        features = []
        for idx, row in df.iterrows():
            feature = {
                "type": "Feature",
                "properties": {
                    "obsdatelocal": row["obsdatelocal"],
                    "obsdateutc": row["TIME"],
                    "teamname_targetid": row.get("TEAM_TARGID", 0),
                    "frp": row["FRP"] if pd.notnull(row["FRP"]) and not math.isnan(row["FRP"]) else None,
                },
                "geometry": {
                    "coordinates": [row["LONGITUDE"], row["LATITUDE"]],
                    "type": "Point",
                },
                "id": int(idx) + 1
            }
            features.append(feature)
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    def perimeter_csv_to_geojson(self, gdf) -> dict:    
        gdf.columns = gdf.columns.str.upper()

        # Convert PERIMETER column to geometry
        gdf['geometry'] = gdf['PERIMETER'].apply(lambda g: mapping(wkt.loads(g)) if pd.notnull(g) else None)

        # Parse START_TIME and convert to UTC
        gdf["START_TIME"] = pd.to_datetime(gdf["START_TIME"], errors="coerce", utc=True)

        # Calculate obsdatelocal
        def calculate_obsdatelocal(row):
            if pd.notnull(row["START_TIME"]) and row["geometry"]:
                coords = row["geometry"]["coordinates"][0][0]
                tz_name = tf.timezone_at(lng=coords[0], lat=coords[1])
                if tz_name:
                    return row["START_TIME"].astimezone(ZoneInfo(tz_name)).replace(tzinfo=None).isoformat()
            return None

        gdf["obsdatelocal"] = gdf.apply(calculate_obsdatelocal, axis=1)

        # Format START_TIME as ISO 8601
        gdf["START_TIME"] = gdf["START_TIME"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Generate GeoJSON features
        features = [
            {
                "type": "Feature",
                "properties": {
                    "obsdatelocal": row["obsdatelocal"],
                    "obsdateutc": row["START_TIME"],
                    "teamname_targetid": row.get("TEAM_TARGID", None),
                    "burnedarea": row.get("BURNEDAREA", 0),
                    "obsdirection_n": row.get("OBSDIRECTION_N", 0),
                    "obsdirection_e": row.get("OBSDIRECTION_E", 0),
                    "obsdirection_s": row.get("OBSDIRECTION_S", 0),
                    "obsdirection_w": row.get("OBSDIRECTION_W", 0),
                    "rateofspread_n": row.get("RATEOFSPREAD_N", 0),
                    "rateofspread_e": row.get("RATEOFSPREAD_E", 0),
                    "rateofspread_s": row.get("RATEOFSPREAD_S", 0),
                    "rateofspread_w": row.get("RATEOFSPREAD_W", 0),
                    "intensity": row.get("INTENSITY", 0),
                },
                "geometry": row["geometry"],
                "id": idx + 1
            }
            for idx, row in gdf.iterrows()
        ]

        return {
            "type": "FeatureCollection",
            "features": features
        }