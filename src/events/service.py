import json
from sqlmodel.ext.asyncio.session import AsyncSession
from geoalchemy2.functions import ST_AsGeoJSON
from sqlalchemy import select
from sqlmodel import text
import math
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
                        "obsdatelocal": row.time.isoformat() if row.time else None,
                        "obsdateutc": row.time.isoformat() if row.time else None,
                        "frp": row.frp,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [row.longitude, row.latitude]  
                    },
                    "id": idx  
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
                        "obsdatelocal": row.time.isoformat() if row.time else None,
                        "obsdateutc": row.time.isoformat() if row.time else None,
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
                        "obsdatelocal": row.start_time.isoformat() if row.start_time else None,
                        "obsdateutc": row.last_time.isoformat() if row.last_time else None,
                        "teamname_targetid": f"Mayday_{getattr(row, 'targetid', None)}",  # Replace with dynamic value if needed
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
                        "obsdatelocal": row.time.isoformat() if row.time else None,
                        "obsdateutc": row.time.isoformat() if row.time else None,
                        "teamname_targetid": f"Mayday_{getattr(row, "targetID", 0)}", #if hasattr(row, "group_id") and row.group_id else None,
                        "frp": row.frp if row.frp is not None and not math.isnan(row.frp) else None,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [
                            row.longitude if row.longitude is not None and not math.isnan(row.longitude) else None,
                            row.latitude if row.latitude is not None and not math.isnan(row.latitude) else None,
                        ],
                    },
                    "id": idx  # Add unique ID
                }
                for idx, row in enumerate(events, start=1)
            ]
        }
        return response