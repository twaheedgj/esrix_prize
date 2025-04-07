from io import StringIO
from fastapi.responses import JSONResponse
import pandas as pd
from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import APIRouter, Depends, File, UploadFile
from routes.models import Feature
from typing import List
from events.service import Service
import geopandas as gpd
from db.main import get_session
xprize_router = APIRouter()
service = Service()

@xprize_router.post("/")
async def read_root(features: List[Feature]):
    # Use model_dump() to serialize each Feature object
    serialized_features = [feature.model_dump() for feature in features]
    return {"message": serialized_features}
@xprize_router.get("/leo")
async def get_firms(session :AsyncSession= Depends(get_session)):
    
    response = await service.leona(session)
    return response

@xprize_router.get("/geo")
async def get_geo(session :AsyncSession= Depends(get_session)):
    response = await service.geoevents(session)
    return response

@xprize_router.get("/perimeter")
async def get_perimeter(session :AsyncSession= Depends(get_session)):
    response = await service.perimeter(session)
    return response

@xprize_router.get("/combined")
async def get_combined(session :AsyncSession= Depends(get_session)):
    response = await service.combined_geojson(session)
    return response

@xprize_router.post("/point_json")
async def convert_csv_to_geojson(file: UploadFile = File(...)):
    # Read the file contents
    contents = await file.read()

    # Convert bytes into a CSV (since it's uploaded as a file)
    csv_file = StringIO(contents.decode("utf-8"))
    df = pd.read_csv(csv_file)

    # Convert to GeoJSON
    geojson = service.csv_to_geojson(df)

    # Return the GeoJSON as a JSON response
    return JSONResponse(content=geojson)

@xprize_router.post("/perimeter_json")
async def convert_perimeter_csv(file: UploadFile = File(...)):
    contents = await file.read()
    csv_data = contents.decode("utf-8")
    df = pd.read_csv(StringIO(csv_data))
    try:
        geojson = service.perimeter_csv_to_geojson(df)
        return JSONResponse(content=geojson)
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=400)