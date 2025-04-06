from sqlmodel.ext.asyncio.session import AsyncSession
from fastapi import APIRouter, Depends
from routes.models import Feature
from typing import List
from events.service import Service
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