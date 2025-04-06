from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class Geometry(BaseModel):
    type: str 
    coordinates: List

class FeatureProperties(BaseModel):
    OBJECTID: int
    TargetNumber: int
    NotificationNumber: int
    StartTime: Optional[int]
    EndTime: Optional[int]

class Feature(BaseModel):
    type: str
    id: int
    geometry: Geometry
    properties: Optional[FeatureProperties] = None
