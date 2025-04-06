from datetime import datetime
from typing import Optional
from geoalchemy2 import Geometry
from pydantic import Field
from sqlmodel import SQLModel


# class Perimeter(SQLModel, table=True):
#     __tablename__ = "perimeter"
#     __table_args__ = {"schema": "xp"}

#     start_time: Optional[datetime] = Field(Nullable=False,primary_key=True)
#     last_time: Optional[datetime] = Field(Nullable=False)
#     perimeter: Optional[str] = Field(default=None, sa_column=Geometry("POLYGON", srid=4326))


# class LEOPOINT(SQLModel, table=True):
#     __tablename__ = "leo_point"
#     __table_args__ = {"schema": "xp"}

#     time: Optional[datetime] = Field(Nullable=False,primary_key=True)
#     latitude: Optional[float] = Field(Nullable=False)
#     longitude: Optional[float] = Field(Nullable=False)
#     frp: Optional[float] = Field(Nullable=False)

# class GEOPOINT(SQLModel, table=True):
#     __tablename__ = "geo_point"
#     __table_args__ = {"schema": "xp"}

#     event_id: Optional[int] = Field(Nullable=False, primary_key=True)
#     timeline: Optional[str] = Field(Nullable=False)
#     group_id: Optional[int] = Field(Nullable=False)
#     time: Optional[datetime] = Field(Nullable=False)
#     latitude: Optional[float] = Field(Nullable=False)
#     longitude: Optional[float] = Field(Nullable=False)
#     frp: Optional[float] = Field(Nullable=False)

