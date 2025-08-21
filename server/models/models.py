from sqlmodel import Column, Integer, SQLModel, Field
from typing import Optional
from datetime import datetime, time
from enum import IntEnum


class Store(SQLModel, table=True):
    __tablename__ = "store"
    store_id: str = Field(primary_key=True, index=True)
    timezone_str: Optional[str] = Field(default="America/Chicago")


class BusinessHours(SQLModel, table=True):
    __tablename__ = "business_hours"
    id: Optional[int] = Field(default=None, primary_key=True)
    store_id: str = Field(foreign_key="store.store_id", index=True)
    day_of_week: int
    start_time_local: time
    end_time_local: time


# 👇 Use IntEnum instead of str Enum
class StoreStatusEnum(IntEnum):
    INACTIVE = 0
    ACTIVE = 1


class StoreStatus(SQLModel, table=True):
    __tablename__ = "store_status_log"
    id: Optional[int] = Field(default=None, primary_key=True)
    store_id: str = Field(foreign_key="store.store_id", index=True)
    timestamp_utc: datetime
    status: StoreStatusEnum = Field(sa_column=Column(Integer))  

class ReportStatusEnum(IntEnum):
    PENDING = 0
    COMPLETED = 1


class StoreReport(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    store_id: Optional[str] = Field(
        default=None, foreign_key="store.store_id", index=True
    )
    status: ReportStatusEnum = Field(default=ReportStatusEnum.PENDING)
    report_url: Optional[str] = Field(default=None)
