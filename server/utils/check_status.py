from sqlmodel import Session, select
from server.models.models import StoreStatus,BusinessHours
from server.config.db import engine

def check_store_status_data():
    with Session(engine) as session:
        rows = session.exec(select(BusinessHours)).all()
        for row in rows:
            print(f"ID={row.id}, Store={row.store_id}, "
                  f"Timestamp={row.timestamp_utc}, Status={row.status}")

if __name__ == "__main__":
    check_store_status_data()
