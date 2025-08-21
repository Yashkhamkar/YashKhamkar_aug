import time
from datetime import datetime, timedelta
from pytz import timezone as pytz_timezone, UTC
from sqlmodel import select, Session, func
from server.models.models import Store, BusinessHours, StoreStatus, StoreStatusEnum
import csv, os


def get_max_timestamp(session: Session) -> datetime:
    max_ts = session.exec(select(func.max(StoreStatus.timestamp_utc))).first()
    return max_ts or datetime.now(UTC)


def is_in_business_hours(
    store_id: str, timestamp_utc: datetime, session: Session
) -> bool:
    store = session.get(Store, store_id)
    tz_str = store.timezone_str if store and store.timezone_str else "America/Chicago"
    tz = pytz_timezone(tz_str)
    local_time = timestamp_utc.astimezone(tz)
    day = local_time.weekday()
    current_time = local_time.time()

    bh = session.exec(
        select(BusinessHours)
        .where(BusinessHours.store_id == store_id)
        .where(BusinessHours.day_of_week == day)
    ).first()

    if bh is None:
        return True  # open 24/7 if no business hours data
    return bh.start_time_local <= current_time <= bh.end_time_local


def calculate_uptime_downtime(
    store_id: str, start_time: datetime, end_time: datetime, session: Session
):
    logs = session.exec(
        select(StoreStatus)
        .where(StoreStatus.store_id == store_id)
        .where(StoreStatus.timestamp_utc >= start_time)
        .where(StoreStatus.timestamp_utc <= end_time)
        .order_by(StoreStatus.timestamp_utc)
    ).all()

    uptime_minutes = downtime_minutes = 0

    if not logs:
        downtime_minutes = (end_time - start_time).total_seconds() / 60
        return uptime_minutes, downtime_minutes

    prev_ts = start_time
    prev_status = logs[0].status
    for log in logs:
        if is_in_business_hours(store_id, prev_ts, session):
            delta_minutes = (log.timestamp_utc - prev_ts).total_seconds() / 60
            if prev_status == StoreStatusEnum.ACTIVE:
                uptime_minutes += delta_minutes
            else:
                downtime_minutes += delta_minutes
        prev_ts = log.timestamp_utc
        prev_status = log.status

    if is_in_business_hours(store_id, prev_ts, session):
        delta_minutes = (end_time - prev_ts).total_seconds() / 60
        if prev_status == StoreStatusEnum.ACTIVE:
            uptime_minutes += delta_minutes
        else:
            downtime_minutes += delta_minutes

    return round(uptime_minutes, 2), round(downtime_minutes, 2)


def generate_report(session: Session, limit: int = 200):
    start_time_overall = time.time()
    max_ts = get_max_timestamp(session)
    csv_data = []

    stores = session.exec(select(Store).limit(limit)).all()

    for idx, store in enumerate(stores, 1):
        start_hour = max_ts - timedelta(hours=1)
        uptime_h, downtime_h = calculate_uptime_downtime(
            store.store_id, start_hour, max_ts, session
        )

        start_day = max_ts - timedelta(days=1)
        uptime_d, downtime_d = calculate_uptime_downtime(
            store.store_id, start_day, max_ts, session
        )

        start_week = max_ts - timedelta(days=7)
        uptime_w, downtime_w = calculate_uptime_downtime(
            store.store_id, start_week, max_ts, session
        )

        csv_data.append(
            {
                "store_id": store.store_id,
                "uptime_last_hour": uptime_h,
                "downtime_last_hour": downtime_h,
                "uptime_last_day": round(uptime_d / 60, 2),
                "downtime_last_day": round(downtime_d / 60, 2),
                "uptime_last_week": round(uptime_w / 60, 2),
                "downtime_last_week": round(downtime_w / 60, 2),
            }
        )
        print(f"{idx} store done")

    print(f"Total time taken: {round(time.time() - start_time_overall, 2)} seconds")
    return csv_data


def save_csv_report(report_id: str, csv_data, folder="reports"):
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"{report_id}.csv")
    with open(file_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_data[0].keys())
        writer.writeheader()
        writer.writerows(csv_data)
    return file_path
