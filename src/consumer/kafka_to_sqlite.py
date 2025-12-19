import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer

TOPIC = "opensky_raw_states"
KAFKA_BOOTSTRAP = "kafka:9092"
DB_PATH = Path("data/app.db")

def normalize_opensky_message(msg_json: dict) -> pd.DataFrame:
    ingested_at = msg_json.get("ingested_at")
    payload = msg_json.get("payload", {})
    api_time = payload.get("time")
    states = payload.get("states") or []


    rows = []
    for s in states:
        row = {
            "ingested_at": ingested_at,
            "api_time": api_time,
            "icao24": s[0] if len(s) > 0 else None,
            "callsign": (s[1].strip() if len(s) > 1 and s[1] else None),
            "origin_country": s[2] if len(s) > 2 else None,
            "longitude": s[5] if len(s) > 5 else None,
            "latitude": s[6] if len(s) > 6 else None,
            "baro_altitude": s[7] if len(s) > 7 else None,
            "on_ground": int(bool(s[8])) if len(s) > 8 and s[8] is not None else None,
            "velocity": s[9] if len(s) > 9 else None,
            "true_track": s[10] if len(s) > 10 else None,
            "vertical_rate": s[11] if len(s) > 11 else None,
            "geo_altitude": s[13] if len(s) > 13 else None,
            "position_source": s[16] if len(s) > 16 else None,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    df = df[df["icao24"].notna()]

    for col in ["longitude","latitude","baro_altitude","geo_altitude","velocity","true_track","vertical_rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["api_time"] = pd.to_numeric(df["api_time"], errors="coerce").astype("Int64")

    df = df[df["velocity"].isna() | ((df["velocity"] >= 0) & (df["velocity"] <= 400))]  # м/с, грубо
    df = df[df["geo_altitude"].isna() | ((df["geo_altitude"] >= -500) & (df["geo_altitude"] <= 20000))]

    return df

def insert_events(df: pd.DataFrame):
    if df.empty:
        return

    conn = sqlite3.connect(DB_PATH)
    df.to_sql("events", conn, if_exists="append", index=False)
    conn.close()

def consume_and_load(group_id="opensky_hourly_loader", max_messages=50, max_seconds=30):
    import time

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda v: v.decode("utf-8"),
        request_timeout_ms=30000,
        session_timeout_ms=10000,
    )

    start = time.time()
    processed = 0

    try:
        consumer.poll(timeout_ms=1000)

        while processed < max_messages and (time.time() - start) < max_seconds:
            batch = consumer.poll(timeout_ms=2000, max_records=20)
            if not batch:
                continue

            for tp, messages in batch.items():
                for m in messages:
                    msg_json = json.loads(m.value)
                    df = normalize_opensky_message(msg_json)
                    insert_events(df)
                    processed += 1
                    if processed >= max_messages:
                        break
    finally:
        consumer.close()

    print(f"Done. Processed {processed} kafka messages.")


if __name__ == "__main__":
    consume_and_load(group_id="opensky_debug_2", max_messages=10, max_seconds=30)