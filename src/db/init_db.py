import sqlite3
from pathlib import Path

DB_PATH = Path("data/app.db")

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ingested_at TEXT NOT NULL,
        api_time INTEGER NOT NULL,
        icao24 TEXT,
        callsign TEXT,
        origin_country TEXT,
        longitude REAL,
        latitude REAL,
        baro_altitude REAL,
        geo_altitude REAL,
        velocity REAL,
        true_track REAL,
        vertical_rate REAL,
        on_ground INTEGER,
        position_source INTEGER
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_summary (
        summary_date TEXT PRIMARY KEY,
        total_rows INTEGER,
        unique_aircraft INTEGER,
        avg_velocity REAL,
        max_velocity REAL,
        avg_geo_altitude REAL,
        max_geo_altitude REAL,
        on_ground_count INTEGER
    );
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()
