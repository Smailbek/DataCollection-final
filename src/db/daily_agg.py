import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("data/app.db")

def compute_daily_summary():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql_query("""
        SELECT
            date(ingested_at) AS summary_date,
            COUNT(*) AS total_rows,
            COUNT(DISTINCT icao24) AS unique_aircraft,
            AVG(velocity) AS avg_velocity,
            MAX(velocity) AS max_velocity,
            AVG(geo_altitude) AS avg_geo_altitude,
            MAX(geo_altitude) AS max_geo_altitude,
            SUM(CASE WHEN on_ground = 1 THEN 1 ELSE 0 END) AS on_ground_count
        FROM events
        GROUP BY date(ingested_at);
    """, conn)


    df.to_sql("daily_summary", conn, if_exists="replace", index=False)
    conn.close()

if __name__ == "__main__":
    compute_daily_summary()