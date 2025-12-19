OpenSky Airflow Kafka Pipeline
Project Overview

This project implements an end-to-end data pipeline for collecting, streaming, processing, and aggregating real-time aircraft data using the OpenSky Network API.

The pipeline is orchestrated with Apache Airflow, uses Kafka for streaming, and SQLite as the final storage layer.

Architecture
OpenSky API
    ↓
Kafka (opensky_raw_states topic)
    ↓
Airflow DAGs
    ↓
SQLite (events table)
    ↓
Daily Aggregation (daily_summary table)

Technologies Used

Python 3

Apache Airflow 2.9

Apache Kafka

Docker & Docker Compose

SQLite

OpenSky Network REST API

Data Source

API: https://opensky-network.org/api/states/all

Provides real-time aircraft state vectors:

ICAO24

Callsign

Country

Latitude / Longitude

Altitude

Velocity

Heading

Timestamp

Airflow DAGs
1. opensky_api_to_kafka

Schedule: every 2 minutes

Fetches aircraft state data from OpenSky API

Publishes raw JSON messages to Kafka topic opensky_raw_states

2. opensky_kafka_to_sqlite_hourly

Schedule: hourly

Consumes messages from Kafka

Cleans and normalizes the data

Stores records into SQLite table events

3. opensky_sqlite_daily_summary

Schedule: daily

Aggregates data from events

Computes daily statistics:

total events

number of countries

average / max velocity

average / max altitude

Stores results in daily_summary table

Database Schema
Table: events

Stores raw aircraft events.

Example fields:

icao24

callsign

origin_country

latitude

longitude

altitude

velocity

heading

event_time

Table: daily_summary

Stores aggregated daily metrics.

Example fields:

summary_date

total_events

unique_countries

avg_velocity

max_velocity

avg_altitude

max_altitude

How to Run the Project
1. Start Kafka and Airflow
docker compose up -d

2. Open Airflow UI

URL: http://localhost:8080

Trigger DAGs in order:

opensky_api_to_kafka

opensky_kafka_to_sqlite_hourly

opensky_sqlite_daily_summary

Verification

Data can be verified directly from SQLite:

import sqlite3

with sqlite3.connect("app.db") as conn:
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM events;")
    print(cur.fetchone())

    cur.execute("SELECT * FROM daily_summary ORDER BY summary_date DESC LIMIT 1;")
    print(cur.fetchone())

Result

Real-time aircraft data is successfully ingested

Data is streamed through Kafka

Processed and stored using Airflow

Daily analytics are computed and persisted

Conclusion

This project demonstrates a fully functional data engineering pipeline with:

API ingestion

Streaming

Batch processing

Data storage

Aggregation and analytics