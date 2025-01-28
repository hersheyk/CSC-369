import csv
from datetime import datetime
import time
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl
import duckdb

def query_data(db_name, start, end):

    conn = duckdb.connect(db_name)

    most_distinct_colors = conn.execute(f'''
        SELECT pixel_color, COUNT(DISTINCT user_id) AS distinct_count
        FROM "{db_name}"
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY pixel_color
        ORDER BY distinct_count DESC
        LIMIT 5
    ''', (start, end)).fetchall()

    first_time_users = conn.execute(f'''
        SELECT COUNT(DISTINCT user_id) AS first_time_user_count
        FROM "{db_name}"
        WHERE timestamp BETWEEN ? AND ?
          AND user_id NOT IN (
              SELECT DISTINCT user_id
              FROM "{db_name}"
              WHERE timestamp < ?
          )
    ''', (start, end, start)).fetchone()

    pixel_percentiles = conn.execute(f'''
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY pixel_count) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixel_count) AS p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY pixel_count) AS p90,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixel_count) AS p99
        FROM (
            SELECT user_id, COUNT(*) AS pixel_count
            FROM "{db_name}"
            WHERE timestamp BETWEEN ? AND ?
            GROUP BY user_id
        ) AS pixel_counts
    ''', (start, end)).fetchone()

    avg_session_length = conn.execute(f'''
        WITH sessions AS (
            SELECT user_id,
                  CAST(timestamp AS TIMESTAMP) AS timestamp,
                  LEAD(CAST(timestamp AS TIMESTAMP)) OVER (PARTITION BY user_id ORDER BY timestamp) AS next_timestamp,
                  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) AS session_id
            FROM "{db_name}"
            WHERE timestamp BETWEEN ? AND ?
        ),
        session_durations AS (
            SELECT user_id,
                  session_id,
                  EXTRACT(EPOCH FROM (next_timestamp - timestamp)) AS session_duration
            FROM sessions
            WHERE EXTRACT(EPOCH FROM (next_timestamp - timestamp)) <= 900  -- 15 minutes
        )
        SELECT AVG(session_duration) AS avg_session_length
        FROM session_durations
        GROUP BY user_id
        HAVING COUNT(session_id) > 1
    ''', (start, end)).fetchone()




    conn.close()
    return most_distinct_colors, avg_session_length, pixel_percentiles, first_time_users

# MAIN
name = 'rplace'

start = "2022-04-01 12:00:00"
end = "2022-04-01 18:00:00"

start_time = time.perf_counter_ns()

result = query_data(name, start, end)

end_time = time.perf_counter_ns()

print(f"Most placed colors: {result[0]}")
print(f"Average Session Length: {result[1][0]} seconds")
print(f"Pixel Percentiles: {result[2]}")
print(f"First-Time Users: {result[3][0]}")
print(f"Execution time: {(end_time - start_time)/ 1e9} seconds")