import csv
from datetime import datetime
import time
import pandas as pd
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl
import duckdb

csv_file = "/content/drive/MyDrive/2022_place_canvas_history.csv"
parquet_file = "2022pyarrow.parquet"

DATESTRING_FORMAT = "%Y-%m-%d %H:%M:%S"
BLOCK_SIZE = 100_000_000

read_options = pv.ReadOptions(block_size=BLOCK_SIZE)
csv_reader = pv.open_csv(csv_file, read_options=read_options)

parquet_writer = None
user_id_map = {}
next_user_id = 0

try:
    for record_batch in csv_reader:
        print(f"Processing batch with {record_batch.num_rows} rows...")

        df = pl.from_arrow(record_batch)

        df = df.with_columns(
            pl.col("timestamp")
            .str.replace(r" UTC$", "")
            .str.strptime(
                pl.Datetime,
                format="%Y-%m-%d %H:%M:%S%.f",
                strict=False
            )
            .alias("timestamp")
        )

        df = (
            df.filter(
                pl.col("coordinate").str.count_matches(",") == 1
            )
            .with_columns(
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_0")
                .cast(pl.Int64)
                .alias("x"),
                pl.col("coordinate")
                .str.split_exact(",", 1)
                .struct.field("field_1")
                .cast(pl.Int64)
                .alias("y"),
            )
            .drop("coordinate")
            )

        for uid in df["user_id"].unique():
            if uid not in user_id_map:
                user_id_map[uid] = len(user_id_map)

        # Replace user_id column with mapped values
        df = df.with_columns(
        pl.col("user_id").replace(user_id_map).alias("user_id")
          )
        table = df.to_arrow()

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                parquet_file,
                schema=table.schema,
                compression="zstd"
            )
        parquet_writer.write_table(table)

finally:
    if parquet_writer:
        parquet_writer.close()

print(f"Successfully converted {csv_file} to {parquet_file}")


parquet_file = "/content/drive/MyDrive/2022pyarrow.parquet"
def database(db_name, data):
    conn = duckdb.connect(db_name)

    conn.execute(f'''
        CREATE TABLE IF NOT EXISTS "{db_name}" (
            timestamp TEXT,
            user_id TEXT,
            pixel_color TEXT,
            coordinate_x INTEGER,
            coordinate_y INTEGER
        )
    ''')

    conn.execute(f'''
            INSERT INTO "{db_name}"
            SELECT * FROM read_parquet(?);
        ''', [data])

    conn.close()

database("rplace", parquet_file)
