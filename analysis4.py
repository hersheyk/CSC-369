import polars as pl
import time



def query_data(df, start, end):

    start = pl.datetime(start)
    end = pl.datetime(end)
    
    filtered_df = df.filter((pl.col('timestamp') >= start) & (pl.col('timestamp') <= end))
    
    most_placed_color = filtered_df.groupby('pixel_color').agg(pl.count().alias('count')).sort('count', reverse=True).select(pl.col('pixel_color').first()).to_numpy()[0][0]
    most_placed_color_count = filtered_df.groupby('pixel_color').agg(pl.count().alias('count')).sort('count', reverse=True).select(pl.col('count').first()).to_numpy()[0][0]
    
    most_placed_location = filtered_df.groupby('coordinate').agg(pl.count().alias('count')).sort('count', reverse=True).select(pl.col('coordinate').first()).to_numpy()[0][0]
    most_placed_location_count = filtered_df.groupby('coordinate').agg(pl.count().alias('count')).sort('count', reverse=True).select(pl.col('count').first()).to_numpy()[0][0]
    
    return (most_placed_color, most_placed_color_count), (most_placed_location, most_placed_location_count)

#MAIN
data_file = "2022_place_canvas_history.csv"
df = pl.read_csv(data_file)
start_time = time.perf_counter_ns()


start = "2022-04-03 23:30:00"
end = "2022-04-04 0:00:00"

result = query_data(df, start, end)

end_time = time.perf_counter_ns()

print(f"Most placed color: {result[0][0]} with count: {result[0][1]}")
print(f"Most placed location: {result[1][0]} with count: {result[1][1]}")

print(f"Execution time: {(end_time - start_time) / 1e9} seconds")
