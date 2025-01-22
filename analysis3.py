import pandas as pd
df = pd.read_csv("2022_place_canvas_history.csv")
df.head()
def query_data(df, start, end):
    # Filter data between the given timestamps
    filtered_df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
    
    # Find the most placed color
    most_placed_color = filtered_df['pixel_color'].value_counts().idxmax()
    most_placed_color_count = filtered_df['pixel_color'].value_counts().max()
    
    most_placed_location = filtered_df['coordinate'].value_counts().idxmax()
    most_placed_location_count = filtered_df['coordinate'].value_counts().max()
    
    return (most_placed_color, most_placed_color_count), (most_placed_location, most_placed_location_count)


# MAIN
start = "2022-04-03 23:30:00"
end = "2022-04-04 0:00:00"

start_time = time.perf_counter_ns()
result = query_data(df, start, end)
data_file = "2022_place_canvas_history.csv"

end_time = time.perf_counter_ns()

print(f"Most placed color: {result[0][0]} with count: {result[0][1]}")
print(f"Most placed location: {result[1][0]} with count: {result[1][1]}")

print(f"Execution time: {(end_time - start_time)/ 1e9} seconds")
