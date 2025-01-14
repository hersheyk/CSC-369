import sqlite3
import csv
from datetime import datetime
import time

def database(db_name, data):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # Create a table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS canvas_data (
            timestamp TEXT,
            user_id TEXT,
            pixel_color TEXT,
            coordinate_x INTEGER,
            coordinate_y INTEGER
        )
    ''')
    conn.commit()
    with open(data, 'r') as file:
        opened_file = csv.reader(file)
        next(opened_file)  
        data = []
        for row in opened_file:
            if len(row) == 4:
                timestamp, user_id, pixel, coordinate = row
                try:
                    coordinate_x, coordinate_y = map(int, coordinate.strip('"').split(','))
                    data.append((timestamp, user_id, pixel, coordinate_x, coordinate_y))
                except ValueError:
                    print(f"coordinate line error: {row}")
        
        cursor.executemany('''
            INSERT INTO canvas_data (timestamp, user_id, pixel_color, coordinate_x, coordinate_y) 
            VALUES (?, ?, ?, ?, ?)
        ''', data)
        conn.commit()
    conn.close()

def query_data(db_name, start, end):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT pixel_color, COUNT(pixel_color) AS color_count
        FROM canvas_data
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY pixel_color
        ORDER BY color_count DESC
        LIMIT 1
    ''', (start, end))
    most_placed_color = cursor.fetchone()

    cursor.execute('''
        SELECT coordinate_x || ',' || coordinate_y AS coordinate, COUNT(*) AS location_count
        FROM canvas_data
        WHERE timestamp BETWEEN ? AND ?
        GROUP BY coordinate
        ORDER BY location_count DESC
        LIMIT 1
    ''', (start, end))
    most_placed_location = cursor.fetchone()

    conn.close()
    return most_placed_color, most_placed_location

# MAIN
name = 'canvas_data.db'

start = "2022-04-03 23:00:00"
end = "2022-04-04 2:00:00"

start_time = time.perf_counter_ns()

database(name,"2022_place_canvas_history.csv")

result = query_data(name, start, end)

end_time = time.perf_counter_ns()

print("Most placed color:", result[0])
print("Most placed location:", result[1])
print("Time taken:", (end_time - start_time) / 1e9, "seconds")