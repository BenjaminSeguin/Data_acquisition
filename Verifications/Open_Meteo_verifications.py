import sqlite3
import os

# Check both databases
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
databases = [
    (os.path.join(base_dir, 'Database', 'weather_hourly.db'), 'weather_hourly', 'datetime'),
    (os.path.join(base_dir, 'Database', 'weather_daily.db'), 'weather_daily', 'date')
]

for db_path, table, time_col in databases:
    print(f"\nProcessing database {table}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Look for missing values
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {time_col} IS NULL OR city IS NULL")
    missing = cursor.fetchone()[0]
    if missing > 0:
        print(f"{missing} rows with missing {time_col} or city")
    else:
        print(f"No missing values")
    
    # Look for duplicates
    cursor.execute(f"""
        SELECT {time_col}, city, COUNT(*) as count
        FROM {table}
        GROUP BY {time_col}, city
        HAVING count > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"{len(duplicates)} duplicate entries")
        for dup in duplicates[:3]:
            print(f"{dup[1]} at {dup[0]}")
    else:
        print(f"No duplicates")
    
    # Count records per city
    print(f"\nRecord counts:")
    cursor.execute(f"SELECT city, COUNT(*) FROM {table} GROUP BY city")
    for city, count in cursor.fetchall():
        print(f"  {city}: {count:,} records")
    
    conn.close()
