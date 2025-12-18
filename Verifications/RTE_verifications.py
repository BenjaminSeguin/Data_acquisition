import sqlite3
import os

# Check RTE database
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
db_path = os.path.join(base_dir, 'Database', 'rte.db')
table = 'rte_data'
time_col = 'datetime'

print(f"Checking database: {db_path}")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Look for missing values
cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {time_col} IS NULL")
missing = cursor.fetchone()[0]
if missing > 0:
    print(f"{missing} rows with missing {time_col}")
else:
    print(f"No missing datetime values")

# Look for duplicates
cursor.execute(f"""
    SELECT {time_col}, COUNT(*) as count
    FROM {table}
    GROUP BY {time_col}
    HAVING count > 1
""")
duplicates = cursor.fetchall()
if duplicates:
    print(f"\n{len(duplicates)} duplicate entries found:")
    for dup in duplicates[:5]:
        print(f"  {dup[0]}: {dup[1]} occurrences")
    
    # Remove duplicates, keeping only the first occurrence
    print(f"\nRemoving duplicates (keeping first occurrence)...")
    cursor.execute(f"""
        DELETE FROM {table}
        WHERE rowid NOT IN (
            SELECT MIN(rowid)
            FROM {table}
            GROUP BY {time_col}
        )
    """)
    removed = cursor.rowcount
    conn.commit()
    print(f"Removed {removed} duplicate records")
else:
    print(f"No duplicates found")

# Total record count
cursor.execute(f"SELECT COUNT(*) FROM {table}")
total = cursor.fetchone()[0]
print(f"\nTotal records: {total:,}")

# Date range
cursor.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table}")
min_date, max_date = cursor.fetchone()
print(f"Date range: {min_date} to {max_date}")

conn.close()
