import sqlite3
import pandas as pd
import os

# Check transparency database
script_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = os.path.dirname(script_dir)
db_path = os.path.join(base_dir, 'Database', 'entsoe.db')
table = 'entsoe_data'
time_col = 'datetime'

print(f"Checking database: {db_path}")
print("=" * 60)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"\nTables in database: {[t[0] for t in tables]}")

    # Get column names
    cursor.execute(f"PRAGMA table_info({table})")
    columns = cursor.fetchall()
    print(f"\nColumns in '{table}' table:")
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")

    # Look for missing values in date column
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {time_col} IS NULL")
    missing = cursor.fetchone()[0]
    if missing > 0:
        print(f"\n⚠️  {missing} rows with missing {time_col}")
    else:
        print(f"\n✓ No missing datetime values")

    # Look for duplicates
    cursor.execute(f"""
        SELECT {time_col}, COUNT(*) as count
        FROM {table}
        GROUP BY {time_col}
        HAVING count > 1
    """)
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"\n⚠️  {len(duplicates)} duplicate entries found:")
        for dup in duplicates[:5]:
            print(f"  {dup[0]}: {dup[1]} occurrences")
        if len(duplicates) > 5:
            print(f"  ... and {len(duplicates) - 5} more")
    else:
        print(f"✓ No duplicates found")

    # Total record count
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    total = cursor.fetchone()[0]
    print(f"\n✓ Total records: {total:,}")

    # Date range
    cursor.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table}")
    min_date, max_date = cursor.fetchone()
    print(f"✓ Date range: {min_date} to {max_date}")

    # Check for NULL values in data columns
    print(f"\nChecking for NULL values in data columns:")
    for col in columns:
        col_name = col[1]
        if col_name != time_col:
            cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE `{col_name}` IS NULL")
            null_count = cursor.fetchone()[0]
            if null_count > 0:
                print(f"  ⚠️  {col_name}: {null_count} NULL values ({null_count/total*100:.1f}%)")
            else:
                print(f"  ✓ {col_name}: No NULL values")

    # Sample of first few rows
    print(f"\nFirst 5 rows preview:")
    df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5", conn)
    print(df.to_string())

    # Statistics on numeric columns
    print(f"\nBasic statistics:")
    numeric_cols = [col[1] for col in columns if col[1] != time_col]
    if numeric_cols:
        stats_query = f"SELECT * FROM {table}"
        df_full = pd.read_sql_query(stats_query, conn)
        print(df_full[numeric_cols].describe())

    conn.close()
    print("\n" + "=" * 60)
    print("Verification complete!")

except sqlite3.Error as e:
    print(f"\n❌ Database error: {e}")
except Exception as e:
    print(f"\n❌ Error: {e}")