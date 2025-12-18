import sqlite3
import subprocess
import sys
import os

def run_verification_script(script_path, script_name):
    """Run a verification script and capture output"""
    print("\n" + "=" * 80)
    print(f"Running {script_name}")
    print("=" * 80)
    
    if not os.path.exists(script_path):
        print(f"⚠️  Script not found: {script_path}")
        return
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        if result.returncode != 0:
            print(f"⚠️  Script exited with code {result.returncode}")
    except Exception as e:
        print(f"❌ Error running {script_name}: {e}")

def verify_final_database():
    """Verify final.db database (same logic as RTE_verifications.py)"""
    print("\n" + "=" * 80)
    print("Verifying final.db")
    print("=" * 80)
    
    db_path = 'Database/final.db'
    
    if not os.path.exists(db_path):
        print(f"⚠️  Database not found: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            print("⚠️  No tables found in database")
            conn.close()
            return
        
        print(f"\nTables found: {[t[0] for t in tables]}")
        
        # Check each table
        for (table_name,) in tables:
            print(f"\n--- Table: {table_name} ---")
            
            # Get columns
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            col_names = [col[1] for col in columns]
            
            # Try to identify time column
            time_col = None
            for col in ['datetime', 'date', 'time']:
                if col in col_names:
                    time_col = col
                    break
            
            if not time_col:
                print(f"⚠️  No datetime/date column found in {table_name}")
                # Just show record count
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                total = cursor.fetchone()[0]
                print(f"Total records: {total:,}")
                continue
            
            # Check if city column exists
            has_city = 'city' in col_names
            
            # Look for missing values in time column
            if has_city:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {time_col} IS NULL OR city IS NULL")
                missing = cursor.fetchone()[0]
                if missing > 0:
                    print(f"⚠️  {missing} rows with missing {time_col} or city")
                else:
                    print(f"✓ No missing {time_col} or city values")
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {time_col} IS NULL")
                missing = cursor.fetchone()[0]
                if missing > 0:
                    print(f"⚠️  {missing} rows with missing {time_col}")
                else:
                    print(f"✓ No missing {time_col} values")
            
            # Look for duplicates (datetime + city if city column exists)
            if has_city:
                cursor.execute(f"""
                    SELECT {time_col}, city, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY {time_col}, city
                    HAVING count > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"⚠️  {len(duplicates)} duplicate entries found:")
                    for dup in duplicates[:5]:
                        print(f"  {dup[1]} at {dup[0]}: {dup[2]} occurrences")
                    
                    # Remove duplicates, keeping only the first occurrence
                    print(f"\nRemoving duplicates (keeping first occurrence)...")
                    cursor.execute(f"""
                        DELETE FROM {table_name}
                        WHERE rowid NOT IN (
                            SELECT MIN(rowid)
                            FROM {table_name}
                            GROUP BY {time_col}, city
                        )
                    """)
                    removed = cursor.rowcount
                    conn.commit()
                    print(f"✓ Removed {removed} duplicate records")
                else:
                    print(f"✓ No duplicates found")
            else:
                cursor.execute(f"""
                    SELECT {time_col}, COUNT(*) as count
                    FROM {table_name}
                    GROUP BY {time_col}
                    HAVING count > 1
                """)
                duplicates = cursor.fetchall()
                if duplicates:
                    print(f"⚠️  {len(duplicates)} duplicate entries found:")
                    for dup in duplicates[:5]:
                        print(f"  {dup[0]}: {dup[1]} occurrences")
                    
                    # Remove duplicates, keeping only the first occurrence
                    print(f"\nRemoving duplicates (keeping first occurrence)...")
                    cursor.execute(f"""
                        DELETE FROM {table_name}
                        WHERE rowid NOT IN (
                            SELECT MIN(rowid)
                            FROM {table_name}
                            GROUP BY {time_col}
                        )
                    """)
                    removed = cursor.rowcount
                    conn.commit()
                    print(f"✓ Removed {removed} duplicate records")
                else:
                    print(f"✓ No duplicates found")
            
            # Total record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cursor.fetchone()[0]
            print(f"Total records: {total:,}")
            
            # Date range
            try:
                cursor.execute(f"SELECT MIN({time_col}), MAX({time_col}) FROM {table_name}")
                min_date, max_date = cursor.fetchone()
                print(f"Date range: {min_date} to {max_date}")
            except:
                pass
        
        conn.close()
        print("\n" + "=" * 80)
        print("final.db verification complete!")
        print("=" * 80)
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

def main():
    print("\n" + "=" * 80)
    print("COMPREHENSIVE DATABASE VERIFICATION")
    print("=" * 80)
    
    # Run Open_Meteo_verifications.py
    run_verification_script(
        'Verifications/Open_Meteo_verifications.py',
        'Open_Meteo_verifications.py'
    )
    
    # Run RTE_verifications.py
    run_verification_script(
        'Verifications/RTE_verifications.py',
        'RTE_verifications.py'
    )
    
    # Run transparency_verifications.py
    run_verification_script(
        'Verifications/transparency_verifications.py',
        'transparency_verifications.py'
    )
    
    # Verify final.db
    verify_final_database()
    
    print("\n" + "=" * 80)
    print("ALL VERIFICATIONS COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
