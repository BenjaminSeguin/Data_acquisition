"""
RTE Data Processor - Extract .xls files to SQLite database

Usage:
    rte_db = RTEDatabase()
    rte_db.create_database_from_folder('RTE_daily_data', 'rte_energy.db')
"""

import os
import glob
import sqlite3
import pandas as pd
from datetime import datetime


class RTEDataExtractor:
    """Reads and parses RTE .xls files (TSV format)"""
    
    @staticmethod
    def read_file(filepath):
        try:
            df = pd.read_csv(filepath, sep='\t', encoding='ISO-8859-1', index_col=False)
            return df
        except Exception as e:
            print(f"Error reading {filepath}: {e}")
            return None
    
    @staticmethod
    def validate_file(df):
        if df is None or df.empty:
            return False
        required_columns = ['Date', 'Heures', 'Consommation']
        return all(col in df.columns for col in required_columns)


class RTEDataProcessor:
    """Converts DataFrames to structured records"""
    
    @staticmethod
    def extract_records(df):
        records = []
        
        if 'Périmètre' not in df.columns:
            df = df.reset_index()
        
        df_filtered = df[df['Consommation'].notna()].copy()
        
        for _, row in df_filtered.iterrows():
            try:
                date_str = str(row['Date']).strip()
                time_str = str(row['Heures']).strip()
                
                if not time_str.endswith(':00'):
                    continue
                
                datetime_obj = pd.to_datetime(f"{date_str} {time_str}", format='%Y-%m-%d %H:%M')
                
                record = {
                    'datetime': datetime_obj.strftime('%Y-%m-%d %H:%M:%S'),
                    'date': date_str,
                    'time': time_str,
                    'perimetre': RTEDataProcessor._safe_str(row.get('Périmètre', '')),
                    'nature': RTEDataProcessor._safe_str(row.get('Nature', '')),
                    'consommation': RTEDataProcessor._safe_float(row.get('Consommation')),
                    'prevision_j1': RTEDataProcessor._safe_float(row.get('Prévision J-1')),
                    'prevision_j': RTEDataProcessor._safe_float(row.get('Prévision J')),
                    'nucleaire': RTEDataProcessor._safe_float(row.get('Nucléaire')),
                    'eolien': RTEDataProcessor._safe_float(row.get('Eolien')),
                    'solaire': RTEDataProcessor._safe_float(row.get('Solaire')),
                    'hydraulique': RTEDataProcessor._safe_float(row.get('Hydraulique')),
                    'gaz': RTEDataProcessor._safe_float(row.get('Gaz')),
                    'charbon': RTEDataProcessor._safe_float(row.get('Charbon')),
                    'fioul': RTEDataProcessor._safe_float(row.get('Fioul')),
                    'bioenergies': RTEDataProcessor._safe_float(row.get('Bioénergies')),
                    'pompage': RTEDataProcessor._safe_float(row.get('Pompage')),
                    'stockage_batterie': RTEDataProcessor._safe_float(row.get(' Stockage batterie')),
                    'destockage_batterie': RTEDataProcessor._safe_float(row.get('Déstockage batterie')),
                    'ech_physiques': RTEDataProcessor._safe_float(row.get('Ech. physiques')),
                    'ech_comm_angleterre': RTEDataProcessor._safe_float(row.get('Ech. comm. Angleterre')),
                    'ech_comm_espagne': RTEDataProcessor._safe_float(row.get('Ech. comm. Espagne')),
                    'ech_comm_italie': RTEDataProcessor._safe_float(row.get('Ech. comm. Italie')),
                    'ech_comm_suisse': RTEDataProcessor._safe_float(row.get('Ech. comm. Suisse')),
                    'ech_comm_allemagne_belgique': RTEDataProcessor._safe_float(row.get('Ech. comm. Allemagne-Belgique')),
                    'taux_co2': RTEDataProcessor._safe_float(row.get('Taux de Co2')),
                    'fioul_tac': RTEDataProcessor._safe_float(row.get('Fioul - TAC')),
                    'fioul_cogen': RTEDataProcessor._safe_float(row.get('Fioul - Cogén.')),
                    'fioul_autres': RTEDataProcessor._safe_float(row.get('Fioul - Autres')),
                    'gaz_tac': RTEDataProcessor._safe_float(row.get('Gaz - TAC')),
                    'gaz_cogen': RTEDataProcessor._safe_float(row.get('Gaz - Cogén.')),
                    'gaz_ccg': RTEDataProcessor._safe_float(row.get('Gaz - CCG')),
                    'gaz_autres': RTEDataProcessor._safe_float(row.get('Gaz - Autres')),
                    'hydraulique_fil_eau': RTEDataProcessor._safe_float(row.get('Hydraulique - Fil de l?eau + éclusée')),
                    'hydraulique_lacs': RTEDataProcessor._safe_float(row.get('Hydraulique - Lacs')),
                    'hydraulique_step': RTEDataProcessor._safe_float(row.get('Hydraulique - STEP turbinage')),
                    'bioenergies_dechets': RTEDataProcessor._safe_float(row.get('Bioénergies - Déchets')),
                    'bioenergies_biomasse': RTEDataProcessor._safe_float(row.get('Bioénergies - Biomasse')),
                    'bioenergies_biogaz': RTEDataProcessor._safe_float(row.get('Bioénergies - Biogaz')),
                    'eolien_terrestre': RTEDataProcessor._safe_float(row.get('Eolien terrestre')),
                    'eolien_offshore': RTEDataProcessor._safe_float(row.get('Eolien offshore'))
                }
                
                records.append(record)
                
            except Exception as e:
                print(f"Error processing row at {row.get('Date', 'unknown')} {row.get('Heures', 'unknown')}: {e}")
                continue
        
        return records
    
    @staticmethod
    def _safe_float(value):
        if pd.isna(value) or value in ['ND', '', 'N']:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _safe_str(value):
        return '' if pd.isna(value) else str(value).strip()
    
    @staticmethod
    def process_file(filepath):
        df = RTEDataExtractor.read_file(filepath)
        if not RTEDataExtractor.validate_file(df):
            print(f"Invalid file format: {filepath}")
            return []
        return RTEDataProcessor.extract_records(df)
    
    @staticmethod
    def process_multiple_files(filepaths, verbose=True):
        all_records = []
        if verbose:
            print(f"Processing {len(filepaths)} files...")
        
        for i, filepath in enumerate(filepaths, 1):
            if verbose and i % 10 == 0:
                print(f"  Processed {i}/{len(filepaths)} files... ({len(all_records):,} records so far)")
            records = RTEDataProcessor.process_file(filepath)
            all_records.extend(records)
        
        if verbose:
            print(f"Processing complete: {len(all_records):,} total records from {len(filepaths)} files")
        return all_records


class RTEDataStorage:
    """SQLite database operations"""
    
    @staticmethod
    def ensure_directory(filepath):
        directory = os.path.dirname(filepath)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created directory: {directory}")
    
    @staticmethod
    def create_table(cursor, table_name, sample_record):
        """
        Create database table based on record structure.
        
        Args:
            cursor: SQLite cursor
            table_name (str): Name of table to create
            sample_record (dict): Sample record to determine schema
        """
        columns = []
        
        for key in sample_record.keys():
            # String fields
            if key in ['datetime', 'date', 'time', 'perimetre', 'nature']:
                columns.append(f"{key} TEXT")
            # Numeric fields
            else:
                columns.append(f"{key} REAL")
        
        # Create table with datetime as Primary Key
        create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {', '.join(columns)},
                PRIMARY KEY (datetime)
            )
        """
        
        try:
            cursor.execute(create_query)
        except sqlite3.OperationalError:
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}") # Drop existing table
            cursor.execute(create_query)
    
    @staticmethod
    def save_to_database(records, db_path, table_name='rte_data', replace_duplicates=True):
        """
        Save records to SQLite database.
        
        Args:
            records (list): List of record dictionaries
            db_path (str): Path to SQLite database file
            table_name (str): Name of the table
            replace_duplicates (bool): If True, replace existing records with same datetime
        """
        if not records:
            print("No records to save.")
            return
        
        RTEDataStorage.ensure_directory(db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table based on first record
        RTEDataStorage.create_table(cursor, table_name, records[0])
        
        # Prepare insert query
        columns = list(records[0].keys())
        placeholders = ", ".join(["?" for _ in columns])
        
        if replace_duplicates:
            insert_query = f"INSERT OR REPLACE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        else:
            insert_query = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Insert all records
        inserted = 0
        for record in records:
            try:
                values = [record[col] for col in columns]
                cursor.execute(insert_query, values)
                inserted += 1
            except sqlite3.Error as e:
                print(f"Error inserting record at {record.get('datetime', 'unknown')}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"{inserted:,} records saved to table '{table_name}' in {db_path}")
    
    @staticmethod
    def query_database(db_path, query):
        """
        Execute SQL query on database.
        
        Args:
            db_path (str): Path to SQLite database
            query (str): SQL query to execute
            
        Returns:
            list: Query results
        """
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        conn.close()
        return results
    
    @staticmethod
    def get_database_stats(db_path, table_name='rte_data'):
        """
        Get summary statistics for the database.
        
        Args:
            db_path (str): Path to database
            table_name (str): Table name
            
        Returns:
            dict: Statistics dictionary
        """
        if not os.path.exists(db_path):
            return None
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        try:
            # Total records
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            stats['total_records'] = cursor.fetchone()[0]
            
            # Date range
            cursor.execute(f"SELECT MIN(date), MAX(date) FROM {table_name}")
            date_range = cursor.fetchone()
            stats['date_range'] = date_range
            
            # Average consumption
            cursor.execute(f"SELECT AVG(consommation) FROM {table_name} WHERE consommation IS NOT NULL")
            stats['avg_consumption'] = cursor.fetchone()[0]
            
            # Average production by source
            sources = ['nucleaire', 'eolien', 'solaire', 'hydraulique', 'gaz', 'charbon', 'fioul', 'bioenergies']
            stats['avg_production'] = {}
            
            for source in sources:
                cursor.execute(f"SELECT AVG({source}) FROM {table_name} WHERE {source} IS NOT NULL")
                avg = cursor.fetchone()[0]
                if avg:
                    stats['avg_production'][source] = avg
        
        except sqlite3.Error as e:
            print(f"Error getting stats: {e}")
        
        finally:
            conn.close()
        
        return stats


class RTEDatabase:
    """Main interface combining all components"""
    
    def __init__(self):
        self.extractor = RTEDataExtractor()
        self.processor = RTEDataProcessor()
        self.storage = RTEDataStorage()
    
    def process_file(self, filepath):
        """
        Process a single RTE file.
        
        Args:
            filepath (str): Path to RTE file
            
        Returns:
            list: List of records
        """
        return self.processor.process_file(filepath)
    
    def process_files(self, filepaths, verbose=True):
        """
        Process multiple RTE files.
        
        Args:
            filepaths (list): List of file paths
            verbose (bool): Print progress
            
        Returns:
            list: Combined records
        """
        return self.processor.process_multiple_files(filepaths, verbose)
    
    def save_to_database(self, records, db_path, table_name='rte_data'):
        """
        Save records to database.
        
        Args:
            records (list): Records to save
            db_path (str): Database path
            table_name (str): Table name
        """
        self.storage.save_to_database(records, db_path, table_name)
    
    def create_database_from_folder(self, folder_path, db_path, 
                                     file_pattern='RTE_*.xls', 
                                     table_name='rte_data'):
        """
        Create complete database from a folder of RTE files.
        
        Args:
            folder_path (str): Path to folder containing RTE files
            db_path (str): Output database path
            file_pattern (str): Glob pattern for files
            table_name (str): Table name in database
            
        Returns:
            dict: Statistics about created database
        """
        print("=" * 70)
        print("RTE Data to SQLite Database")
        print("=" * 70)
        
        # Find all files
        pattern = os.path.join(folder_path, file_pattern)
        files = sorted(glob.glob(pattern))
        
        if not files:
            print(f"No files found matching pattern: {pattern}")
            return None
        
        print(f"\nFound {len(files)} files in {folder_path}")
        print(f"Date range: {os.path.basename(files[0])} to {os.path.basename(files[-1])}")
        
        # Process all files
        print("\nProcessing files...")
        records = self.process_files(files)
        
        if not records:
            print("No records extracted. Exiting.")
            return None
        
        # Save to database
        print(f"\nSaving to database: {db_path}")
        self.save_to_database(records, db_path, table_name)
        
        # Get and display statistics
        print("\nDatabase Statistics:")
        print("-" * 70)
        stats = self.storage.get_database_stats(db_path, table_name)
        
        if stats:
            print(f"Total records: {stats['total_records']:,}")
            print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
            print(f"Average consumption: {stats['avg_consumption']:,.0f} MW")
            
            print("\nAverage production by source (MW):")
            for source, avg in sorted(stats['avg_production'].items(), key=lambda x: x[1], reverse=True):
                print(f"  {source:15s}: {avg:>10,.0f} MW")
        
        print("\n" + "=" * 70)
        print("Database created successfully!")
        print("=" * 70)
        
        return stats
    
    def query(self, db_path, query):
        """
        Query the database.
        
        Args:
            db_path (str): Database path
            query (str): SQL query
            
        Returns:
            list: Query results
        """
        return self.storage.query_database(db_path, query)


# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Create SQLite database from RTE energy data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create database from default folder
  python RTE_to_database.py
  
  # Specify custom paths
  python RTE_to_database.py --folder RTE_daily_data --output energy_weather_data/rte.db
  
  # Custom table name
  python RTE_to_database.py --table rte_energy_data
        """
    )
    
    parser.add_argument(
        "--folder",
        type=str,
        default="RTE_daily_data",
        help="Folder containing RTE .xls files (default: RTE_daily_data)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="rte_energy.db",
        help="Output database path (default: rte_energy.db)"
    )
    
    parser.add_argument(
        "--table",
        type=str,
        default="rte_data",
        help="Table name in database (default: rte_data)"
    )
    
    parser.add_argument(
        "--pattern",
        type=str,
        default="RTE_*.xls",
        help="File pattern to match (default: RTE_*.xls)"
    )
    
    args = parser.parse_args()
    
    # Create database
    rte_db = RTEDatabase()
    rte_db.create_database_from_folder(
        folder_path=args.folder,
        db_path=args.output,
        file_pattern=args.pattern,
        table_name=args.table
    )
