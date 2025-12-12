import snowflake.connector
import pandas as pd
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters (loaded from environment variables)
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
}

# Names for warehouse, database, and schema to create
WAREHOUSE_NAME = os.getenv('WAREHOUSE_NAME')
DATABASE_NAME = os.getenv('DATABASE_NAME')
SCHEMA_NAME = os.getenv('SCHEMA_NAME')

# Specify the folder containing your CSV files
CSV_FOLDER = os.getenv('CSV_FOLDER', os.getcwd())

# Automatically find all CSV files in the specified folder
CSV_FILES = [os.path.join(CSV_FOLDER, f) for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]


def validate_config():
    """Validate that all required environment variables are set"""
    required_vars = {
        'SNOWFLAKE_USER': SNOWFLAKE_CONFIG['user'],
        'SNOWFLAKE_PASSWORD': SNOWFLAKE_CONFIG['password'],
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_CONFIG['account'],
        'WAREHOUSE_NAME': WAREHOUSE_NAME,
        'DATABASE_NAME': DATABASE_NAME,
        'SCHEMA_NAME': SCHEMA_NAME
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print("✗ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease set these variables in your .env file")
        return False
    
    return True


def create_snowflake_connection(use_database=False):
    """Establish connection to Snowflake"""
    try:
        config = SNOWFLAKE_CONFIG.copy()
        if use_database:
            config.update({
                'warehouse': WAREHOUSE_NAME,
                'database': DATABASE_NAME,
                'schema': SCHEMA_NAME
            })
        conn = snowflake.connector.connect(**config)
        print("✓ Connected to Snowflake successfully")
        return conn
    except Exception as e:
        print(f"✗ Error connecting to Snowflake: {e}")
        raise


def setup_snowflake_resources():
    """Create warehouse, database, and schema"""
    print("\nSetting up Snowflake resources...")
    
    # Connect without specifying warehouse/database
    conn = create_snowflake_connection(use_database=False)
    cursor = conn.cursor()
    
    try:
        # Create warehouse
        print(f"  Creating warehouse: {WAREHOUSE_NAME}")
        cursor.execute(f"""
            CREATE WAREHOUSE IF NOT EXISTS {WAREHOUSE_NAME}
            WITH WAREHOUSE_SIZE = 'XSMALL'
            AUTO_SUSPEND = 300
            AUTO_RESUME = TRUE
        """)
        print(f"  ✓ Warehouse {WAREHOUSE_NAME} ready")
        
        # Use the warehouse
        cursor.execute(f"USE WAREHOUSE {WAREHOUSE_NAME}")
        
        # Create database
        print(f"  Creating database: {DATABASE_NAME}")
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
        print(f"  ✓ Database {DATABASE_NAME} ready")
        
        # Create schema
        print(f"  Creating schema: {SCHEMA_NAME}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}")
        print(f"  ✓ Schema {SCHEMA_NAME} ready")
        
        print("✓ All Snowflake resources created successfully\n")
        
    except Exception as e:
        print(f"✗ Error setting up resources: {e}")
        raise
    finally:
        cursor.close()
        conn.close()


def create_table_from_dataframe(conn, df, table_name):
    """Create table in Snowflake based on DataFrame schema"""
    cursor = conn.cursor()
    
    # Map pandas dtypes to Snowflake types
    type_mapping = {
        'object': 'VARCHAR',
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    
    # Generate CREATE TABLE statement
    columns = []
    for col, dtype in df.dtypes.items():
        snow_type = type_mapping.get(str(dtype), 'VARCHAR')
        columns.append(f'"{col}" {snow_type}')
    
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(columns)}
    )
    """
    
    try:
        cursor.execute(create_sql)
        print(f"✓ Table {table_name} created/verified")
    except Exception as e:
        print(f"✗ Error creating table {table_name}: {e}")
        raise
    finally:
        cursor.close()


def load_csv_to_snowflake(conn, csv_file, table_name=None):
    """Load a single CSV file into Snowflake"""
    
    # Use filename as table name if not provided
    if table_name is None:
        table_name = Path(csv_file).stem.upper()
    
    try:
        # Read CSV into pandas DataFrame
        print(f"\nProcessing {csv_file}...")
        df = pd.read_csv(csv_file)
        print(f"  ✓ Loaded {len(df)} rows from CSV")
        
        # Create table if it doesn't exist
        create_table_from_dataframe(conn, df, table_name)
        
        # Write DataFrame to Snowflake
        from snowflake.connector.pandas_tools import write_pandas
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            auto_create_table=False,
            overwrite=False  # Set to True to replace existing data
        )
        
        if success:
            print(f"  ✓ Loaded {nrows} rows into {table_name}")
        else:
            print(f"  ✗ Failed to load data into {table_name}")
            
        return success
        
    except FileNotFoundError:
        print(f"  ✗ File not found: {csv_file}")
        return False
    except Exception as e:
        print(f"  ✗ Error loading {csv_file}: {e}")
        return False


def main():
    """Main function to load all CSV files"""
    print("="*50)
    print("CSV to Snowflake Load Process")
    print("="*50)
    
    # Validate configuration
    if not validate_config():
        return
    
    # Check if CSV files were found
    if not CSV_FILES:
        print(f"\n✗ No CSV files found in: {CSV_FOLDER}")
        print("Please make sure:")
        print("  1. The CSV_FOLDER path in .env is correct")
        print("  2. Your files have .csv extension")
        return
    
    print(f"\nFound {len(CSV_FILES)} CSV file(s):")
    for i, f in enumerate(CSV_FILES, 1):
        print(f"  {i}. {Path(f).name}")
    
    # Step 1: Create warehouse, database, and schema
    setup_snowflake_resources()
    
    # Step 2: Connect with the created resources
    conn = create_snowflake_connection(use_database=True)
    
    try:
        # Track results
        results = {'success': 0, 'failed': 0}
        
        # Load each CSV file
        for csv_file in CSV_FILES:
            success = load_csv_to_snowflake(conn, csv_file)
            if success:
                results['success'] += 1
            else:
                results['failed'] += 1
        
        # Summary
        print("\n" + "="*50)
        print(f"Load Complete!")
        print(f"  Success: {results['success']}/{len(CSV_FILES)}")
        print(f"  Failed:  {results['failed']}/{len(CSV_FILES)}")
        print(f"\nYour data is in:")
        print(f"  Database: {DATABASE_NAME}")
        print(f"  Schema:   {SCHEMA_NAME}")
        print(f"  Warehouse: {WAREHOUSE_NAME}")
        print("="*50)
        
    finally:
        conn.close()
        print("\n✓ Connection closed")


if __name__ == "__main__":
    main()