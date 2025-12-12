import snowflake.connector
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
}

# Database configuration
DATABASE_NAME = os.getenv('DATABASE_NAME', 'MY_DATABASE')
SCHEMA_NAME = os.getenv('SCHEMA_NAME', 'PUBLIC')
WAREHOUSE_NAME = os.getenv('WAREHOUSE_NAME', 'MY_WAREHOUSE')


def validate_config():
    """Validate that all required environment variables are set"""
    required_vars = {
        'SNOWFLAKE_USER': SNOWFLAKE_CONFIG['user'],
        'SNOWFLAKE_PASSWORD': SNOWFLAKE_CONFIG['password'],
        'SNOWFLAKE_ACCOUNT': SNOWFLAKE_CONFIG['account'],
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        print("✗ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"  - {var}")
        print("\nPlease set these variables in your .env file")
        return False
    
    return True


def create_database():
    """Create database, schema, and warehouse in Snowflake"""
    
    print("="*60)
    print("Snowflake Database Creation Script")
    print("="*60)
    
    # Validate configuration
    if not validate_config():
        return
    
    try:
        # Connect to Snowflake
        print("\nConnecting to Snowflake...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✓ Connected successfully")
        
        cursor = conn.cursor()
        
        # Create warehouse
       
        
        
        
        # Create database
        print(f"\nCreating database: {DATABASE_NAME}")
        cursor.execute(f"""
            CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}
            COMMENT = 'Database created by setup script'
        """)
        print(f"✓ Database '{DATABASE_NAME}' created successfully")
        
        # Create schema
        print(f"\nCreating schema: {SCHEMA_NAME}")
        cursor.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}.{SCHEMA_NAME}
            COMMENT = 'Schema created by setup script'
        """)
        print(f"✓ Schema '{SCHEMA_NAME}' created successfully")
        
        # Display summary
        print("\n" + "="*60)
        print("Setup Complete!")
        print("="*60)
        print(f"Warehouse: {WAREHOUSE_NAME}")
        print(f"Database:  {DATABASE_NAME}")
        print(f"Schema:    {SCHEMA_NAME}")
        print(f"\nFull path: {DATABASE_NAME}.{SCHEMA_NAME}")
        print("="*60)
        
        # Close cursor and connection
        cursor.close()
        conn.close()
        print("\n✓ Connection closed successfully")
        
    except snowflake.connector.errors.ProgrammingError as e:
        print(f"\n✗ Snowflake Error: {e}")
        print("\nPlease check:")
        print("  - Your credentials are correct")
        print("  - Your account identifier is correct")
        print("  - You have permission to create databases")
        
    except Exception as e:
        print(f"\n✗ Unexpected Error: {e}")


if __name__ == "__main__":
    create_database()