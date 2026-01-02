import boto3
import pyarrow.parquet as pq
from io import BytesIO
from collections import defaultdict
import json
from datetime import datetime
import os

def get_folders_in_prefix(s3_client, bucket_name, prefix):
    """Get all unique folder paths under a prefix."""
    folders = set()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    
    for page in pages:
        # Get common prefixes (folders)
        if 'CommonPrefixes' in page:
            for prefix_info in page['CommonPrefixes']:
                folders.add(prefix_info['Prefix'])
    
    return sorted(folders)


def get_parquet_files_in_folder(s3_client, bucket_name, folder_prefix):
    """Get all parquet files in a specific folder."""
    parquet_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    parquet_files.append(obj['Key'])
    
    return parquet_files


def analyze_folder_schemas(bucket_name, folder_prefix):
    """Analyze all parquet schemas in a single folder."""
    s3_client = boto3.client('s3')
    schemas = {}
    schema_summary = defaultdict(set)
    
    print(f"\n{'='*80}")
    print(f"Analyzing Folder: {folder_prefix}")
    print(f"{'='*80}")
    
    parquet_files = get_parquet_files_in_folder(s3_client, bucket_name, folder_prefix)
    
    if not parquet_files:
        print(f"⚠️  No Parquet files found in {folder_prefix}")
        return None, None
    
    print(f"Found {len(parquet_files)} Parquet file(s)")
    
    # Sample first file for schema
    sample_file = parquet_files[0]
    print(f"\nSampling schema from: {sample_file}")
    print("-" * 80)
    
    try:
        # Download file to memory
        obj = s3_client.get_object(Bucket=bucket_name, Key=sample_file)
        parquet_bytes = obj['Body'].read()
        
        # Read Parquet schema
        parquet_file = pq.ParquetFile(BytesIO(parquet_bytes))
        schema = parquet_file.schema_arrow
        
        schemas[sample_file] = schema
        
        # Print schema details
        print(f"Columns: {len(schema)}")
        print(f"Row Groups: {parquet_file.num_row_groups}")
        print(f"\nSchema:")
        
        for i in range(len(schema)):
            field = schema.field(i)
            print(f"  {i+1}. {field.name:30} {str(field.type):20} (nullable={field.nullable})")
            schema_summary[field.name].add(str(field.type))
        
        # Show metadata if available
        if parquet_file.metadata.metadata:
            print(f"\nMetadata:")
            for key, value in parquet_file.metadata.metadata.items():
                print(f"  {key.decode()}: {value.decode()}")
        
        # Verify consistency with other files (sample a few more if many files exist)
        files_to_check = parquet_files[1:min(5, len(parquet_files))]
        if files_to_check:
            print(f"\nVerifying schema consistency across {len(files_to_check)} additional file(s)...")
            for file_key in files_to_check:
                try:
                    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    pf = pq.ParquetFile(BytesIO(obj['Body'].read()))
                    file_schema = pf.schema_arrow
                    
                    for i in range(len(file_schema)):
                        field = file_schema.field(i)
                        schema_summary[field.name].add(str(field.type))
                except Exception as e:
                    print(f"  ⚠️  Error checking {file_key}: {str(e)}")
        
        # Check for inconsistencies
        inconsistent_columns = [(col, types) for col, types in schema_summary.items() if len(types) > 1]
        if inconsistent_columns:
            print(f"\n⚠️  WARNING: Inconsistent schemas detected!")
            for col_name, types in inconsistent_columns:
                print(f"  Column '{col_name}' has different types: {', '.join(types)}")
        else:
            print(f"\n✅ Schema is consistent across sampled files")
        
        return schema, schema_summary
        
    except Exception as e:
        print(f"ERROR reading file: {str(e)}")
        return None, None


def generate_athena_ddl(schema, bucket_name, folder_prefix, table_name):
    """Generate Athena CREATE EXTERNAL TABLE statement from Parquet schema."""
    
    # Map PyArrow types to Athena types
    type_mapping = {
        'int32': 'INT',
        'int64': 'BIGINT',
        'float': 'FLOAT',
        'double': 'DOUBLE',
        'string': 'STRING',
        'binary': 'BINARY',
        'bool': 'BOOLEAN',
        'timestamp[us]': 'TIMESTAMP',
        'timestamp[ms]': 'TIMESTAMP',
        'date32[day]': 'DATE',
    }
    
    def map_type(arrow_type):
        arrow_type_str = str(arrow_type)
        
        if 'decimal' in arrow_type_str.lower():
            return arrow_type_str.upper().replace('DECIMAL128', 'DECIMAL')
        
        if arrow_type_str in type_mapping:
            return type_mapping[arrow_type_str]
        
        for arrow_key, athena_type in type_mapping.items():
            if arrow_key in arrow_type_str:
                return athena_type
        
        return 'STRING'
    
    # Build columns
    columns = []
    for i in range(len(schema)):
        field = schema.field(i)
        athena_type = map_type(field.type)
        columns.append(f"  {field.name} {athena_type}")
    
    columns_str = ',\n'.join(columns)
    
    # DDL Statements
    ddl_statements = {}
    
    # 1. Regular S3 External Table
    regular_ddl = f"""-- Regular S3 External Table for {table_name}
CREATE EXTERNAL TABLE IF NOT EXISTS database_name.{table_name} (
{columns_str}
)
STORED AS PARQUET
LOCATION 's3://{bucket_name}/{folder_prefix}'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'classification'='parquet'
);"""
    
    # 2. S3 Tables (Iceberg) using CTAS
    ctas_ddl = f"""-- S3 Tables (Iceberg) using CTAS for {table_name}
-- Step 1: Create external table (use regular_s3.sql)
-- Step 2: Create S3 Table from it
CREATE TABLE s3_tables_catalog.database_name.{table_name}_iceberg
WITH (
  table_type = 'ICEBERG',
  location = 's3://your-s3-tables-bucket/{table_name}/',
  is_external = false,
  format = 'PARQUET'
)
AS SELECT * FROM database_name.{table_name};"""
    
    # 3. Direct S3 Tables creation
    direct_iceberg_ddl = f"""-- Direct S3 Tables (Iceberg) creation for {table_name}
CREATE TABLE s3_tables_catalog.database_name.{table_name}_iceberg (
{columns_str}
)
LOCATION 's3://your-s3-tables-bucket/{table_name}/'
TBLPROPERTIES (
  'table_type' = 'ICEBERG',
  'format' = 'parquet'
);

-- Then insert data
INSERT INTO s3_tables_catalog.database_name.{table_name}_iceberg
SELECT * FROM database_name.{table_name};"""
    
    ddl_statements['regular_s3'] = regular_ddl
    ddl_statements['s3_tables_ctas'] = ctas_ddl
    ddl_statements['s3_tables_direct'] = direct_iceberg_ddl
    
    return ddl_statements


def save_folder_schemas(all_folder_data, bucket_name, prefix):
    """Save schema information for all folders."""
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"Schema_Gold"
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n" + "=" * 80)
    print(f"SAVING SCHEMA INFORMATION TO: {output_dir}/")
    print("=" * 80)
    
    # Save per-folder DDLs
    for folder_name, data in all_folder_data.items():
        if data['schema'] is None:
            continue
        
        folder_dir = os.path.join(output_dir, folder_name)
        os.makedirs(folder_dir, exist_ok=True)
        
        # Save each DDL type
        for ddl_type, ddl_content in data['ddl_statements'].items():
            sql_file = os.path.join(folder_dir, f"{ddl_type}.sql")
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(ddl_content)
        
        # Save combined DDL
        all_ddl_file = os.path.join(folder_dir, "all_ddl_statements.sql")
        with open(all_ddl_file, 'w', encoding='utf-8') as f:
            f.write(f"-- Schema DDL Statements for {folder_name}\n")
            f.write(f"-- Generated: {timestamp}\n")
            f.write(f"-- Source: s3://{bucket_name}/{data['folder_prefix']}\n")
            f.write("-- " + "="*76 + "\n\n")
            
            for ddl_type, ddl_content in data['ddl_statements'].items():
                f.write(f"\n-- {ddl_type.upper().replace('_', ' ')}\n")
                f.write("-- " + "-"*76 + "\n")
                f.write(ddl_content)
                f.write("\n\n")
        
        print(f"✅ Saved DDL for table '{folder_name}' in {folder_dir}/")
    
    # Create master summary
    summary_file = os.path.join(output_dir, "MASTER_SUMMARY.txt")
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"Schema Analysis Summary\n")
        f.write(f"Source: s3://{bucket_name}/{prefix}\n")
        f.write(f"Generated: {timestamp}\n")
        f.write("=" * 80 + "\n\n")
        
        for folder_name, data in all_folder_data.items():
            f.write(f"\nTable: {folder_name}\n")
            f.write("-" * 80 + "\n")
            f.write(f"Location: s3://{bucket_name}/{data['folder_prefix']}\n")
            
            if data['schema']:
                f.write(f"Columns: {len(data['schema'])}\n")
                f.write("\nColumn Details:\n")
                for i in range(len(data['schema'])):
                    field = data['schema'].field(i)
                    f.write(f"  - {field.name:30} {str(field.type):20}\n")
            else:
                f.write("No schema available\n")
            f.write("\n")
    
    print(f"✅ Saved master summary: {summary_file}")
    
    # Create README
    readme_file = os.path.join(output_dir, "README.md")
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write(f"""# Schema Analysis Results

**Source:** `s3://{bucket_name}/{prefix}`  
**Generated:** {timestamp}  
**Total Tables:** {len(all_folder_data)}

## Folder Structure

Each folder represents a table and contains:
- `regular_s3.sql` - Standard S3 external table DDL
- `s3_tables_ctas.sql` - S3 Tables (Iceberg) using CTAS approach
- `s3_tables_direct.sql` - Direct S3 Tables (Iceberg) creation
- `all_ddl_statements.sql` - All DDL statements combined

## Tables Found

""")
        for folder_name, data in all_folder_data.items():
            f.write(f"- **{folder_name}** - `{data['folder_prefix']}`\n")
        
        f.write(f"""
## Usage

1. Review `MASTER_SUMMARY.txt` for all table schemas
2. Navigate to each table's folder
3. Choose the appropriate DDL file
4. Update database names and locations as needed
5. Execute in AWS Athena

## Next Steps

- Replace `database_name` with your actual database name
- Replace `your-s3-tables-bucket` with your actual bucket for Iceberg tables
- Review and test the DDL statements before production use
""")
    
    print(f"✅ Saved README: {readme_file}")
    print(f"\n📁 All files saved in: {output_dir}/")
    print(f"\nTo get started, open: {summary_file}")


def analyze_all_folders(bucket_name, prefix):
    """Main function to analyze all folders under a prefix."""
    s3_client = boto3.client('s3')
    
    print(f"\nScanning for folders in s3://{bucket_name}/{prefix}")
    print("=" * 80)
    
    # Get all folders
    folders = get_folders_in_prefix(s3_client, bucket_name, prefix)
    
    if not folders:
        print(f"No folders found in s3://{bucket_name}/{prefix}")
        return
    
    print(f"Found {len(folders)} folder(s):\n")
    for folder in folders:
        print(f"  - {folder}")
    
    # Analyze each folder
    all_folder_data = {}
    
    for folder_prefix in folders:
        # Extract table name from folder path
        table_name = folder_prefix.rstrip('/').split('/')[-1]
        
        # Analyze this folder's schemas
        schema, schema_summary = analyze_folder_schemas(bucket_name, folder_prefix)
        
        if schema:
            # Generate DDL statements
            ddl_statements = generate_athena_ddl(schema, bucket_name, folder_prefix, table_name)
            
            all_folder_data[table_name] = {
                'folder_prefix': folder_prefix,
                'schema': schema,
                'schema_summary': schema_summary,
                'ddl_statements': ddl_statements
            }
            
            print(f"\n```sql")
            print(ddl_statements['regular_s3'])
            print("```")
    
    # Save all results
    if all_folder_data:
        save_folder_schemas(all_folder_data, bucket_name, prefix)
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print(f"Analyzed {len(all_folder_data)} table(s)")


# Example usage
if __name__ == "__main__":
    # Configuration
    BUCKET = "movielens-elt-project"
    PREFIX = "gold/"
    
    # Analyze all folders
    analyze_all_folders(BUCKET, PREFIX)