import boto3
import time
from datetime import datetime
from typing import Dict, List, Optional

class S3TablesETL:

    
    def __init__(self, 
                 region: str = 'us-east-1',
                 output_location: str = None,
                 source_bucket: str = None):
       
        self.region = region
        
        # Auto-configure output location if not provided
        if output_location is None and source_bucket:
            self.output_location = f's3://{source_bucket}/athena-silver-results/'
        elif output_location:
            self.output_location = output_location
        else:
            raise ValueError("Either output_location or source_bucket must be provided")
        
        # Initialize AWS clients
        self.athena_client = boto3.client('athena', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3tables_client = boto3.client('s3tables', region_name=region)
        self.glue_client = boto3.client('glue', region_name=region)
        
        print(f"Athena results will be saved to: {self.output_location}")
        
    def create_table_bucket(self, bucket_name: str) -> str:
     
        try:
            response = self.s3tables_client.create_table_bucket(
                name=bucket_name
            )
            table_bucket_arn = response['arn']
            print(f"✓ Created table bucket: {bucket_name}")
            print(f"  ARN: {table_bucket_arn}")
            return table_bucket_arn
        except self.s3tables_client.exceptions.ConflictException:
            print(f"✓ Table bucket already exists: {bucket_name}")
            # Get existing bucket ARN
            response = self.s3tables_client.get_table_bucket(
                tableBucketARN=f"arn:aws:s3tables:{self.region}:{boto3.client('sts').get_caller_identity()['Account']}:bucket/{bucket_name}"
            )
            return response['arn']
        except Exception as e:
            print(f"✗ Error creating table bucket: {str(e)}")
            raise
    
    def create_namespace(self, table_bucket_arn: str, namespace_name: str) -> str:
       
        try:
            response = self.s3tables_client.create_namespace(
                tableBucketARN=table_bucket_arn,
                namespace=[namespace_name]
            )
            print(f"✓ Created namespace: {namespace_name}")
            print(f"  Response: {response}")
            return namespace_name
        except self.s3tables_client.exceptions.ConflictException:
            print(f"✓ Namespace already exists: {namespace_name}")
            return namespace_name
        except Exception as e:
            print(f"✗ Error creating namespace: {str(e)}")
            raise
    
    def create_external_table_in_glue(self, 
                                       database_name: str,
                                       table_name: str, 
                                       s3_location: str,
                                       columns: List[Dict]) -> bool:
       
        try:
            # Ensure database exists
            try:
                self.glue_client.get_database(Name=database_name)
            except self.glue_client.exceptions.EntityNotFoundException:
                self.glue_client.create_database(
                    DatabaseInput={'Name': database_name}
                )
                print(f"✓ Created Glue database: {database_name}")
            
            # Convert schema to Glue format
            glue_columns = []
            for col in columns:
                glue_type = self._convert_type_to_glue(col['type'])
                glue_columns.append({
                    'Name': col['name'],
                    'Type': glue_type
                })
            
            # Create table
            self.glue_client.create_table(
                DatabaseName=database_name,
                TableInput={
                    'Name': table_name,
                    'StorageDescriptor': {
                        'Columns': glue_columns,
                        'Location': s3_location,
                        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        'SerdeInfo': {
                            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        }
                    },
                    'TableType': 'EXTERNAL_TABLE',
                    'Parameters': {
                        'EXTERNAL': 'TRUE',
                        'parquet.compression': 'SNAPPY'
                    }
                }
            )
            print(f"✓ Created external table: {database_name}.{table_name}")
            return True
        except self.glue_client.exceptions.AlreadyExistsException:
            print(f"✓ External table already exists: {database_name}.{table_name}")
            return True
        except Exception as e:
            print(f"✗ Error creating external table: {str(e)}")
            return False
    
    def _convert_type_to_glue(self, parquet_type: str) -> str:
        """Convert Parquet type to Glue/Hive type."""
        type_mapping = {
            'int64': 'bigint',
            'int32': 'int',
            'double': 'double',
            'string': 'string',
            'timestamp[us]': 'timestamp',
            'date32[day]': 'date',
            'bool': 'boolean',
            'boolean': 'boolean',
            'decimal128(38, 9)': 'decimal(38,9)',
            'list<element: string not null>': 'array<string>'
        }
        return type_mapping.get(parquet_type.lower(), 'string')
    
    def _convert_type_to_athena(self, parquet_type: str) -> str:
        """Convert Parquet type to Athena SQL type."""
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'double': 'DOUBLE',
            'string': 'STRING',
            'timestamp[us]': 'TIMESTAMP',
            'date32[day]': 'DATE',
            'bool': 'BOOLEAN',
            'boolean': 'BOOLEAN',
            'decimal128(38, 9)': 'DECIMAL(38,9)',
            'list<element: string not null>': 'ARRAY<STRING>'
        }
        return type_mapping.get(parquet_type.lower(), 'STRING')
    
    def execute_athena_query(self, query: str, wait: bool = True) -> Optional[str]:

        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': self.output_location}
            )
            query_id = response['QueryExecutionId']
            print(f"  Query submitted: {query_id}")
            
            if wait:
                return self._wait_for_query(query_id)
            return query_id
        except Exception as e:
            print(f"✗ Error executing query: {str(e)}")
            print(f"  Query: {query[:200]}...")
            return None
    
    def _wait_for_query(self, query_id: str, max_wait: int = 300) -> Optional[str]:
        """Wait for Athena query to complete."""
        start_time = time.time()
        while time.time() - start_time < max_wait:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_id
            )
            status = response['QueryExecution']['Status']['State']
            
            if status == 'SUCCEEDED':
                print(f"  ✓ Query completed successfully")
                return query_id
            elif status in ['FAILED', 'CANCELLED']:
                reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
                print(f"  ✗ Query {status.lower()}")
                print(f"    Reason: {reason}")
                
                # Try to get more detailed error info
                if 'AthenaError' in response['QueryExecution']['Status']:
                    error_info = response['QueryExecution']['Status']['AthenaError']
                    print(f"    Error Type: {error_info.get('ErrorType', 'Unknown')}")
                    print(f"    Error Message: {error_info.get('ErrorMessage', 'Unknown')}")
                return None
            
            time.sleep(2)
        
        print(f"  ✗ Query timeout after {max_wait}s")
        return None
    
    def create_s3_table_from_source(self,
                                     source_database: str,
                                     source_table: str,
                                     s3_table_catalog: str,
                                     s3_namespace: str,
                                     target_table: str,
                                     columns: List[Dict]) -> bool:
     
        # Build column list
        column_list = ',\n    '.join([col['name'] for col in columns])
        
        # Build CTAS query
        query = f"""
CREATE TABLE "{s3_table_catalog}"."{s3_namespace}"."{target_table}"
WITH (
    format = 'PARQUET'
)
AS
SELECT
    {column_list}
FROM "{source_database}"."{source_table}"
"""
        
        print(f"\n→ Creating S3 Table: {target_table}")
        print(f"  Source: {source_database}.{source_table}")
        print(f"  Target: {s3_table_catalog}.{s3_namespace}.{target_table}")
        
        result = self.execute_athena_query(query)
        return result is not None
    
    def process_silver_layer(self,
                            source_bucket: str,
                            source_prefix: str,
                            source_database: str,
                            table_bucket_name: str,
                            namespace_name: str,
                            schema_config: Dict) -> Dict[str, bool]:
      
        results = {}
        
        print("="*70)
        print("S3 TABLES ETL PIPELINE - SILVER LAYER")
        print("="*70)
        
        # Step 1: Create table bucket
        print("\n[1/4] Creating Table Bucket...")
        table_bucket_arn = self.create_table_bucket(table_bucket_name)
        
        # Step 2: Create namespace
        print("\n[2/4] Creating Namespace...")
        namespace = self.create_namespace(table_bucket_arn, namespace_name)
        
        # Step 3: Create external tables in Glue
        print("\n[3/4] Creating External Tables in Glue...")
        for table_name, schema_info in schema_config['schemas'].items():
            # Build S3 location
            s3_location = schema_info['location']
            
            # Create external table
            self.create_external_table_in_glue(
                database_name=source_database,
                table_name=table_name,
                s3_location=s3_location,
                columns=schema_info['columns']
            )
        
        # Step 4: Create S3 Tables and load data
        print("\n[4/4] Creating S3 Tables and Loading Data...")
        s3_table_catalog = "s3tablescatalog/" + table_bucket_name
        
        for table_name, schema_info in schema_config['schemas'].items():
            # Remove 'silver_' prefix for cleaner target table names
            target_table = table_name.replace('silver_', '')
            
            success = self.create_s3_table_from_source(
                source_database=source_database,
                source_table=table_name,
                s3_table_catalog=s3_table_catalog,
                s3_namespace=namespace,
                target_table=target_table,
                columns=schema_info['columns']
            )
            results[target_table] = success
            time.sleep(1)  # Brief pause between operations
        
        # Print summary
        print("\n" + "="*70)
        print("PROCESSING SUMMARY")
        print("="*70)
        successful = sum(1 for v in results.values() if v)
        total = len(results)
        print(f"\nTotal tables: {total}")
        print(f"Successful: {successful}")
        print(f"Failed: {total - successful}")
        
        print("\nTable Status:")
        for table, status in results.items():
            status_icon = "✓" if status else "✗"
            print(f"  {status_icon} {table}")
        
        return results


# Example usage
if __name__ == "__main__":
    # Silver layer schema configuration
    schema_config = {
        "bucket": "movielens-elt-project",
        "prefix": "silver/",
        "schemas": {
            "silver_genome_scores": {
                "location": "s3://movielens-elt-project/silver/silver_genome_scores/",
                "columns": [
                    {"name": "movie_id", "type": "int64"},
                    {"name": "tag_id", "type": "int64"},
                    {"name": "relevance", "type": "double"},
                    {"name": "relevance_rounded", "type": "double"},
                    {"name": "relevance_category", "type": "string"},
                    {"name": "is_relevance_out_of_range", "type": "bool"},
                    {"name": "is_duplicate", "type": "bool"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            },
            "silver_genome_tags": {
                "location": "s3://movielens-elt-project/silver/silver_genome_tags/",
                "columns": [
                    {"name": "tag_id", "type": "int64"},
                    {"name": "tag", "type": "string"},
                    {"name": "tag_original", "type": "string"},
                    {"name": "tag_length", "type": "int64"},
                    {"name": "is_tag_missing", "type": "bool"},
                    {"name": "is_tag_too_short", "type": "bool"},
                    {"name": "is_tag_too_long", "type": "bool"},
                    {"name": "has_special_chars", "type": "bool"},
                    {"name": "is_duplicate", "type": "bool"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            },
            "silver_links": {
                "location": "s3://movielens-elt-project/silver/silver_links/",
                "columns": [
                    {"name": "movie_id", "type": "int64"},
                    {"name": "imdb_id_raw", "type": "string"},
                    {"name": "imdb_id", "type": "string"},
                    {"name": "tmdb_id", "type": "int64"},
                    {"name": "is_imdb_missing", "type": "bool"},
                    {"name": "is_tmdb_missing", "type": "bool"},
                    {"name": "is_duplicate", "type": "bool"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            },
            "silver_movies": {
                "location": "s3://movielens-elt-project/silver/silver_movies/",
                "columns": [
                    {"name": "movie_id", "type": "int64"},
                    {"name": "title", "type": "string"},
                    {"name": "title_clean", "type": "string"},
                    {"name": "release_year", "type": "int64"},
                    {"name": "content_type", "type": "string"},
                    {"name": "genres", "type": "string"},
                    {"name": "genres_array", "type": "list<element: string not null>"},
                    {"name": "genre_count", "type": "int64"},
                    {"name": "is_title_missing", "type": "bool"},
                    {"name": "is_genre_missing", "type": "bool"},
                    {"name": "has_malformed_year", "type": "bool"},
                    {"name": "has_invalid_year", "type": "bool"},
                    {"name": "year_pattern_type", "type": "string"},
                    {"name": "has_invalid_genre", "type": "bool"},
                    {"name": "is_duplicate", "type": "bool"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            },
            "silver_ratings": {
                "location": "s3://movielens-elt-project/silver/silver_ratings/",
                "columns": [
                    {"name": "user_id", "type": "int64"},
                    {"name": "movie_id", "type": "int64"},
                    {"name": "rating", "type": "decimal128(38, 9)"},
                    {"name": "timestamp_unix", "type": "int64"},
                    {"name": "rating_datetime", "type": "timestamp[us]"},
                    {"name": "rating_date", "type": "date32[day]"},
                    {"name": "rating_year", "type": "int64"},
                    {"name": "rating_month", "type": "int64"},
                    {"name": "rating_day", "type": "int64"},
                    {"name": "rating_day_of_week", "type": "string"},
                    {"name": "data_quality_status", "type": "string"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            },
            "silver_tags": {
                "location": "s3://movielens-elt-project/silver/silver_tags/",
                "columns": [
                    {"name": "user_id", "type": "int64"},
                    {"name": "movie_id", "type": "int64"},
                    {"name": "tag", "type": "string"},
                    {"name": "tag_original", "type": "string"},
                    {"name": "tag_length", "type": "int64"},
                    {"name": "tag_word_count", "type": "int64"},
                    {"name": "tag_type", "type": "string"},
                    {"name": "tagged_at", "type": "timestamp[us]"},
                    {"name": "tagged_date", "type": "date32[day]"},
                    {"name": "tagged_year", "type": "int64"},
                    {"name": "tagged_month", "type": "int64"},
                    {"name": "tagged_day_of_week", "type": "int64"},
                    {"name": "is_tag_missing", "type": "bool"},
                    {"name": "is_tag_too_short", "type": "bool"},
                    {"name": "is_tag_too_long", "type": "bool"},
                    {"name": "has_special_chars", "type": "bool"},
                    {"name": "is_duplicate", "type": "bool"},
                    {"name": "duplicate_rank", "type": "int64"},
                    {"name": "loaded_at", "type": "timestamp[us]"},
                    {"name": "dbt_run_id", "type": "string"},
                    {"name": "dbt_run_started_at", "type": "string"}
                ]
            }
        }
    }
    
    # Initialize ETL processor
    etl = S3TablesETL(
        region='us-east-1',
        source_bucket='movielens-elt-project'
    )
    
    # Process all silver layer tables
    results = etl.process_silver_layer(
        source_bucket='movielens-elt-project',
        source_prefix='silver/',
        source_database='default',  # Glue database for external tables
        table_bucket_name='movielens-silver',
        namespace_name='movielenssilver_namespace',
        schema_config=schema_config
    )
    
    print("\n✓ ETL Pipeline Complete!")