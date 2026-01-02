### bronze layer s3 to s3 tables code 
 
import boto3
import time
from datetime import datetime
from typing import Dict, List, Optional

class S3TablesETL:

    
    def __init__(self, 
                 region: str = 'us-east-1',
                 output_location: str = None,
                 bronze_bucket: str = None):
       
        self.region = region
        
        # Auto-configure output location if not provided
        if output_location is None and bronze_bucket:
            self.output_location = f's3://{bronze_bucket}/athena-results/'
        elif output_location:
            self.output_location = output_location
        else:
            raise ValueError("Either output_location or bronze_bucket must be provided")
        
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
            'double': 'double',
            'string': 'string',
            'timestamp[us]': 'timestamp',
            'boolean': 'boolean'
        }
        return type_mapping.get(parquet_type.lower(), 'string')
    
    def _convert_type_to_athena(self, parquet_type: str) -> str:
        """Convert Parquet type to Athena SQL type."""
        type_mapping = {
            'int64': 'BIGINT',
            'double': 'DOUBLE',
            'string': 'STRING',
            'timestamp[us]': 'TIMESTAMP',
            'boolean': 'BOOLEAN'
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
    
    def create_s3_table_from_bronze(self,
                                     source_database: str,
                                     source_table: str,
                                     s3_table_catalog: str,
                                     s3_namespace: str,
                                     target_table: str,
                                     columns: List[Dict]) -> bool:
     
        # Build column list (exclude metadata columns for cleaner target)
        data_columns = [col for col in columns 
                       if not col['name'].startswith('_')]
        
        column_list = ',\n    '.join([col['name'] for col in data_columns])
        
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
    
    def process_bronze_layer(self,
                            bronze_bucket: str,
                            bronze_prefix: str,
                            source_database: str,
                            table_bucket_name: str,
                            namespace_name: str,
                            schema_config: Dict) -> Dict[str, bool]:
      
        results = {}
        
        print("="*70)
        print("S3 TABLES ETL PIPELINE")
        print("="*70)
        
        # Step 1: Create table bucket
        print("\n[1/4] Creating Table Bucket...")
        table_bucket_arn = self.create_table_bucket(table_bucket_name)
        
        # Step 2: Create namespace
        print("\n[2/4] Creating Namespace...")
        namespace = self.create_namespace(table_bucket_arn, namespace_name)
        
        # Step 3: Create external tables in Glue
        print("\n[3/4] Creating External Tables in Glue...")
        for file_path, schema_info in schema_config['schemas'].items():
            # Extract table name from file path
            table_name = file_path.split('/')[-1].replace('.parquet', '')
            
            # Build S3 location
            s3_location = f"s3://{bronze_bucket}/{file_path.rsplit('/', 1)[0]}/"
            
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
        
        for file_path, schema_info in schema_config['schemas'].items():
            source_table = file_path.split('/')[-1].replace('.parquet', '')
            # Remove 'bronze_' prefix for cleaner target table names
            target_table = source_table.replace('bronze_', '')
            
            success = self.create_s3_table_from_bronze(
                source_database=source_database,
                source_table=source_table,
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
    # Your schema configuration (from the provided JSON)
    schema_config = {
        "bucket": "movielens-elt-project",
        "prefix": "bronze_layer/",
        "schemas": {
            "bronze_layer/bronze_genome_scores.parquet": {
                "columns": [
                    {"name": "movieId", "type": "int64", "nullable": True},
                    {"name": "tagId", "type": "int64", "nullable": True},
                    {"name": "relevance", "type": "double", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            },
            "bronze_layer/bronze_genome_tags.parquet": {
                "columns": [
                    {"name": "tagId", "type": "int64", "nullable": True},
                    {"name": "tag", "type": "string", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            },
            "bronze_layer/bronze_links.parquet": {
                "columns": [
                    {"name": "movieId", "type": "int64", "nullable": True},
                    {"name": "imdbId", "type": "int64", "nullable": True},
                    {"name": "tmdbId", "type": "double", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            },
            "bronze_layer/bronze_movies.parquet": {
                "columns": [
                    {"name": "movieId", "type": "int64", "nullable": True},
                    {"name": "title", "type": "string", "nullable": True},
                    {"name": "genres", "type": "string", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            },
            "bronze_layer/bronze_ratings.parquet": {
                "columns": [
                    {"name": "userId", "type": "int64", "nullable": True},
                    {"name": "movieId", "type": "int64", "nullable": True},
                    {"name": "rating", "type": "double", "nullable": True},
                    {"name": "timestamp", "type": "int64", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            },
            "bronze_layer/bronze_tags.parquet": {
                "columns": [
                    {"name": "userId", "type": "int64", "nullable": True},
                    {"name": "movieId", "type": "int64", "nullable": True},
                    {"name": "tag", "type": "string", "nullable": True},
                    {"name": "timestamp", "type": "int64", "nullable": True},
                    {"name": "_ingestion_timestamp", "type": "timestamp[us]", "nullable": True},
                    {"name": "_source_file", "type": "string", "nullable": True},
                    {"name": "_source_bucket", "type": "string", "nullable": True}
                ]
            }
        }
    }
    
    # Initialize ETL processor
    etl = S3TablesETL(
        region='us-east-1',
        bronze_bucket='movielens-elt-project'  # Auto-configures Athena output location
    )
    
    # Process all bronze layer tables
    results = etl.process_bronze_layer(
        bronze_bucket='movielens-elt-project',
        bronze_prefix='bronze_layer/',
        source_database='default',  # Glue database for external tables
        table_bucket_name='movielens-bronze',
        namespace_name='movielensbronze_namespace',
        schema_config=schema_config
    )
    
    print("\n✓ ETL Pipeline Complete!")