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
            self.output_location = f's3://{source_bucket}/athena-gold-results/'
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
            'list<element: string not null>': 'array<string>',
            'list<element: struct<genre_name: string, avg_rating: decimal128(38, 9), total_ratings: int64, genre_health_score: double> not null>': 'array<struct<genre_name:string,avg_rating:decimal(38,9),total_ratings:bigint,genre_health_score:double>>'
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
            'list<element: string not null>': 'ARRAY<STRING>',
            'list<element: struct<genre_name: string, avg_rating: decimal128(38, 9), total_ratings: int64, genre_health_score: double> not null>': 'ARRAY<STRUCT<genre_name:STRING,avg_rating:DECIMAL(38,9),total_ratings:BIGINT,genre_health_score:DOUBLE>>'
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
    
    def process_gold_layer(self,
                          source_bucket: str,
                          source_prefix: str,
                          source_database: str,
                          table_bucket_name: str,
                          namespace_name: str,
                          schema_config: Dict) -> Dict[str, bool]:
      
        results = {}
        
        print("="*70)
        print("S3 TABLES ETL PIPELINE - GOLD LAYER")
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
            success = self.create_s3_table_from_source(
                source_database=source_database,
                source_table=table_name,
                s3_table_catalog=s3_table_catalog,
                s3_namespace=namespace,
                target_table=table_name,
                columns=schema_info['columns']
            )
            results[table_name] = success
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
    # Gold layer schema configuration
    schema_config = {
        "bucket": "movielens-elt-project",
        "prefix": "gold/",
        "schemas": {
            "dim_date": {
                "location": "s3://movielens-elt-project/gold/dim_date/",
                "columns": [
                    {"name": "date_key", "type": "int64"},
                    {"name": "date_day", "type": "date32[day]"},
                    {"name": "year", "type": "int64"},
                    {"name": "month", "type": "int64"},
                    {"name": "quarter", "type": "int64"},
                    {"name": "week_of_year", "type": "int64"},
                    {"name": "day_of_month", "type": "int64"},
                    {"name": "day_of_week_num", "type": "int64"},
                    {"name": "day_of_year", "type": "int64"},
                    {"name": "year_month", "type": "string"},
                    {"name": "year_quarter", "type": "string"},
                    {"name": "month_name", "type": "string"},
                    {"name": "day_name", "type": "string"},
                    {"name": "month_name_short", "type": "string"},
                    {"name": "day_name_short", "type": "string"},
                    {"name": "week_start_date", "type": "date32[day]"},
                    {"name": "month_start_date", "type": "date32[day]"},
                    {"name": "quarter_start_date", "type": "date32[day]"},
                    {"name": "year_start_date", "type": "date32[day]"},
                    {"name": "month_end_date", "type": "date32[day]"},
                    {"name": "quarter_end_date", "type": "date32[day]"},
                    {"name": "year_end_date", "type": "date32[day]"},
                    {"name": "days_from_today", "type": "int64"},
                    {"name": "weeks_from_today", "type": "int64"},
                    {"name": "months_from_today", "type": "int64"},
                    {"name": "is_weekend", "type": "bool"},
                    {"name": "is_weekday", "type": "bool"},
                    {"name": "is_month_start", "type": "bool"},
                    {"name": "is_month_end", "type": "bool"},
                    {"name": "is_quarter_start", "type": "bool"},
                    {"name": "is_quarter_end", "type": "bool"},
                    {"name": "is_year_start", "type": "bool"},
                    {"name": "is_year_end", "type": "bool"},
                    {"name": "is_today", "type": "bool"},
                    {"name": "is_yesterday", "type": "bool"},
                    {"name": "period_label", "type": "string"},
                    {"name": "season", "type": "string"},
                    {"name": "fiscal_quarter", "type": "int64"},
                    {"name": "fiscal_year", "type": "int64"},
                    {"name": "created_at", "type": "timestamp[us]"}
                ]
            },
            "dim_genres": {
                "location": "s3://movielens-elt-project/gold/dim_genres/",
                "columns": [
                    {"name": "genre_key", "type": "int64"},
                    {"name": "genre_name", "type": "string"},
                    {"name": "genre_super_category", "type": "string"},
                    {"name": "genre_mood", "type": "string"},
                    {"name": "target_audience", "type": "string"},
                    {"name": "typical_content_rating", "type": "string"},
                    {"name": "typical_setting", "type": "string"},
                    {"name": "created_At", "type": "timestamp[us]"},
                    {"name": "updated_At", "type": "timestamp[us]"}
                ]
            },
            "dim_movies": {
                "location": "s3://movielens-elt-project/gold/dim_movies/",
                "columns": [
                    {"name": "movie_id", "type": "int64"},
                    {"name": "title", "type": "string"},
                    {"name": "title_clean", "type": "string"},
                    {"name": "release_year", "type": "int64"},
                    {"name": "release_decade", "type": "double"},
                    {"name": "release_decade_label", "type": "string"},
                    {"name": "movie_era", "type": "string"},
                    {"name": "movie_age_years", "type": "int64"},
                    {"name": "movie_age_category", "type": "string"},
                    {"name": "genres", "type": "string"},
                    {"name": "genres_array", "type": "list<element: string not null>"},
                    {"name": "genre_count", "type": "int64"},
                    {"name": "primary_genre", "type": "string"},
                    {"name": "genre_category", "type": "string"},
                    {"name": "imdb_id", "type": "string"},
                    {"name": "tmdb_id", "type": "int64"},
                    {"name": "is_genre_missing", "type": "bool"},
                    {"name": "has_invalid_year", "type": "bool"},
                    {"name": "has_invalid_genre", "type": "bool"},
                    {"name": "is_imdb_missing", "type": "bool"},
                    {"name": "is_tmdb_missing", "type": "bool"},
                    {"name": "data_completeness", "type": "string"},
                    {"name": "effective_date", "type": "timestamp[us]"},
                    {"name": "expiration_date", "type": "timestamp[us]"},
                    {"name": "is_current", "type": "bool"},
                    {"name": "created_at", "type": "timestamp[us]"}
                ]
            },
            "dim_tags": {
                "location": "s3://movielens-elt-project/gold/dim_tags/",
                "columns": [
                    {"name": "tag_key", "type": "int64"},
                    {"name": "tag_name", "type": "string"},
                    {"name": "tag_source", "type": "string"},
                    {"name": "tag_length", "type": "int64"},
                    {"name": "word_count", "type": "int64"},
                    {"name": "tag_category", "type": "string"},
                    {"name": "tag_sentiment", "type": "string"},
                    {"name": "created_at", "type": "timestamp[us]"},
                    {"name": "updated_at", "type": "timestamp[us]"}
                ]
            },
            "dim_users": {
                "location": "s3://movielens-elt-project/gold/dim_users/",
                "columns": [
                    {"name": "user_id", "type": "int64"},
                    {"name": "first_rating_date", "type": "date32[day]"},
                    {"name": "last_rating_date", "type": "date32[day]"},
                    {"name": "first_rating_year", "type": "int64"},
                    {"name": "last_rating_year", "type": "int64"},
                    {"name": "user_tenure_days", "type": "int64"},
                    {"name": "day_since_last_activity", "type": "int64"},
                    {"name": "lifetime_rating_count", "type": "int64"},
                    {"name": "lifetime_movies_count", "type": "int64"},
                    {"name": "avg_ratings_per_day", "type": "double"},
                    {"name": "user_type", "type": "string"},
                    {"name": "tenure_category", "type": "string"},
                    {"name": "activity_status", "type": "string"},
                    {"name": "user_cohort", "type": "string"},
                    {"name": "user_cohort_group", "type": "string"},
                    {"name": "effective_date", "type": "timestamp[us]"},
                    {"name": "expiration_date", "type": "timestamp[us]"},
                    {"name": "is_current", "type": "bool"},
                    {"name": "created_at", "type": "timestamp[us]"}
                ]
            },
            "fact_genome_scores": {
                "location": "s3://movielens-elt-project/gold/fact_genome_scores/",
                "columns": [
                    {"name": "genome_score_key", "type": "int64"},
                    {"name": "movie_id", "type": "int64"},
                    {"name": "tag_id", "type": "int64"},
                    {"name": "tag_key", "type": "int64"},
                    {"name": "tag", "type": "string"},
                    {"name": "tag_original", "type": "string"},
                    {"name": "relevance_score", "type": "double"},
                    {"name": "relevance_rounded", "type": "double"},
                    {"name": "relevance_category", "type": "string"},
                    {"name": "relevance_detail_category", "type": "string"},
                    {"name": "relevance_band", "type": "double"},
                    {"name": "is_highly_relevant", "type": "int64"},
                    {"name": "is_medium_relevant", "type": "int64"},
                    {"name": "is_lowly_relevant", "type": "int64"},
                    {"name": "source_loaded_at", "type": "timestamp[us]"},
                    {"name": "created_at", "type": "timestamp[us]"},
                    {"name": "source_table", "type": "string"}
                ]
            },
            "fact_user_tags": {
                "location": "s3://movielens-elt-project/gold/fact_user_tags/",
                "columns": [
                    {"name": "tag_event_key", "type": "int64"},
                    {"name": "user_id", "type": "int64"},
                    {"name": "movie_id", "type": "int64"},
                    {"name": "tag_key", "type": "int64"},
                    {"name": "date_key", "type": "int64"},
                    {"name": "tag", "type": "string"},
                    {"name": "tag_type", "type": "string"},
                    {"name": "tag_original", "type": "string"},
                    {"name": "tag_length", "type": "int64"},
                    {"name": "tag_word_count", "type": "int64"},
                    {"name": "tagged_year", "type": "int64"},
                    {"name": "tagged_month", "type": "int64"},
                    {"name": "tagged_at", "type": "timestamp[us]"},
                    {"name": "tagged_date", "type": "date32[day]"},
                    {"name": "tag_complexity", "type": "string"},
                    {"name": "source_loaded_at", "type": "timestamp[us]"},
                    {"name": "created_at", "type": "timestamp[us]"},
                    {"name": "source_table", "type": "string"}
                ]
            },
            "kpi_executive_summary": {
                "location": "s3://movielens-elt-project/gold/kpi_executive_summary/",
                "columns": [
                    {"name": "total_users", "type": "int64"},
                    {"name": "total_movies", "type": "int64"},
                    {"name": "total_ratings", "type": "int64"},
                    {"name": "overall_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "platform_start_date", "type": "date32[day]"},
                    {"name": "platform_last_date", "type": "date32[day]"},
                    {"name": "platform_lifetime_days", "type": "int64"},
                    {"name": "active_users_30d", "type": "int64"},
                    {"name": "active_users_90d", "type": "int64"},
                    {"name": "ratings_30d", "type": "int64"},
                    {"name": "ratings_90d", "type": "int64"},
                    {"name": "avg_rating_30d", "type": "decimal128(38, 9)"},
                    {"name": "calc_avg_daily_ratings_30d", "type": "double"},
                    {"name": "pct_users_active_30d", "type": "double"},
                    {"name": "avg_daily_ratings_lifetime", "type": "double"},
                    {"name": "avg_ratings_per_user", "type": "double"},
                    {"name": "avg_ratings_per_movie", "type": "double"},
                    {"name": "super_power_users", "type": "int64"},
                    {"name": "power_users", "type": "int64"},
                    {"name": "heavy_users", "type": "int64"},
                    {"name": "regular_users", "type": "int64"},
                    {"name": "casual_users", "type": "int64"},
                    {"name": "light_users", "type": "int64"},
                    {"name": "high_quality_movies", "type": "int64"},
                    {"name": "good_quality_movies", "type": "int64"},
                    {"name": "average_quality_movies", "type": "int64"},
                    {"name": "blockbluster_movies", "type": "int64"},
                    {"name": "very_popular_movies", "type": "int64"},
                    {"name": "top_10_genres_by_quality", "type": "list<element: struct<genre_name: string, avg_rating: decimal128(38, 9), total_ratings: int64, genre_health_score: double> not null>"},
                    {"name": "top_10_genres_by_volume", "type": "list<element: struct<genre_name: string, total_ratings: int64, avg_rating: decimal128(38, 9), genre_health_score: double> not null>"},
                    {"name": "latest_daily_ratings", "type": "int64"},
                    {"name": "latest_daily_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "avg_daily_ratings_30d", "type": "double"},
                    {"name": "latest_qoq_change_pct", "type": "double"},
                    {"name": "latest_mom_change_pct", "type": "double"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },
            "kpi_genre_combinations": {
                "location": "s3://movielens-elt-project/gold/kpi_genre_combinations/",
                "columns": [
                    {"name": "genre_combination", "type": "string"},
                    {"name": "primary_genre", "type": "string"},
                    {"name": "secondary_genre", "type": "string"},
                    {"name": "primary_super_category", "type": "string"},
                    {"name": "secondary_super_category", "type": "string"},
                    {"name": "total_movies", "type": "int64"},
                    {"name": "recent_movies_10yr", "type": "int64"},
                    {"name": "recent_movies_5yr", "type": "int64"},
                    {"name": "total_ratings", "type": "int64"},
                    {"name": "unique_users", "type": "int64"},
                    {"name": "avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "rating_median", "type": "decimal128(38, 9)"},
                    {"name": "rating_stddev", "type": "double"},
                    {"name": "positive_ratings", "type": "int64"},
                    {"name": "negative_ratings", "type": "int64"},
                    {"name": "pct_positive", "type": "double"},
                    {"name": "avg_ratings_per_movie", "type": "double"},
                    {"name": "combination_tier", "type": "string"},
                    {"name": "synergy_level", "type": "string"},
                    {"name": "market_position", "type": "string"},
                    {"name": "genre_synergy_score", "type": "double"},
                    {"name": "market_share_pct", "type": "double"},
                    {"name": "pct_recent_movies", "type": "double"},
                    {"name": "trend_status", "type": "string"},
                    {"name": "audience_reception", "type": "string"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },
                        "kpi_genre_performance": {
                "location": "s3://movielens-elt-project/gold/kpi_genre_performance/",
                "columns": [
                    {"name": "genre_name", "type": "string"},
                    {"name": "genre_key", "type": "int64"},
                    {"name": "genre_super_category", "type": "string"},
                    {"name": "genre_mood", "type": "string"},
                    {"name": "target_audience", "type": "string"},
                    {"name": "typical_content_rating", "type": "string"},
                    {"name": "total_movies", "type": "int64"},
                    {"name": "movies_recent_10yr", "type": "int64"},
                    {"name": "movies_recent_5yr", "type": "int64"},
                    {"name": "total_ratings", "type": "int64"},
                    {"name": "total_unique_users", "type": "int64"},
                    {"name": "avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "rating_median", "type": "decimal128(38, 9)"},
                    {"name": "rating_stddev", "type": "double"},
                    {"name": "pct_positive_ratings", "type": "double"},
                    {"name": "avg_ratings_per_movie", "type": "double"},
                    {"name": "ratings_recent_5yr", "type": "int64"},
                    {"name": "avg_rating_recent_5yr", "type": "decimal128(38, 9)"},
                    {"name": "pct_ratings_recent_5yr", "type": "double"},
                    {"name": "pct_movies_recent_5yr", "type": "double"},
                    {"name": "rank_quality", "type": "int64"},
                    {"name": "rank_popularity", "type": "int64"},
                    {"name": "rank_content_volume", "type": "int64"},
                    {"name": "rank_audience_reach", "type": "int64"},
                    {"name": "quality_category", "type": "string"},
                    {"name": "popularity_category", "type": "string"},
                    {"name": "quality_trend", "type": "string"},
                    {"name": "genre_health_score", "type": "double"},
                    {"name": "market_share_pct", "type": "double"},
                    {"name": "health_category", "type": "string"},
                    {"name": "strategic_position", "type": "string"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },

            "kpi_movie_performance": {
                "location": "s3://movielens-elt-project/gold/kpi_movie_performance/",
                "columns": [
                    {"name": "movie_id", "type": "int64"},
                    {"name": "title", "type": "string"},
                    {"name": "title_clean", "type": "string"},
                    {"name": "release_year", "type": "int64"},
                    {"name": "release_decade", "type": "string"},
                    {"name": "movie_era", "type": "string"},
                    {"name": "primary_genre", "type": "string"},
                    {"name": "genre_category", "type": "string"},
                    {"name": "genre_count", "type": "int64"},
                    {"name": "imdb_id", "type": "string"},
                    {"name": "tmdb_id", "type": "int64"},
                    {"name": "total_ratings", "type": "int64"},
                    {"name": "avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "rating_median", "type": "decimal128(38, 9)"},
                    {"name": "rating_stddev", "type": "double"},
                    {"name": "positive_ratings", "type": "int64"},
                    {"name": "negative_ratings", "type": "int64"},
                    {"name": "unique_raters", "type": "int64"},
                    {"name": "pct_positive_ratings", "type": "double"},
                    {"name": "pct_negative_ratings", "type": "double"},
                    {"name": "total_user_tags", "type": "int64"},
                    {"name": "unique_taggers", "type": "int64"},
                    {"name": "total_genome_tags", "type": "int64"},
                    {"name": "high_relevance_tags", "type": "int64"},
                    {"name": "movie_performance_tier", "type": "string"},
                    {"name": "popularity_tier", "type": "string"},
                    {"name": "controversy_level", "type": "string"},
                    {"name": "rating_tier", "type": "string"},
                    {"name": "composite_score", "type": "double"},
                    {"name": "first_rating_date", "type": "date32[day]"},
                    {"name": "last_rating_date", "type": "date32[day]"},
                    {"name": "rating_span_days", "type": "int64"},
                    {"name": "data_completeness", "type": "string"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },

            "kpi_tag_analytics": {
                "location": "s3://movielens-elt-project/gold/kpi_tag_analytics/",
                "columns": [
                    {"name": "tag_name", "type": "string"},
                    {"name": "tag_key", "type": "int64"},
                    {"name": "tag_category", "type": "string"},
                    {"name": "tag_sentiment", "type": "string"},
                    {"name": "tag_type", "type": "string"},
                    {"name": "user_tag_uses", "type": "int64"},
                    {"name": "users_who_tagged", "type": "int64"},
                    {"name": "unique_movies_tagged", "type": "int64"},
                    {"name": "genome_tag_uses", "type": "int64"},
                    {"name": "genome_avg_relevance", "type": "double"},
                    {"name": "genome_movie_count", "type": "int64"},
                    {"name": "high_relevance_count", "type": "int64"},
                    {"name": "avg_rating_tagged_movies", "type": "decimal128(38, 9)"},
                    {"name": "ratings_for_tagged_movies", "type": "int64"},
                    {"name": "total_movie_coverage", "type": "int64"},
                    {"name": "rank_popularity", "type": "int64"},
                    {"name": "rank_movie_coverage", "type": "int64"},
                    {"name": "rank_genome_relevance", "type": "int64"},
                    {"name": "rank_rating_correlation", "type": "int64"},
                    {"name": "relevance_category", "type": "string"},
                    {"name": "activity_status", "type": "string"},
                    {"name": "tag_quality_score", "type": "double"},
                    {"name": "avg_uses_per_user", "type": "double"},
                    {"name": "avg_tags_per_movie", "type": "double"},
                    {"name": "pct_uses_last_year", "type": "double"},
                    {"name": "pct_uses_last_90d", "type": "double"},
                    {"name": "tag_performance_segment", "type": "string"},
                    {"name": "tag_lifecycle_stage", "type": "string"},
                    {"name": "first_used", "type": "date32[day]"},
                    {"name": "last_used", "type": "date32[day]"},
                    {"name": "days_since_last_use", "type": "int64"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },

            "kpi_time_series": {
                "location": "s3://movielens-elt-project/gold/kpi_time_series/",
                "columns": [
                    {"name": "rating_date", "type": "date32[day]"},
                    {"name": "rating_month", "type": "timestamp[us]"},
                    {"name": "date_key", "type": "int64"},
                    {"name": "year", "type": "int64"},
                    {"name": "quarter", "type": "int64"},
                    {"name": "month", "type": "int64"},
                    {"name": "month_name", "type": "string"},
                    {"name": "week_of_year", "type": "int64"},
                    {"name": "day_of_week_num", "type": "int64"},
                    {"name": "day_name", "type": "string"},
                    {"name": "year_month", "type": "string"},
                    {"name": "year_quarter", "type": "string"},
                    {"name": "is_weekend", "type": "bool"},
                    {"name": "is_weekday", "type": "bool"},
                    {"name": "season", "type": "string"},
                    {"name": "daily_ratings", "type": "int64"},
                    {"name": "daily_active_users", "type": "int64"},
                    {"name": "daily_unique_movies", "type": "int64"},
                    {"name": "daily_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "daily_positive_ratings", "type": "int64"},
                    {"name": "daily_negative_ratings", "type": "int64"},
                    {"name": "daily_pct_positive", "type": "double"},
                    {"name": "ma_7day_ratings", "type": "double"},
                    {"name": "ma_30day_ratings", "type": "double"},
                    {"name": "ma_7day_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "dod_rating_change", "type": "int64"},
                    {"name": "dod_rating_change_pct", "type": "double"},
                    {"name": "wow_rating_change", "type": "int64"},
                    {"name": "wow_rating_change_pct", "type": "double"},
                    {"name": "monthly_total_ratings", "type": "int64"},
                    {"name": "monthly_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "mom_rating_change", "type": "int64"},
                    {"name": "mom_rating_change_pct", "type": "double"},
                    {"name": "quarterly_total_ratings", "type": "int64"},
                    {"name": "quarterly_avg_rating", "type": "decimal128(38, 9)"},
                    {"name": "qoq_rating_change", "type": "int64"},
                    {"name": "qoq_rating_change_pct", "type": "double"},
                    {"name": "trend_direction", "type": "string"},
                    {"name": "activity_level", "type": "string"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            },

            "kpi_user_engagement": {
                "location": "s3://movielens-elt-project/gold/kpi_user_engagement/",
                "columns": [
                    {"name": "user_id", "type": "int64"},
                    {"name": "user_type", "type": "string"},
                    {"name": "tenure_category", "type": "string"},
                    {"name": "user_cohort_group", "type": "string"},
                    {"name": "rating_behavior_profile", "type": "string"},
                    {"name": "taste_diversity", "type": "string"},
                    {"name": "total_ratings", "type": "int64"},
                    {"name": "unique_movies_rated", "type": "int64"},
                    {"name": "tenure_days", "type": "int64"},
                    {"name": "avg_rating_given", "type": "decimal128(38, 9)"},
                    {"name": "rating_stddev", "type": "double"},
                    {"name": "positive_ratings_given", "type": "int64"},
                    {"name": "negative_ratings_given", "type": "int64"},
                    {"name": "neutral_ratings_given", "type": "int64"},
                    {"name": "pct_positive_ratings", "type": "double"},
                    {"name": "user_engagement_score", "type": "double"},
                    {"name": "engagement_percentile", "type": "string"},
                    {"name": "first_rating_date", "type": "date32[day]"},
                    {"name": "last_rating_date", "type": "date32[day]"},
                    {"name": "kpi_generated_at", "type": "timestamp[us]"}
                ]
            }
        }
    }


    etl = S3TablesETL(
        region='us-east-1',
        source_bucket='movielens-elt-project'
    )
    
    # Process all gold layer tables
    results = etl.process_gold_layer(
        source_bucket='movielens-elt-project',
        source_prefix='gold/',
        source_database='default',  # Glue database for external tables
        table_bucket_name='movielens-gold',
        namespace_name='movielensgold_namespace',
        schema_config=schema_config
    )
    
    print("\n✓ ETL Pipeline Complete!")    