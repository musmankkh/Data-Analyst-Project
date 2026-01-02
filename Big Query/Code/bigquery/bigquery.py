import boto3
import pandas as pd
from google.cloud import bigquery
from io import StringIO
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

# Load from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3ToBigQueryBronze:
    def __init__(self, aws_access_key_id, aws_secret_access_key, 
                 project_id, dataset_id='bronze_layer', aws_region='eu-north-1'):
       
        # Initialize S3 client with correct region
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region  # ADD THIS LINE
        )
        
        # Initialize BigQuery client with Application Default Credentials
        self.bq_client = bigquery.Client(project=project_id)
        
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Create bronze dataset
        self._create_bronze_dataset()
    
    def _create_bronze_dataset(self):
        """Create BigQuery dataset for bronze layer if it doesn't exist"""
        dataset_ref = f"{self.project_id}.{self.dataset_id}"
        try:
            self.bq_client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset.description = "Bronze layer - raw data from S3"
            self.bq_client.create_dataset(dataset)
            logger.info(f"Created dataset {dataset_ref}")
    
    def extract_csv_from_s3(self, bucket_name, file_key):
      
        try:
            logger.info(f"Extracting {file_key} from bucket {bucket_name}")
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            
            # Read CSV into pandas DataFrame
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            
            # Add metadata columns
            df['_ingestion_timestamp'] = datetime.utcnow()
            df['_source_file'] = file_key
            df['_source_bucket'] = bucket_name
            
            logger.info(f"Successfully extracted {len(df)} rows from {file_key}")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting file from S3: {str(e)}")
            raise
    
    def load_to_bronze(self, df, table_name, write_disposition='WRITE_TRUNCATE'):
     
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
            logger.info(f"Loading data to Bronze: {table_ref}")
            
            # Configure load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                autodetect=True  # Auto-detect schema from DataFrame
            )
            
            # Load DataFrame to BigQuery
            job = self.bq_client.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"Successfully loaded {len(df)} rows to {table_ref}")
            
        except Exception as e:
            logger.error(f"Error loading data to BigQuery: {str(e)}")
            raise
    
    def extract_all_csv_from_s3(self, bucket_name, prefix=''):
        
        try:
            logger.info(f"Listing CSV files in bucket {bucket_name} with prefix '{prefix}'")
            
            # List objects in bucket
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            csv_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.csv'):
                            csv_files.append(key)
            
            logger.info(f"Found {len(csv_files)} CSV files")
            return csv_files
            
        except Exception as e:
            logger.error(f"Error listing S3 objects: {str(e)}")
            raise
    
    def create_bronze_tables(self, bucket_name, prefix='', table_prefix='bronze_'):
      
        try:
            logger.info("=" * 60)
            logger.info("Creating Bronze Layer Tables from S3")
            logger.info("=" * 60)
            
            # Get all CSV files
            csv_files = self.extract_all_csv_from_s3(bucket_name, prefix)
            
            if not csv_files:
                logger.warning("No CSV files found in S3 bucket")
                return []
            
            # Process each file
            created_tables = []
            for file_key in csv_files:
                try:
                    # Extract from S3
                    df = self.extract_csv_from_s3(bucket_name, file_key)
                    
                    # Generate table name from file name
                    file_name = file_key.split('/')[-1].replace('.csv', '')
                    table_name = f"{table_prefix}{file_name}".replace('-', '_').replace(' ', '_').lower()
                    
                    # Load to BigQuery
                    self.load_to_bronze(df, table_name)
                    created_tables.append(table_name)
                    
                except Exception as e:
                    logger.error(f"Error processing file {file_key}: {str(e)}")
                    continue
            
            logger.info("\n" + "=" * 60)
            logger.info(f"Bronze Layer Created Successfully!")
            logger.info(f"Total Tables Created: {len(created_tables)}")
            logger.info(f"Tables: {', '.join(created_tables)}")
            logger.info("=" * 60)
            
            return created_tables
            
        except Exception as e:
            logger.error(f"Bronze layer creation failed: {str(e)}")
            raise
    
    def create_single_bronze_table(self, bucket_name, file_key, table_name):
      
        try:
            logger.info(f"Creating single bronze table: {table_name}")
            
            # Extract from S3
            df = self.extract_csv_from_s3(bucket_name, file_key)
            
            # Load to BigQuery
            self.load_to_bronze(df, table_name)
            
            logger.info(f"Successfully created table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise
    
    def list_bronze_tables(self):
        """List all tables in the bronze layer dataset"""
        try:
            dataset_ref = f"{self.project_id}.{self.dataset_id}"
            tables = list(self.bq_client.list_tables(dataset_ref))
            
            if tables:
                logger.info(f"Tables in {dataset_ref}:")
                for table in tables:
                    logger.info(f"  - {table.table_id}")
            else:
                logger.info(f"No tables found in {dataset_ref}")
            
            return [table.table_id for table in tables]
            
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            raise
    
    def get_table_info(self, table_name):
      
        try:
            table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = self.bq_client.get_table(table_ref)
            
            info = {
                'table_name': table.table_id,
                'num_rows': table.num_rows,
                'num_columns': len(table.schema),
                'created': table.created,
                'modified': table.modified,
                'size_mb': table.num_bytes / (1024 * 1024),
                'schema': [(field.name, field.field_type) for field in table.schema]
            }
            
            logger.info(f"Table Info for {table_name}:")
            logger.info(f"  Rows: {info['num_rows']}")
            logger.info(f"  Columns: {info['num_columns']}")
            logger.info(f"  Size: {info['size_mb']:.2f} MB")
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting table info: {str(e)}")
            raise
    
    def preview_table(self, table_name, limit=10):
        
        try:
            query = f"""
            SELECT *
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            LIMIT {limit}
            """
            
            df = self.bq_client.query(query).to_dataframe()
            logger.info(f"Preview of {table_name} (first {limit} rows):")
            print(df)
            
            return df
            
        except Exception as e:
            logger.error(f"Error previewing table: {str(e)}")
            raise



if __name__ == "__main__":
    
    # Get from environment variables
    AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        raise ValueError("AWS credentials not found. Set environment variables!")
   
    PROJECT_ID = 'project-cbe8701e-25df-447d-9da'
    BUCKET_NAME = 'elt-movielens'
    AWS_REGION = 'eu-north-1'
   
    bronze = S3ToBigQueryBronze(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        project_id=PROJECT_ID,
        dataset_id='bronze_layer',
        aws_region=AWS_REGION
    )
    
    # Create bronze tables
    bronze.create_bronze_tables(
        bucket_name=BUCKET_NAME,
        prefix='movielens/',
        table_prefix='bronze_'
    )