from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import Conflict
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO

# ---------------- CONFIG ----------------
PROJECT_ID = "project-cbe8701e-25df-447d-9da"
DATASET_ID = "gold_layer"
GCS_BUCKET = "project-cbe8701e-goldx-movielens"
S3_BUCKET = "movielens-elt-project"  # SET YOUR S3 BUCKET
S3_PREFIX = "gold"  # Folder in S3
LOCATION = "US"
AWS_REGION = "us-east-1"  # SET YOUR AWS REGION
# ----------------------------------------


def create_gcs_bucket(bucket_name, location, project_id):
    """Creates GCS bucket if it doesn't exist"""
    storage_client = storage.Client(project=project_id)
    try:
        bucket = storage.Bucket(storage_client, bucket_name)
        bucket.location = location
        storage_client.create_bucket(bucket)
        print(f"✅ Created GCS bucket: {bucket_name}")
    except Conflict:
        print(f"ℹ️ Bucket already exists: {bucket_name}")


def export_table_to_gcs_sharded(project_id, dataset_id, table_id, bucket_name):
    """Exports BigQuery table to GCS (allows sharding for large tables)"""
    bq_client = bigquery.Client(project=project_id)
    
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    destination_uri = f"gs://{bucket_name}/{dataset_id}/{table_id}-*.parquet"
    
    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression=bigquery.Compression.SNAPPY
    )
    
    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location=LOCATION
    )
    extract_job.result()
    print(f"✔ Exported to GCS: {table_ref}")
    return f"{dataset_id}/{table_id}"


def merge_and_upload_to_s3(gcs_bucket, gcs_prefix, s3_bucket, s3_key, aws_region):
    """
    Reads all sharded Parquet files from GCS, merges them, 
    and uploads as a single file to S3
    """
    gcs_client = storage.Client()
    s3_client = boto3.client('s3', region_name=aws_region)
    
    # List all sharded files in GCS
    blobs = list(gcs_client.list_blobs(gcs_bucket, prefix=gcs_prefix))
    
    if len(blobs) == 0:
        print(f"⚠️  No files found for {gcs_prefix}")
        return
    
    print(f"   Found {len(blobs)} file(s) in GCS")
    
    # Read and merge all Parquet files
    tables = []
    for blob in blobs:
        if blob.name.endswith('.parquet'):
            print(f"   Reading: {blob.name}")
            parquet_bytes = blob.download_as_bytes()
            table = pq.read_table(BytesIO(parquet_bytes))
            tables.append(table)
    
    if not tables:
        print(f"⚠️  No valid Parquet files found")
        return
    
    # Merge all tables
    merged_table = pa.concat_tables(tables)
    print(f"   Merged {len(tables)} file(s) → {merged_table.num_rows:,} rows")
    
    # Write to S3 as single file
    buffer = BytesIO()
    pq.write_table(merged_table, buffer, compression='snappy')
    buffer.seek(0)
    
    s3_client.upload_fileobj(buffer, s3_bucket, s3_key)
    print(f"✅ Uploaded to S3: s3://{s3_bucket}/{s3_key}")


def process_dataset(project_id, dataset_id, gcs_bucket, s3_bucket, s3_prefix, aws_region):
    """Export all tables from BigQuery to S3 with separate folders for each table"""
    bq_client = bigquery.Client(project=project_id)
    
    dataset_ref = f"{project_id}.{dataset_id}"
    tables = bq_client.list_tables(dataset_ref)
    
    table_count = 0
    view_count = 0
    
    for table in tables:
        full_table = bq_client.get_table(f"{dataset_ref}.{table.table_id}")
        
        if full_table.table_type == "VIEW":
            print(f"⊘ Skipped VIEW: {table.table_id}")
            view_count += 1
            continue
        
        print(f"\n📦 Processing: {table.table_id}")
        
        # Step 1: Export to GCS (may create multiple files)
        gcs_prefix = export_table_to_gcs_sharded(
            project_id, dataset_id, table.table_id, gcs_bucket
        )
        
        # Step 2: Merge and upload to S3 in separate folder for each table
        # Changed: Each table gets its own folder
        s3_key = f"{s3_prefix}/{table.table_id}/{table.table_id}.parquet"
        merge_and_upload_to_s3(
            gcs_bucket, 
            gcs_prefix, 
            s3_bucket, 
            s3_key,
            aws_region
        )
        
        table_count += 1
    
    print(f"\n🎉 Processed {table_count} tables")
    print(f"⊘ Skipped {view_count} views")


def create_s3_bucket_if_not_exists(bucket_name, region):
    """Creates S3 bucket if it doesn't exist"""
    s3_client = boto3.client('s3', region_name=region)
    
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"ℹ️ S3 bucket already exists: {bucket_name}")
    except:
        try:
            if region == 'us-east-1':
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"✅ Created S3 bucket: {bucket_name}")
        except Exception as e:
            print(f"❌ Failed to create S3 bucket: {e}")
            raise


def main():
    create_gcs_bucket(GCS_BUCKET, LOCATION, PROJECT_ID)
    create_s3_bucket_if_not_exists(S3_BUCKET, AWS_REGION)
    process_dataset(PROJECT_ID, DATASET_ID, GCS_BUCKET, S3_BUCKET, S3_PREFIX, AWS_REGION)


if __name__ == "__main__":
    main()