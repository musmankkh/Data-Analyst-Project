import boto3
import os
from botocore.exceptions import ClientError

# ======================
# CONFIG
# ======================
BUCKET_NAME = "elt-movielens"
REGION = "eu-north-1"   

LOCAL_DATASET_DIR = "Dataset"    
S3_ROOT_PREFIX = "movielens/"      

# ======================
# S3 CLIENT
# ======================
s3 = boto3.client("s3", region_name=REGION)

# ======================
# CREATE BUCKET
# ======================
def create_bucket(bucket, region):
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists")
    except ClientError:
        print(f"Creating bucket '{bucket}'...")
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print("Bucket created")

create_bucket(BUCKET_NAME, REGION)

# ======================
# CREATE ROOT FOLDER
# ======================
s3.put_object(Bucket=BUCKET_NAME, Key=S3_ROOT_PREFIX)
print(f"Created root folder: s3://{BUCKET_NAME}/{S3_ROOT_PREFIX}")

# ======================
# UPLOAD FILES
# ======================
for file in os.listdir(LOCAL_DATASET_DIR):
    local_path = os.path.join(LOCAL_DATASET_DIR, file)

    if os.path.isfile(local_path):
        s3_key = S3_ROOT_PREFIX + file
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"Uploaded {file} → s3://{BUCKET_NAME}/{s3_key}")

print("✅ Dataset uploaded successfully!")