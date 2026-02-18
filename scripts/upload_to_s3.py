import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()
AWS_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DATA_DIR = "/opt/airflow/data"

def file_exists_in_s3(s3_client, bucket, key):
    """
    Checks if a file exists in S3 without downloading it.
    """
    try:
        # head_object retrieves ONLY metadata (fast & cheap)
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        # If the error code is 404 (Not Found), then the file is missing
        if e.response['Error']['Code'] == "404":
            return False
        else:
            # If it's another error (permission, etc.), crash nicely
            print(f"Error checking {key}: {e}")
            return False

def upload_directory_to_s3():
    if not AWS_BUCKET:
        print("CRITICAL ERROR: S3_BUCKET_NAME not found.")
        return

    print(f"--- Starting Smart Upload to S3 ({AWS_BUCKET}) ---")
    
    s3 = boto3.client('s3', region_name=AWS_REGION)
    files_uploaded = 0
    files_skipped = 0

    # Walk through all folders
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            # Skip hidden files
            if file.startswith('.'):
                continue
                
            local_path = os.path.join(root, file)
            
            # Create the S3 Key (Relative Path)
            # Example: data/bronze/fires/2000/file.csv -> raw/fires/2000/file.csv
            relative_path = os.path.relpath(local_path, DATA_DIR)
            s3_key = relative_path.replace("\\", "/") # Windows fix

            # --- THE SMART CHECK ---
            if file_exists_in_s3(s3, AWS_BUCKET, s3_key):
                print(f" [SKIP] {s3_key} already exists.")
                files_skipped += 1
            else:
                try:
                    s3.upload_file(local_path, AWS_BUCKET, s3_key)
                    print(f" [UPLOAD] {s3_key}")
                    files_uploaded += 1
                except Exception as e:
                    print(f" [ERROR] Failed to upload {file}: {e}")

    print(f"\n--- Job Complete ---")
    print(f"Uploaded: {files_uploaded}")
    print(f"Skipped:  {files_skipped}")

if __name__ == "__main__":
    upload_directory_to_s3()