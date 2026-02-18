import boto3
import os

def test_s3():
    bucket = os.getenv("S3_BUCKET_NAME")
    
    print(f"Connecting to AWS S3 Bucket: {bucket}...")
    
    try:
        s3 = boto3.client('s3')
        # Try to list files (this proves authentication works)
        s3.list_objects_v2(Bucket=bucket)
        print("SUCCESS! Connection established.")
        
        # Create a dummy file to prove we can WRITE
        s3.put_object(Bucket=bucket, Key='hello_cloud.txt', Body='Hello from Thushan!')
        print("SUCCESS! Uploaded 'hello_cloud.txt'. Check file in bucket!")
        
    except Exception as e:
        print(f"FAILED: {e}")

if __name__ == "__main__":
    test_s3()