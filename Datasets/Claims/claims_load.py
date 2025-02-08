import os
import glob
import boto3

s3_client = boto3.client("s3")

res = s3_client.list_buckets()
bucket_name = res["Buckets"][0]["Name"]

files = os.path.abspath(__file__).split("\\")
claims_path = "\\".join(files[0 : (len(files) - 1)])

claims_datasets = glob.glob(f"{claims_path}/*.csv")
for file in claims_datasets:
    file_name = os.path.basename(file)
    s3_key = f"landing/{file_name}"
    s3_client.upload_file(file, bucket_name, s3_key)
    print(f"File '{file}' uploaded to 's3://{bucket_name}/{s3_key}'")
