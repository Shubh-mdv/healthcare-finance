import boto3

# Creating instance to interacting with S3
s3_client = boto3.client("s3")
print("Successfuly make connection with S3")

# Fetching bucket list at s3
response = s3_client.list_buckets()
print("List of buckets: ", response["Buckets"])

# Getting bucket name for our bucket
bucket_name = response["Buckets"][0]["Name"]
print("Bucket Name: ", bucket_name)

subfolder_paths = [
    "landing/",
    "stage/archived/",
    "config/",
    "Datasets/EMR/hos-a/archived/",
    "Datasets/EMR/hos-b/archived/",
    "Datasets/Claims/",
    "Datasets/CPT_codes/",
]

# Iterating over paths to create subfolders in our bucket
for path in subfolder_paths:
    s3_client.put_object(Bucket=bucket_name, Key=path)
print("Successfuly created subfolders.")
