import requests
import boto3
from datetime import date
from pyspark.sql import SparkSession

# to get current date
current_date = date.today()

# Initializing spark session
spark = SparkSession.builder.appName("NPI Data").getOrCreate()

# connecting to s3 client to get bucket name
s3_client = boto3.client("s3")
res = s3_client.list_buckets()
bucket_name = res["Buckets"][0]["Name"]

# preparing data for API call
base_url = "https://npiregistry.cms.hhs.gov/api/"
params = {
    "version": "2.1",  # API version
    "state": "CA",  # Example state, replace with desired state or other criteria
    "city": "Los Angeles",  # Example city, replace with desired city
    "limit": 20,  # Limit the number of results for demonstration purposes
}

# calling API
response = requests.get(base_url, params=params)
if response.status_code == 200:
    npi_data = response.json()
    npi_list = [result["number"] for result in npi_data.get("results", {})]

    detailed_result = []
    for npi in npi_list:
        detailed_params = {"version": "2.1", "number": npi}
        detailed_response = requests.get(base_url, params=detailed_params)

        if detailed_response.status_code == 200:
            detailed_data = detailed_response.json()
            if detailed_data["results"]:
                for result in detailed_data["results"]:
                    npi_number = result.get("number")
                    basic_info = result.get("basic")
                    if result["enumeration_type"] == "NPI-1":
                        fname = basic_info.get("first_name", "")
                        lname = basic_info.get("last_name", "")
                    else:
                        fname = basic_info.get("authorized_official_first_name", "")
                        lname = basic_info.get("authorized_official_last_name", "")
                    position = (
                        basic_info.get("authorized_official_title_or_position")
                        if "authorized_official_title_or_position" in basic_info
                        else ""
                    )
                    organization = basic_info.get("organization_name", "")
                    last_updated = basic_info.get("last_updated", "")
                    detailed_result.append(
                        {
                            "npi_id": npi_number,
                            "first_name": fname,
                            "last_name": lname,
                            "position": position,
                            "organization_name": organization,
                            "last_updated": last_updated,
                            "refreshed_at": current_date,
                        }
                    )
    if detailed_result:
        print(detailed_result)
        df = spark.createDataFrame(detailed_result)
        df.show()
        df.write.format("parquet").mode("overwrite").save()
    else:
        print("No detailed result found")
else:
    print(f"Failed to fetch data: {response.status_code} - {response.text}")
