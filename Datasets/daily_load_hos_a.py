import os
import boto3
import pandas as pd
import mysql.connector

config = {
    "host": "hosp-fin-mgmt-db.c9kqkg8qmk5g.ap-south-1.rds.amazonaws.com",
    "user": "admin",
    "password": "PriyankaM2794",
    "database": "hos_a",
}
conn = mysql.connector.connect(**config)
s3_client = boto3.client("s3")


res = s3_client.list_buckets()
bucket_name = res["Buckets"][0]["Name"]
subfolder_path = "Datasets/EMR/hos-a/"

response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=subfolder_path)
datasets = []
if "Contents" in response:
    for obj in response["Contents"]:
        if obj["Key"].endswith(".csv"):
            datasets.append(obj["Key"])

col_list = {
    "patients": [
        "PatientID",
        "FirstName",
        "LastName",
        "MiddleName",
        "SSN",
        "PhoneNumber",
        "Gender",
        "DOB",
        "Address",
        "ModifiedDate",
    ],
    "departments": ["DeptID", "Name"],
    "encounters": [
        "EncounterID",
        "PatientID",
        "EncounterDate",
        "EncounterType",
        "ProviderID",
        "DepartmentID",
        "ProcedureCode",
        "InsertedDate",
        "ModifiedDate",
    ],
    "providers": [
        "ProviderID",
        "FirstName",
        "LastName",
        "Specialization",
        "DeptID",
        "NPI",
    ],
    "transactions": [
        "TransactionID",
        "EncounterID",
        "PatientID",
        "ProviderID",
        "DeptID",
        "VisitDate",
        "ServiceDate",
        "PaidDate",
        "VisitType",
        "Amount",
        "AmountType",
        "PaidAmount",
        "ClaimID",
        "PayorID",
        "ProcedureCode",
        "ICDCode",
        "LineOfBusiness",
        "MedicaidID",
        "MedicareID",
        "InsertDate",
        "ModifiedDate",
    ],
}
cursor = conn.cursor()
for path in datasets:
    basename = os.path.basename(path).split("_")[0]
    table_name = config.get("database") + "." + basename  # Add DB name if needed

    if basename in col_list:
        df = pd.read_csv(path)

        # Creating the parameterized SQL query
        query = f"""
            INSERT INTO {table_name} ({', '.join(col_list[basename])})
            VALUES ({', '.join(['%s'] * len(col_list[basename]))})
        """

        # Convert DataFrame rows to list of tuples
        values = [
            tuple(row[col] for col in col_list[basename]) for _, row in df.iterrows()
        ]

        try:
            cursor.executemany(query, values)  # Efficient batch insert
            conn.commit()
            print(f"Inserted {len(values)} records into {table_name}")
        except mysql.connector.Error as e:
            print(f"Error inserting into {table_name}: {e}")
            conn.rollback()

# Close cursor and connection
cursor.close()
conn.close()
