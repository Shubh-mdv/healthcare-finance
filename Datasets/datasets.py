import boto3
import pandas as pd
import random
from faker import Faker
from datetime import datetime

# Initializing faker instance
s3_client = boto3.client("s3")
response = s3_client.list_buckets()
bucket_name = response["Buckets"][0]["Name"]

fake = Faker()

now = datetime.now()

# Increase Faker's seed for reproducibility
Faker.seed(42)

# Patients Data
# Generate 50,000 patient records for hos-a
num_records = 50000
data = {
    "PatientID": [f"HOSP{str(i).zfill(6)}" for i in range(1, num_records + 1)],
    "FirstName": [fake.first_name() for _ in range(num_records)],
    "LastName": [fake.last_name() for _ in range(num_records)],
    "MiddleName": [fake.random_letter().upper() for _ in range(num_records)],
    "SSN": [fake.ssn() for _ in range(num_records)],
    "PhoneNumber": [fake.phone_number() for _ in range(num_records)],
    "Gender": [random.choice(["Male", "Female"]) for _ in range(num_records)],
    "DOB": [
        fake.date_of_birth(minimum_age=0, maximum_age=100) for _ in range(num_records)
    ],
    "Address": [fake.address().replace("\n", ", ") for _ in range(num_records)],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_records)
    ],
}

# # Creating dataframe based on data
# patient_df = pd.DataFrame(data=data)

# file_name = "patients_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-a/{file_name}"
# patient_df.to_csv(local_file_path, index=False)
# s3_key = f"Datasets/EMR/hos-a/{file_name}"

# s3_client.upload_file(local_file_path, bucket_name, s3_key)
# print(f"File '{local_file_path}' uploaded to 's3://{bucket_name}/{s3_key}'")


# Generate 50,000 patient records for hos-b
num_records = 50000
data = {
    "ID": [f"HOSP{str(i).zfill(6)}" for i in range(1, num_records + 1)],
    "F_Name": [fake.first_name() for _ in range(num_records)],
    "L_Name": [fake.last_name() for _ in range(num_records)],
    "M_Name": [fake.random_letter().upper() for _ in range(num_records)],
    "SSN": [fake.ssn() for _ in range(num_records)],
    "PhoneNumber": [fake.phone_number() for _ in range(num_records)],
    "Gender": [random.choice(["Male", "Female"]) for _ in range(num_records)],
    "DOB": [
        fake.date_of_birth(minimum_age=0, maximum_age=100) for _ in range(num_records)
    ],
    "Address": [fake.address().replace("\n", ", ") for _ in range(num_records)],
    "Updated_Date": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_records)
    ],
}

# Creating dataframe based on data
patient_df = pd.DataFrame(data=data)

# file_name = "patients_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-b/{file_name}"
# patient_df.to_csv(local_file_path, index=False)
# s3_key = f"Datasets/EMR/hos-b/{file_name}"

# s3_client.upload_file(local_file_path, bucket_name, s3_key)
# print(f"File '{local_file_path}' uploaded to 's3://{bucket_name}/{s3_key}'")


# ---------------------------#---------------------#------------------------#-------------
# Departments data
# Define department data
departments = {
    "DeptID": [f"DEPT{str(i).zfill(3)}" for i in range(1, 21)],
    "Name": [
        "Emergency",
        "Cardiology",
        "Neurology",
        "Oncology",
        "Pediatrics",
        "Orthopedics",
        "Dermatology",
        "Gastroenterology",
        "Urology",
        "Radiology",
        "Anesthesiology",
        "Pathology",
        "Surgery",
        "Pulmonology",
        "Nephrology",
        "Ophthalmology",
        "Gynecology",
        "Psychiatry",
        "Endocrinology",
        "Rheumatology",
    ],
}

# Create DataFrame
departments_df = pd.DataFrame(departments)

# file_name = "departments_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-b/{file_name}"
# departments_df.to_csv(local_file_path, index=False)
# s3_key = f"Datasets/EMR/hos-b/{file_name}"

# s3_client.upload_file(local_file_path, bucket_name, s3_key)
# print(f"File '{local_file_path}' uploaded to 's3://{bucket_name}/{s3_key}'")

# ---------------------------#---------------------#------------------------#-------------
# Provider data

num_providers_hospital1 = 25  # Number of providers in Hospital 1
num_providers_hospital2 = 30  # Number of providers in Hospital 2
specializations = [
    "Cardiology",
    "Neurology",
    "Orthopedics",
    "General Surgery",
    "Pediatrics",
    "Radiology",
    "Dermatology",
    "Oncology",
    "Anesthesiology",
    "Emergency Medicine",
    "Psychiatry",
]
departments = [f"DEPT{str(i).zfill(3)}" for i in range(1, 21)]  # 20 department IDs

# Generate Hospital 1 provider data
hospital1_provider_data = {
    "ProviderID": [
        f"H1-PROV{str(i).zfill(4)}" for i in range(1, num_providers_hospital1 + 1)
    ],
    "FirstName": [fake.first_name() for _ in range(num_providers_hospital1)],
    "LastName": [fake.last_name() for _ in range(num_providers_hospital1)],
    "Specialization": [
        random.choice(specializations) for _ in range(num_providers_hospital1)
    ],
    "DeptID": [random.choice(departments) for _ in range(num_providers_hospital1)],
    "NPI": [
        fake.unique.numerify("##########") for _ in range(num_providers_hospital1)
    ],  # NPI as a 10-digit number
}

# Generate Hospital 2 provider data
hospital2_provider_data = {
    "ProviderID": [
        f"H2-PROV{str(i).zfill(4)}" for i in range(1, num_providers_hospital2 + 1)
    ],
    "FirstName": [fake.first_name() for _ in range(num_providers_hospital2)],
    "LastName": [fake.last_name() for _ in range(num_providers_hospital2)],
    "Specialization": [
        random.choice(specializations) for _ in range(num_providers_hospital2)
    ],
    "DeptID": [random.choice(departments) for _ in range(num_providers_hospital2)],
    "NPI": [
        fake.unique.numerify("##########") for _ in range(num_providers_hospital2)
    ],  # NPI as a 10-digit number
}

# Create DataFrames
hospital1_providers_df = pd.DataFrame(hospital1_provider_data)
hospital2_providers_df = pd.DataFrame(hospital2_provider_data)

# file_name1 = "providers_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path1 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-a/{file_name1}"
# hospital1_providers_df.to_csv(local_file_path1, index=False)
# s3_key1 = f"Datasets/EMR/hos-a/{file_name1}"

# s3_client.upload_file(local_file_path1, bucket_name, s3_key1)
# print(f"File '{local_file_path1}' uploaded to 's3://{bucket_name}/{s3_key1}'")


# file_name2 = "providers_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path2 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-b/{file_name2}"
# hospital2_providers_df.to_csv(local_file_path2, index=False)
# s3_key2 = f"Datasets/EMR/hos-b/{file_name2}"

# s3_client.upload_file(local_file_path2, bucket_name, s3_key2)
# print(f"File '{local_file_path2}' uploaded to 's3://{bucket_name}/{s3_key2}'")

# ---------------------------#---------------------#------------------------#-------------
# Claims data

num_claims = 10000  # Number of claim records per hospital
payors = ["Medicare", "Medicaid", "BlueCross", "Aetna", "UnitedHealthcare"]
claim_statuses = ["Pending", "Approved", "Rejected", "Paid", "Denied"]
payor_types = ["Government", "Private", "Self-pay"]

# Generate Hospital 1 claims data
hospital1_claims_data = {
    "ClaimID": [f"CLAIM{str(i).zfill(6)}" for i in range(1, num_claims + 1)],
    "TransactionID": [
        f"TRANS{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_claims)
    ],
    "PatientID": [
        f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_claims)
    ],
    "EncounterID": [
        f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_claims)
    ],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_claims)
    ],
    "DeptID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_claims)],
    "ServiceDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "ClaimDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "PayorID": [random.choice(payors) for _ in range(num_claims)],
    "ClaimAmount": [round(random.uniform(100, 5000), 2) for _ in range(num_claims)],
    "PaidAmount": [round(random.uniform(50, 4500), 2) for _ in range(num_claims)],
    "ClaimStatus": [random.choice(claim_statuses) for _ in range(num_claims)],
    "PayorType": [random.choice(payor_types) for _ in range(num_claims)],
    "Deductible": [round(random.uniform(10, 500), 2) for _ in range(num_claims)],
    "Coinsurance": [round(random.uniform(0, 200), 2) for _ in range(num_claims)],
    "Copay": [round(random.uniform(5, 50), 2) for _ in range(num_claims)],
    "InsertDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
}

# Generate Hospital 2 claims data
hospital2_claims_data = {
    "ClaimID": [f"CLAIM{str(i).zfill(6)}" for i in range(1, num_claims + 1)],
    "TransactionID": [
        f"TRANS{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_claims)
    ],
    "PatientID": [
        f"HOSP2-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_claims)
    ],
    "EncounterID": [
        f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_claims)
    ],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_claims)
    ],
    "DeptID": [f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_claims)],
    "ServiceDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "ClaimDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "PayorID": [random.choice(payors) for _ in range(num_claims)],
    "ClaimAmount": [round(random.uniform(100, 5000), 2) for _ in range(num_claims)],
    "PaidAmount": [round(random.uniform(50, 4500), 2) for _ in range(num_claims)],
    "ClaimStatus": [random.choice(claim_statuses) for _ in range(num_claims)],
    "PayorType": [random.choice(payor_types) for _ in range(num_claims)],
    "Deductible": [round(random.uniform(10, 500), 2) for _ in range(num_claims)],
    "Coinsurance": [round(random.uniform(0, 200), 2) for _ in range(num_claims)],
    "Copay": [round(random.uniform(5, 50), 2) for _ in range(num_claims)],
    "InsertDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_claims)
    ],
}

# Create DataFrames
hospital1_claims_df = pd.DataFrame(hospital1_claims_data)
hospital2_claims_df = pd.DataFrame(hospital2_claims_data)

# file_name1 = "hospital1_claims_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path1 = f"D:/HealtCare-Finance Project/Datasets/Claims/{file_name1}"
# hospital1_claims_df.to_csv(local_file_path1, index=False)
# s3_key1 = f"Datasets/Claims/{file_name1}"

# s3_client.upload_file(local_file_path1, bucket_name, s3_key1)
# print(f"File '{local_file_path1}' uploaded to 's3://{bucket_name}/{s3_key1}'")


# file_name2 = "hospital2_claims_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path2 = f"D:/HealtCare-Finance Project/Datasets/Claims/{file_name2}"
# hospital2_claims_df.to_csv(local_file_path2, index=False)
# s3_key2 = f"Datasets/Claims/{file_name2}"

# s3_client.upload_file(local_file_path2, bucket_name, s3_key2)
# print(f"File '{local_file_path2}' uploaded to 's3://{bucket_name}/{s3_key2}'")

# ---------------------------#---------------------#------------------------#-------------
# transaction data

# Parameters for data generation
num_transactions = 10000  # Number of transaction records per hospital
amount_types = ["Co-pay", "Insurance", "Self-pay", "Medicaid", "Medicare"]
visit_types = ["Routine", "Follow-up", "Emergency", "Consultation"]
line_of_business = ["Commercial", "Medicaid", "Medicare", "Self-Pay"]
icd_codes = [
    f"I{random.randint(10, 99)}.{random.randint(0, 9)}" for _ in range(100)
]  # Sample ICD codes
cpt_codes = [str(random.randint(10000, 99999)) for _ in range(1000)]  # Sample CPT codes

# Generate Hospital 1 transaction data
hospital1_transaction_data = {
    "TransactionID": [
        f"TRANS{str(i).zfill(6)}" for i in range(1, num_transactions + 1)
    ],
    "EncounterID": [
        f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_transactions)
    ],
    "PatientID": [
        f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}"
        for _ in range(num_transactions)
    ],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_transactions)
    ],
    "DeptID": [
        f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_transactions)
    ],
    "VisitDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "ServiceDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "PaidDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "VisitType": [random.choice(visit_types) for _ in range(num_transactions)],
    "Amount": [round(random.uniform(50, 1000), 2) for _ in range(num_transactions)],
    "AmountType": [random.choice(amount_types) for _ in range(num_transactions)],
    "PaidAmount": [round(random.uniform(20, 800), 2) for _ in range(num_transactions)],
    "ClaimID": [
        f"CLAIM{str(random.randint(100000, 999999))}" for _ in range(num_transactions)
    ],
    "PayorID": [
        f"PAYOR{str(random.randint(1000, 9999))}" for _ in range(num_transactions)
    ],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_transactions)],
    "ICDCode": [random.choice(icd_codes) for _ in range(num_transactions)],
    "LineOfBusiness": [
        random.choice(line_of_business) for _ in range(num_transactions)
    ],
    "MedicaidID": [
        f"MEDI{str(random.randint(10000, 99999))}" for _ in range(num_transactions)
    ],
    "MedicareID": [
        f"MCARE{str(random.randint(10000, 99999))}" for _ in range(num_transactions)
    ],
    "InsertDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
}

# Generate Hospital 2 transaction data
hospital2_transaction_data = {
    "TransactionID": [
        f"TRANS{str(i).zfill(6)}" for i in range(1, num_transactions + 1)
    ],
    "EncounterID": [
        f"ENC{str(random.randint(1, 10000)).zfill(6)}" for _ in range(num_transactions)
    ],
    "PatientID": [
        f"HOSP2-{str(random.randint(1, 5000)).zfill(6)}"
        for _ in range(num_transactions)
    ],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_transactions)
    ],
    "DeptID": [
        f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_transactions)
    ],
    "VisitDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "ServiceDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "PaidDate": [
        fake.date_this_year(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "VisitType": [random.choice(visit_types) for _ in range(num_transactions)],
    "Amount": [round(random.uniform(50, 1000), 2) for _ in range(num_transactions)],
    "AmountType": [random.choice(amount_types) for _ in range(num_transactions)],
    "PaidAmount": [round(random.uniform(20, 800), 2) for _ in range(num_transactions)],
    "ClaimID": [
        f"CLAIM{str(random.randint(100000, 999999))}" for _ in range(num_transactions)
    ],
    "PayorID": [
        f"PAYOR{str(random.randint(1000, 9999))}" for _ in range(num_transactions)
    ],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_transactions)],
    "ICDCode": [random.choice(icd_codes) for _ in range(num_transactions)],
    "LineOfBusiness": [
        random.choice(line_of_business) for _ in range(num_transactions)
    ],
    "MedicaidID": [
        f"MEDI{str(random.randint(10000, 99999))}" for _ in range(num_transactions)
    ],
    "MedicareID": [
        f"MCARE{str(random.randint(10000, 99999))}" for _ in range(num_transactions)
    ],
    "InsertDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_transactions)
    ],
}

# Create DataFrames
hospital1_transactions_df = pd.DataFrame(hospital1_transaction_data)
hospital2_transactions_df = pd.DataFrame(hospital2_transaction_data)

# file_name1 = "transactions_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path1 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-a/{file_name1}"
# hospital1_transactions_df.to_csv(local_file_path1, index=False)
# s3_key1 = f"Datasets/EMR/hos-a/{file_name1}"

# s3_client.upload_file(local_file_path1, bucket_name, s3_key1)
# print(f"File '{local_file_path1}' uploaded to 's3://{bucket_name}/{s3_key1}'")


# file_name2 = "transactions_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path2 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-b/{file_name2}"
# hospital2_transactions_df.to_csv(local_file_path2, index=False)
# s3_key2 = f"Datasets/EMR/hos-b/{file_name2}"

# s3_client.upload_file(local_file_path2, bucket_name, s3_key2)
# print(f"File '{local_file_path2}' uploaded to 's3://{bucket_name}/{s3_key2}'")

# ---------------------------#---------------------#------------------------#-------------

# Encounters data
# Parameters for data generation
num_encounters = 10000  # Number of encounter records per hospital
encounter_types = [
    "Inpatient",
    "Outpatient",
    "Emergency",
    "Telemedicine",
    "Routine Checkup",
]
cpt_codes = [str(random.randint(10000, 99999)) for _ in range(1000)]  # Sample CPT codes

# Generate Hospital 1 encounter data
hospital1_encounter_data = {
    "EncounterID": [f"ENC{str(i).zfill(6)}" for i in range(1, num_encounters + 1)],
    "PatientID": [
        f"HOSP1-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_encounters)
    ],
    "EncounterDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
    "EncounterType": [random.choice(encounter_types) for _ in range(num_encounters)],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_encounters)
    ],
    "DepartmentID": [
        f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_encounters)
    ],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_encounters)],
    "InsertedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
}

# Generate Hospital 2 encounter data
hospital2_encounter_data = {
    "EncounterID": [f"ENC{str(i).zfill(6)}" for i in range(1, num_encounters + 1)],
    "PatientID": [
        f"HOSP2-{str(random.randint(1, 5000)).zfill(6)}" for _ in range(num_encounters)
    ],
    "EncounterDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
    "EncounterType": [random.choice(encounter_types) for _ in range(num_encounters)],
    "ProviderID": [
        f"PROV{str(random.randint(1, 500)).zfill(4)}" for _ in range(num_encounters)
    ],
    "DepartmentID": [
        f"DEPT{str(random.randint(1, 20)).zfill(3)}" for _ in range(num_encounters)
    ],
    "ProcedureCode": [random.choice(cpt_codes) for _ in range(num_encounters)],
    "InsertedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
    "ModifiedDate": [
        fake.date_this_decade(before_today=True, after_today=False)
        for _ in range(num_encounters)
    ],
}

# Create DataFrames
hospital1_encounters_df = pd.DataFrame(hospital1_encounter_data)
hospital2_encounters_df = pd.DataFrame(hospital2_encounter_data)

# file_name1 = "encounters_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path1 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-a/{file_name1}"
# hospital1_encounters_df.to_csv(local_file_path1, index=False)
# s3_key1 = f"Datasets/EMR/hos-a/{file_name1}"

# s3_client.upload_file(local_file_path1, bucket_name, s3_key1)
# print(f"File '{local_file_path1}' uploaded to 's3://{bucket_name}/{s3_key1}'")


# file_name2 = "encounters_" + now.strftime("%Y-%m-%d_%H-%M") + ".csv"
# local_file_path2 = f"D:/HealtCare-Finance Project/Datasets/EMR/hos-b/{file_name2}"
# hospital2_encounters_df.to_csv(local_file_path2, index=False)
# s3_key2 = f"Datasets/EMR/hos-b/{file_name2}"

# s3_client.upload_file(local_file_path2, bucket_name, s3_key2)
# print(f"File '{local_file_path2}' uploaded to 's3://{bucket_name}/{s3_key2}'")
