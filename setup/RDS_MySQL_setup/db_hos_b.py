import mysql.connector

conn = mysql.connector.connect(
    host="hosp-fin-mgmt-db.c9kqkg8qmk5g.ap-south-1.rds.amazonaws.com",
    user="admin",
    password="PriyankaM2794",
)

cursor = conn.cursor()
print("Connected Successfully!")

# For hos-b
cursor.execute("""CREATE DATABASE hos_b;""")

# 1. Departments Table:
departments_query = """
CREATE TABLE hos_b.departments (
    DeptID nvarchar(50) NOT NULL,
    Name nvarchar(50) NOT NULL,
    CONSTRAINT PK_departments PRIMARY KEY (DeptID)
);
"""
cursor.execute(departments_query)
print("Departments Table created successfuly")


# 2. Encounters Table:
encounters_query = """
CREATE TABLE hos_b.encounters (
    EncounterID nvarchar(50) NOT NULL,
    PatientID nvarchar(50) NOT NULL,
    EncounterDate date NOT NULL,
    EncounterType nvarchar(50) NOT NULL,
    ProviderID nvarchar(50) NOT NULL,
    DepartmentID nvarchar(50) NOT NULL,
    ProcedureCode int NOT NULL,
    InsertedDate date NOT NULL,
    ModifiedDate date NOT NULL,
    CONSTRAINT PK_encounters PRIMARY KEY (EncounterID)
);
"""
cursor.execute(encounters_query)
print("Encounters Table created successfuly")

# 3. Hospital2_Patient_Data Table:
patients_query = """
CREATE TABLE hos_b.patients (
    ID nvarchar(50) NOT NULL,
    F_Name nvarchar(50) NOT NULL,
    L_Name nvarchar(50) NOT NULL,
    M_Name nvarchar(50) NOT NULL,
    SSN nvarchar(50) NOT NULL,
    PhoneNumber nvarchar(50) NOT NULL,
    Gender nvarchar(50) NOT NULL,
    DOB date NOT NULL,
    Address nvarchar(100) NOT NULL,
    Updated_Date date NOT NULL,
    CONSTRAINT PK_patients PRIMARY KEY (ID)
);
"""
cursor.execute(patients_query)
print("Patients Table created successfuly")

# 4. Providers Table:
providers_query = """
CREATE TABLE hos_b.providers (
    ProviderID nvarchar(50) NOT NULL,
    FirstName nvarchar(50) NOT NULL,
    LastName nvarchar(50) NOT NULL,
    Specialization nvarchar(50) NOT NULL,
    DeptID nvarchar(50) NOT NULL,
    NPI bigint NOT NULL,
    CONSTRAINT PK_providers PRIMARY KEY (ProviderID)
);
"""
cursor.execute(providers_query)
print("Providers Table created successfuly")

# 5. Transactions Table:
transactions_query = """
CREATE TABLE hos_b.transactions (
    TransactionID nvarchar(50) NOT NULL,
    EncounterID nvarchar(50) NOT NULL,
    PatientID nvarchar(50) NOT NULL,
    ProviderID nvarchar(50) NOT NULL,
    DeptID nvarchar(50) NOT NULL,
    VisitDate date NOT NULL,
    ServiceDate date NOT NULL,
    PaidDate date NOT NULL,
    VisitType nvarchar(50) NOT NULL,
    Amount float NOT NULL,
    AmountType nvarchar(50) NOT NULL,
    PaidAmount float NOT NULL,
    ClaimID nvarchar(50) NOT NULL,
    PayorID nvarchar(50) NOT NULL,
    ProcedureCode int NOT NULL,
    ICDCode nvarchar(50) NOT NULL,
    LineOfBusiness nvarchar(50) NOT NULL,
    MedicaidID nvarchar(50) NOT NULL,
    MedicareID nvarchar(50) NOT NULL,
    InsertDate date NOT NULL,
    ModifiedDate date NOT NULL,
    CONSTRAINT PK_transactions PRIMARY KEY (TransactionID)
);
"""
cursor.execute(transactions_query)
print("Transactions Table created successfuly")

cursor.close()
conn.close()
