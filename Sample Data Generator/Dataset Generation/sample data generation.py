# Databricks notebook source
!pip install faker

# COMMAND ----------

import csv
import io
from faker import Faker
from datetime import datetime

fake = Faker()

# Generate fake data for Patients table (900 samples)
patients_data = []
for _ in range(2500):
    patient_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    patient_data = {
        'PatientID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'DateOfBirth': fake.date_of_birth(minimum_age=18, maximum_age=100),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'Email': fake.email(),
        'CreatedAt': patient_created_at
    }
    patients_data.append(patient_data)

# Generate fake data for MedicalStaff table 800 samples)
medical_staff_data = []
for _ in range(150):
    staff_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    staff_data = {
        'StaffID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Specialization': fake.random_element(elements=('Cardiology', 'Pediatrics', 'Orthopedics', 'Gynecology')),
        'LicenseNumber': fake.unique.random_number(digits=8),
        'CreatedAt': staff_created_at
    }
    medical_staff_data.append(staff_data)

# Generate fake data for Hospitals table (40 samples)
hospitals_data = []
for _ in range(60):
    hospital_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    hospital_data = {
        'HospitalID': fake.unique.random_number(digits=6),
        'HospitalName': fake.company(),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'CreatedAt': hospital_created_at
    }
    hospitals_data.append(hospital_data)

# Generate fake data for Billing (Fact) table (10,000 samples)
billing_data = []
for _ in range(100000):
    patient_id = fake.random_element(patients_data)['PatientID']
    staff_id = fake.random_element(medical_staff_data)['StaffID']
    hospital_id = fake.random_element(hospitals_data)['HospitalID']
    billing_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    billing_record = {
        'BillingID': fake.unique.random_number(digits=6),
        'PatientID': patient_id,
        'StaffID': staff_id,
        'HospitalID': hospital_id,
        'BillingDate': billing_created_at,
        'Amount': fake.random_int(min=50, max=5000),
        'Description': fake.text(),
        'CreatedAt': billing_created_at
    }
    billing_data.append(billing_record)

# Convert data to CSV strings
patients_csv = io.StringIO()
patients_writer = csv.DictWriter(patients_csv, fieldnames=patients_data[0].keys())
patients_writer.writeheader()
patients_writer.writerows(patients_data)

medical_staff_csv = io.StringIO()
medical_staff_writer = csv.DictWriter(medical_staff_csv, fieldnames=medical_staff_data[0].keys())
medical_staff_writer.writeheader()
medical_staff_writer.writerows(medical_staff_data)

hospitals_csv = io.StringIO()
hospitals_writer = csv.DictWriter(hospitals_csv, fieldnames=hospitals_data[0].keys())
hospitals_writer.writeheader()
hospitals_writer.writerows(hospitals_data)

billing_csv = io.StringIO()
billing_writer = csv.DictWriter(billing_csv, fieldnames=billing_data[0].keys())
billing_writer.writeheader()
billing_writer.writerows(billing_data)

# Define Azure DBFS paths
patients_dbfs_path = "/mnt/late/patients.csv"
medical_staff_dbfs_path = "/mnt/late/medical_staff.csv"
hospitals_dbfs_path = "/mnt/late/hospitals.csv"
billing_dbfs_path = "/mnt/late/billing.csv"

# Save data to Azure DBFS
dbutils.fs.put(patients_dbfs_path, patients_csv.getvalue())
dbutils.fs.put(medical_staff_dbfs_path, medical_staff_csv.getvalue())
dbutils.fs.put(hospitals_dbfs_path, hospitals_csv.getvalue())
dbutils.fs.put(billing_dbfs_path, billing_csv.getvalue())

print(f"Patients data saved to: {patients_dbfs_path}")
print(f"Medical Staff data saved to: {medical_staff_dbfs_path}")
print(f"Hospitals data saved to: {hospitals_dbfs_path}")
print(f"Billing data saved to: {billing_dbfs_path}")


# COMMAND ----------

df_patients  = spark.read.csv("/mnt/late/patients.csv", header=True, inferSchema=True)
display(df_patients)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsledemo001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsledemo001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsledemo001.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-01T03:42:44Z&st=2023-10-12T19:42:44Z&spr=https&sig=p6fWGyMnwKMNTDMBRx5J%2B8cTEYkqNujm00xD%2FZafLZk%3D")

df_patients.write.format("csv").save(path) 

# COMMAND ----------

df_staff = spark.read.csv("/mnt/late/medical_staff.csv", header=True, inferSchema=True)
display(df_staff)

# COMMAND ----------

df_hospitals = spark.read.csv("/mnt/late/hospitals.csv", header=True, inferSchema=True)
display(df_hospitals)

# COMMAND ----------

df_billing1 = spark.read.csv("/mnt/late/billing.csv", header=True, inferSchema=True)
display(df_billing1)

# COMMAND ----------

df_billing= df_billing1.dropna()
display(df_billing)

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adlsledemo001.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.adlsledemo001.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.adlsledemo001.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-12-01T03:42:44Z&st=2023-10-12T19:42:44Z&spr=https&sig=p6fWGyMnwKMNTDMBRx5J%2B8cTEYkqNujm00xD%2FZafLZk%3D")

df_patients.write.format("csv").option("header", "true").mode("overwrite").save("abfss://sampledataset@adlsledemo001.dfs.core.windows.net/Patient/patient.csv")
df_staff.write.format("csv").mode("overwrite").option("header", "true").save("abfss://sampledataset@adlsledemo001.dfs.core.windows.net/MedicalStaff/staff.csv")
df_hospitals.write.format("csv").mode("overwrite").option("header", "true").save("abfss://sampledataset@adlsledemo001.dfs.core.windows.net/Hospitals/hospitals.csv")
df_billing.write.format("csv").mode("overwrite").option("header", "true").save("abfss://sampledataset@adlsledemo001.dfs.core.windows.net/Billing/billing.csv")

# COMMAND ----------

# Perform left joins to merge data based on foreign key relationships

merged_data = df_billing1.join(df_hospitals, "HospitalID", "left") \
                     .join(df_staff , "StaffID", "left") \
                     .join(df_patients , "PatientID", "left") 
display(merged_data)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import csv
import io
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Generate fake data for Patients table
patients_data = []
for _ in range(900):
    patient_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    patient_data = {
        'PatientID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'DateOfBirth': fake.date_of_birth(minimum_age=18, maximum_age=100),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'Email': fake.email(),
        'CreatedAt': patient_created_at
    }
    patients_data.append(patient_data)

# Generate fake data for MedicalStaff table
medical_staff_data = []
for _ in range(150):
    staff_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    staff_data = {
        'StaffID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Specialization': fake.random_element(elements=('Cardiology', 'Pediatrics', 'Orthopedics', 'Gynecology')),
        'LicenseNumber': fake.unique.random_number(digits=8),
        'CreatedAt': staff_created_at
    }
    medical_staff_data.append(staff_data)

# Generate fake data for Hospitals table
hospitals_data = []
for _ in range(40):
    hospital_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    hospital_data = {
        'HospitalID': fake.unique.random_number(digits=6),
        'HospitalName': fake.company(),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'CreatedAt': hospital_created_at
    }
    hospitals_data.append(hospital_data)

# Generate fake data for Billing (Fact) table
billing_data = []
for _ in range(200000):
    patient_id = fake.random_element(patients_data)['PatientID']
    staff_id = fake.random_element(medical_staff_data)['StaffID']
    hospital_id = fake.random_element(hospitals_data)['HospitalID']
    billing_created_at = fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    billing_record = {
        'BillingID': fake.unique.random_number(digits=6),
        'PatientID': patient_id,
        'StaffID': staff_id,
        'HospitalID': hospital_id,
        'BillingDate': billing_created_at.strftime('%Y-%m-%d %H:%M:%S'),  # Format as string
        'Amount': fake.random_int(min=50, max=5000),
        'Description': fake.text(),
        'CreatedAt': billing_created_at.strftime('%Y-%m-%d %H:%M:%S')  # Format as string
    }
    billing_data.append(billing_record)

# Convert data to CSV strings
patients_csv = io.StringIO()
patients_writer = csv.DictWriter(patients_csv, fieldnames=patients_data[0].keys())
patients_writer.writeheader()
patients_writer.writerows(patients_data)

medical_staff_csv = io.StringIO()
medical_staff_writer = csv.DictWriter(medical_staff_csv, fieldnames=medical_staff_data[0].keys())
medical_staff_writer.writeheader()
medical_staff_writer.writerows(medical_staff_data)

hospitals_csv = io.StringIO()
hospitals_writer = csv.DictWriter(hospitals_csv, fieldnames=hospitals_data[0].keys())
hospitals_writer.writeheader()
hospitals_writer.writerows(hospitals_data)

billing_csv = io.StringIO()
billing_writer = csv.DictWriter(billing_csv, fieldnames=billing_data[0].keys())
billing_writer.writeheader()
billing_writer.writerows(billing_data)

# Define Azure DBFS paths
patients_dbfs_path = "/mnt/finaldata/patients.csv"
medical_staff_dbfs_path = "/mnt/finaldata/medical_staff.csv"
hospitals_dbfs_path = "/mnt/finaldata/hospitals.csv"
billing_dbfs_path = "/mnt/finaldata/billing.csv"

# Save data to Azure DBFS
dbutils.fs.put(patients_dbfs_path, patients_csv.getvalue())
dbutils.fs.put(medical_staff_dbfs_path, medical_staff_csv.getvalue())
dbutils.fs.put(hospitals_dbfs_path, hospitals_csv.getvalue())
dbutils.fs.put(billing_dbfs_path, billing_csv.getvalue())

print(f"Patients data saved to: {patients_dbfs_path}")
print(f"Medical Staff data saved to: {medical_staff_dbfs_path}")
print(f"Hospitals data saved to: {hospitals_dbfs_path}")
print(f"Billing data saved to: {billing_dbfs_path}")


# COMMAND ----------

df_patients = spark.read.csv("/mnt/finaldata/patients.csv", header=True, inferSchema=True)
display(df_patients)

# COMMAND ----------

df_staff = spark.read.csv("/mnt/data/medical_staff.csv", header=True, inferSchema=True)
display(df_staff)

# COMMAND ----------

df_hospitals = spark.read.csv("/mnt/data/hospitals.csv", header=True, inferSchema=True)
display(df_hospitals)

# COMMAND ----------

df_bill = spark.read.csv("/mnt/finaldata/billing.csv", header=True, inferSchema=True)
display(df_bill)

# COMMAND ----------

df_bill1 = df_bill.dropna()
display(df_bill1)


# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %fs ls dbfs:/dbfs:/FileStore

# COMMAND ----------

from faker import Faker
import csv
import io
import random
from datetime import datetime, timedelta

# Initialize the faker instance
fake = Faker()

# Function to generate random modified date between 2010 and 2023
def generate_modified_date(start_date, end_date):
    return fake.date_time_between_dates(datetime_start=start_date, datetime_end=end_date)

# Function to generate patient records
def generate_patient_record():
    modified_date = generate_modified_date(datetime(2010, 1, 1), datetime(2023, 1, 1))
    return {
        'PatientID': fake.unique.random_number(digits=6, fix_len=True),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'DateOfBirth': fake.date_of_birth(minimum_age=0, maximum_age=120),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'Email': fake.email(),
        'EmergencyContactName': fake.name(),
        'EmergencyContactPhone': fake.phone_number(),
        'ModifiedDate': modified_date.strftime('%Y-%m-%d %H:%M:%S')
    }

# Function to generate doctor records
def generate_doctor_record():
    modified_date = generate_modified_date(datetime(2010, 1, 1), datetime(2023, 1, 1))
    return {
        'DoctorID': fake.unique.random_number(digits=6, fix_len=True),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Specialization': fake.random_element(elements=('Cardiology', 'Pediatrics', 'Orthopedics')),
        'LicenseNumber': fake.unique.random_number(digits=8, fix_len=True),
        'ModifiedDate': modified_date.strftime('%Y-%m-%d %H:%M:%S')
    }

# Function to generate random modified date between start_date and end_date
def generate_modified_date(start_date, end_date):
    if start_date > end_date:
        start_date, end_date = end_date, start_date  # Swap start_date and end_date if start_date is later than end_date
    return fake.date_time_between_dates(datetime_start=start_date, datetime_end=end_date)
    return {
        'AppointmentID': fake.unique.random_number(digits=6, fix_len=True),
        'PatientID': patient_id,
        'DoctorID': doctor_id,
        'AppointmentDate': fake.date_this_year(),
        'AppointmentTime': fake.time(pattern='%H:%M:%S'),
        'Purpose': fake.sentence(),
        'Status': fake.random_element(elements=('Confirmed', 'Pending', 'Canceled')),
        'ModifiedDate': modified_date.strftime('%Y-%m-%d %H:%M:%S')
    }

# Function to generate test results records
def generate_test_results_record(patient_id):
    modified_date = generate_modified_date(datetime.strptime(patients_data[patient_id]['ModifiedDate'], '%Y-%m-%d %H:%M:%S'),
                                           datetime.now())
    return {
        'ResultID': fake.unique.random_number(digits=6, fix_len=True),
        'PatientID': patient_id,
        'TestName': fake.random_element(elements=('Blood Test', 'X-Ray', 'MRI')),
        'TestDate': fake.date_this_year(),
        'ResultDetails': fake.sentence(),
        'ModifiedDate': modified_date.strftime('%Y-%m-%d %H:%M:%S')
    }

# Function to generate billing records
def generate_billing_record(patient_id):
    modified_date = generate_modified_date(datetime.strptime(patients_data[patient_id]['ModifiedDate'], '%Y-%m-%d %H:%M:%S'),
                                           datetime.now())
    total_amount = round(random.uniform(100, 10000), 2)  # Random decimal number between 100 and 10000 with 2 decimal places
    return {
        'BillID': fake.unique.random_number(digits=6, fix_len=True),
        'PatientID': patient_id,
        'TotalAmount': total_amount,
        'PaymentStatus': fake.random_element(elements=('Paid', 'Unpaid')),
        'BillingDate': fake.date_this_year(),
        'ModifiedDate': modified_date.strftime('%Y-%m-%d %H:%M:%S')
    }

# Generate patient records
patients_data = []
for _ in range(15000):  # Generate 1000 patient records
    patient_record = generate_patient_record()
    patients_data.append(patient_record)

# Generate doctor records
doctors_data = []
for _ in range(50):  # Generate 50 doctor records
    doctor_record = generate_doctor_record()
    doctors_data.append(doctor_record)

# Generate appointment records
appointments_data = []
for _ in range(8000):  # Generate 2000 appointment records
    patient_id = fake.random_element(range(len(patients_data)))  # Randomly choose a patient ID
    doctor_id = fake.random_element(range(len(doctors_data)))  # Randomly choose a doctor ID
    appointment_record = generate_appointment_record(patient_id, doctor_id)
    appointments_data.append(appointment_record)

# Generate test results records
test_results_data = []
for _ in range(3000):  # Generate 1500 test results records
    patient_id = fake.random_element(range(len(patients_data)))  # Randomly choose a patient ID
    test_results_record = generate_test_results_record(patient_id)
    test_results_data.append(test_results_record)

# Generate billing records
billing_data = []
for _ in range(7500):  # Generate 800 billing records
    patient_id = fake.random_element(range(len(patients_data)))  # Randomly choose a patient ID
    billing_record = generate_billing_record(patient_id)
    billing_data.append(billing_record)

# Convert data to CSV strings
patients_csv = io.StringIO()
patients_writer = csv.DictWriter(patients_csv, fieldnames=patients_data[0].keys())
patients_writer.writeheader()
patients_writer.writerows(patients_data)

doctors_csv = io.StringIO()
doctors_writer = csv.DictWriter(doctors_csv, fieldnames=doctors_data[0].keys())
doctors_writer.writeheader()
doctors_writer.writerows(doctors_data)


appointments_csv = io.StringIO()
appointments_writer = csv.DictWriter(appointments_csv, fieldnames=appointments_data[0].keys())
appointments_writer.writeheader()
appointments_writer.writerows(appointments_data)

test_results_csv = io.StringIO()
test_results_writer = csv.DictWriter(test_results_csv, fieldnames=test_results_data[0].keys())
test_results_writer.writeheader()
test_results_writer.writerows(test_results_data)

billing_csv = io.StringIO()
billing_writer = csv.DictWriter(billing_csv, fieldnames=billing_data[0].keys())
billing_writer.writeheader()
billing_writer.writerows(billing_data)

# Define Azure DBFS paths
patients_dbfs_path = "dbfs:/dbfs:/FileStore/patients.csv"
doctors_dbfs_path = "dbfs:/dbfs:/FileStore/doctors.csv"
appointments_dbfs_path = "dbfs:/dbfs:/FileStore/appointments.csv"
test_results_dbfs_path = "dbfs:/dbfs:/FileStore/test_results.csv"
billing_dbfs_path = "dbfs:/dbfs:/FileStore/billing.csv"

# Save data to Azure DBFS
dbutils.fs.put(patients_dbfs_path, patients_csv.getvalue())
dbutils.fs.put(doctors_dbfs_path, doctors_csv.getvalue())
dbutils.fs.put(appointments_dbfs_path, appointments_csv.getvalue())
dbutils.fs.put(test_results_dbfs_path, test_results_csv.getvalue())
dbutils.fs.put(billing_dbfs_path, billing_csv.getvalue())

print(f"Patients data saved to: {patients_dbfs_path}")
print(f"Doctors data saved to: {doctors_dbfs_path}")
print(f"Appointments data saved to: {appointments_dbfs_path}")
print(f"Test Results data saved to: {test_results_dbfs_path}")
print(f"Billing data saved to: {billing_dbfs_path}")


# COMMAND ----------

df_doctors = spark.read.csv("dbfs:/dbfs:/FileStore/doctors.csv", header=True, inferSchema=True)
display(df_doctors)

# COMMAND ----------

df_patient = spark.read.csv("dbfs:/dbfs:/FileStore/patients.csv", header=True, inferSchema=True)
display(df_patient)

# COMMAND ----------

df_appointments = spark.read.csv("dbfs:/dbfs:/FileStore/appointments.csv", header=True, inferSchema=True)
display(df_appointments)

# COMMAND ----------

df_test_results = spark.read.csv("dbfs:/dbfs:/FileStore/test_results.csv", header=True, inferSchema=True)
display(df_test_results)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from faker import Faker
import random
from datetime import datetime, timedelta
import csv
import io

fake = Faker()

# Generate fake data for Patients table
patients_data = []
for _ in range(3000):  # Generating 10000 patients
    patient_data = {
        'PatientID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'DateOfBirth': fake.date_of_birth(minimum_age=18, maximum_age=100),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'Email': fake.email(),
        'CreatedAt': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    }
    patients_data.append(patient_data)

# Generate fake data for MedicalStaff table
medical_staff_data = []
for _ in range(150):  # Generating 150 medical staff
    staff_data = {
        'StaffID': fake.unique.random_number(digits=6),
        'FirstName': fake.first_name(),
        'LastName': fake.last_name(),
        'Gender': fake.random_element(elements=('M', 'F')),
        'Specialization': fake.random_element(elements=('Cardiology', 'Pediatrics', 'Orthopedics', 'Gynecology')),
        'LicenseNumber': fake.unique.random_number(digits=8),
        'CreatedAt': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    }
    medical_staff_data.append(staff_data)

# Generate fake data for Hospitals table
hospitals_data = []
for _ in range(30):  # Generating 30 hospitals
    hospital_data = {
        'HospitalID': fake.unique.random_number(digits=6),
        'HospitalName': fake.company(),
        'Address': fake.address().replace('\n', ', '),
        'PhoneNumber': fake.phone_number(),
        'CreatedAt': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    }
    hospitals_data.append(hospital_data)

# Generate fake data for Appointments table
appointments_data = []
for _ in range(10000):  # Generating 5000 appointments
    patient_id = fake.random_element(patients_data)['PatientID']
    staff_id = fake.random_element(medical_staff_data)['StaffID']
    hospital_id = fake.random_element(hospitals_data)['HospitalID']
    appointment_data = {
        'AppointmentID': fake.unique.random_number(digits=6),
        'PatientID': patient_id,
        'StaffID': staff_id,
        'HospitalID': hospital_id,
        'AppointmentDate': fake.date_time_between_dates(datetime_start=fake.date_this_decade(), datetime_end='+30d'),
        'Purpose': fake.random_element(elements=('Consultation', 'Follow-up', 'Test', 'Treatment')),
        'Status': fake.random_element(elements=('Confirmed', 'Pending', 'Canceled')),
        'CreatedAt': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    }
    appointments_data.append(appointment_data)

# Generate fake data for TestResults table
test_results_data = []
for _ in range(3000):  # Generating 3000 test results
    patient_id = fake.random_element(patients_data)['PatientID']
    test_result_data = {
        'ResultID': fake.unique.random_number(digits=6),
        'PatientID': patient_id,
        'TestName': fake.random_element(elements=('Blood Test', 'X-Ray', 'MRI', 'Ultrasound')),
        'TestDate': fake.date_time_this_decade(before_now=True, after_now=False, tzinfo=None),
        'ResultDetails': fake.text(),
        'CreatedAt': fake.date_time_this_century(before_now=True, after_now=False, tzinfo=None)
    }
    test_results_data.append(test_result_data)

# Convert data to CSV strings
patients_csv = io.StringIO()
patients_writer = csv.DictWriter(patients_csv, fieldnames=patients_data[0].keys())
patients_writer.writeheader()
patients_writer.writerows(patients_data)

medical_staff_csv = io.StringIO()
medical_staff_writer = csv.DictWriter(medical_staff_csv, fieldnames=medical_staff_data[0].keys())
medical_staff_writer.writeheader()
medical_staff_writer.writerows(medical_staff_data)

hospitals_csv = io.StringIO()
hospitals_writer = csv.DictWriter(hospitals_csv, fieldnames=hospitals_data[0].keys())
hospitals_writer.writeheader()
hospitals_writer.writerows(hospitals_data)

appointments_csv = io.StringIO()
appointments_writer = csv.DictWriter(appointments_csv, fieldnames=appointments_data[0].keys())
appointments_writer.writeheader()
appointments_writer.writerows(appointments_data)

test_results_csv = io.StringIO()
test_results_writer = csv.DictWriter(test_results_csv, fieldnames=test_results_data[0].keys())
test_results_writer.writeheader()
test_results_writer.writerows(test_results_data)

# Define Azure DBFS paths
patients_dbfs_path = "/mnt/netcaredata/patients2.csv"
medical_staff_dbfs_path = "/mnt/netcaredata/medical_staff2.csv"
hospitals_dbfs_path = "/mnt/netcaredata/hospitals2.csv"
appointments_dbfs_path = "/mnt/netcaredata/appointments2.csv"
test_results_dbfs_path = "/mnt/netcaredata/test_results2.csv"

# Save data to Azure DBFS
dbutils.fs.put(patients_dbfs_path, patients_csv.getvalue())
dbutils.fs.put(medical_staff_dbfs_path, medical_staff_csv.getvalue())
dbutils.fs.put(hospitals_dbfs_path, hospitals_csv.getvalue())
dbutils.fs.put(appointments_dbfs_path, appointments_csv.getvalue())
dbutils.fs.put(test_results_dbfs_path, test_results_csv.getvalue())

print(f"Patients data saved to: {patients_dbfs_path}")
print(f"Medical Staff data saved to: {medical_staff_dbfs_path}")
print(f"Hospitals data saved to: {hospitals_dbfs_path}")
print(f"Appointments data saved to: {appointments_dbfs_path}")
print(f"Test Results data saved to: {test_results_dbfs_path}")



# COMMAND ----------

df_patients = spark.read.csv("/mnt/netcaredata/patients2.csv", header=True, inferSchema=True)
display(df_patients)

# COMMAND ----------

df_patients = spark.read.csv("/mnt/netcaredata/medical_staff2.csv", header=True, inferSchema=True)
display(df_patients)

# COMMAND ----------

df_appointments = spark.read.csv("/mnt/netcaredata/appointments2.csv", header=True, inferSchema=True)
display(df_appointments)
