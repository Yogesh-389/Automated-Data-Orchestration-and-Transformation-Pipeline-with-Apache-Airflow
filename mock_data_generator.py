import pandas as pd
import random
from datetime import datetime, timedelta
import pytz
#from google.cloud import storage

def curr_date_random_time():
    # Get current date
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    # Generate random time
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)

    # Combine date and random time
    random_time = datetime(yesterday.year, yesterday.month, yesterday.day, random_hour, random_minute, random_second)
    return random_time

def generate_unique_patient_id(used_ids):
    while True:
        patient_id = f"P{random.randint(1, 1000)}"
        if patient_id not in used_ids:
            used_ids.add(patient_id)
            return patient_id

def upload_to_cloud():
    utc_now = datetime.utcnow()
    # Convert UTC time to IST timezone
    ist_timezone = pytz.timezone('Asia/Kolkata')
    ist_now = utc_now.astimezone(ist_timezone)
    # Format the IST time as needed
    #ist_now_str = ist_now.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    curr_date = ist_now.strftime(format = "%Y-%m-%d")
    records = []
    genders = ["M", "F"]
    used_ids = set() # it is used to keep track of used patient ids
    for _ in range(1000):
        patient_id = generate_unique_patient_id(used_ids)               #"P" + str(random.randint(1, 1000)).zfill(4)
        age = random.randint(30, 90)
        gender = random.choice(genders)
        diagnosis_code, diagnosis_description = random.choice([
            ("D123", "Diabetes"),
            ("H234", "High Blood Pressure"),
            ("C345", "Cancer"),
            ("A456", "Asthma"),
            ("I567", "Influenza"),
            ("H678", "Heart Disease")
        ])
        diagnosis_date = curr_date_random_time()
        records.append((patient_id, age, gender, diagnosis_code, diagnosis_description, diagnosis_date))

    df = pd.DataFrame(records, columns=["patient_id", "age", "gender", "diagnosis_code", "diagnosis_description", "diagnosis_date"])
    # ----- Writing to google-cloud-storage bucket
    df.to_csv(f'gs://yogesh-projects-data/HealthCareAnalysis/dailyCsvFile/health_data_{curr_date.replace("-", "")}.csv', index=False)
    #-----For Testing purpose writing in current directory
    #df.to_csv(f'health_data_{curr_date.replace("-", "")}.csv', index=False)

upload_to_cloud()