from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.utils.dates import days_ago
import pandas as pd
import random
import pytz

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Helathcare_Data_Analysis_Job',
    default_args=default_args,
    description='A DAG to run Helathcare_Data_Analysis job on Dataproc',
    schedule_interval=timedelta(days=1),
    #start_date=days_ago(1),
    start_date=datetime(2024,2,24),
    catchup=False,
    tags=['example'],
)


# Check if execution_date is provided manually, otherwise use the default execution date
# date_variable = "{{ ds_nodash }}"
date_variable = "{{ dag_run.conf['execution_date'] if dag_run and dag_run.conf and 'execution_date' in dag_run.conf else ds_nodash }}"


#Operators

#1 --> Implementing the File sensor task ...
# Sense the new file in GCS
sense_patients_records_file = GCSObjectsWithPrefixExistenceSensor(
    task_id='sense_patients_records',
    bucket='yogesh-projects-data',
    prefix='HealthCareAnalysis/dailyCsvFile/health_data_',
    mode='poke',
    timeout=300,
    poke_interval=30,
    dag=dag
)


#2--> Operator for PySpark Job
# Define cluster config
# Fetch configuration from Airflow variables
config = Variable.get("cluster_details", deserialize_json=True)
CLUSTER_NAME = config['CLUSTER_NAME']
PROJECT_ID = config['PROJECT_ID']
REGION = config['REGION']

pyspark_job = {
    'main_python_file_uri': 'gs://yogesh-projects-data/HealthCareAnalysis/SparkJob/healthcare_data_analsis_pyspark_app.py'
}

submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    arguments=[f"--date={date_variable}"],  # Passing date as an argument to the PySpark script
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)


# 3--> Bash Operator to move file into the Archived directory...
move_file_task = BashOperator(
    task_id='move_file',
    bash_command=f'gsutil mv gs://yogesh-projects-data/HealthCareAnalysis/dailyCsvFile/*.csv gs://yogesh-projects-data/HealthCareAnalysis/ArchivedCsvFiles/',
    dag=dag,
)





sense_patients_records_file >> submit_pyspark_job >> move_file_task

