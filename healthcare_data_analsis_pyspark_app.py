from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *
from pyspark import StorageLevel
import argparse

def main(date):

    # Create Spark session
    spark = SparkSession.builder \
        .appName("Health Care Data Analysis") \
        .getOrCreate()

    input_path = f"gs://yogesh-projects-data/HealthCareAnalysis/dailyCsvFile/health_data_{date}.csv"
    # Defining the Schema and read the csv file from the path
    # schema = StructType([
    #     StructField("patient_id", StringType(), True),
    #     StructField("age", IntegerType(), True),
    #     StructField("gender", StringType(), True),
    #     StructField("diagnosis_code", StringType(), True),
    #     StructField("diagnosis_description", StringType(), True),
    #     StructField("diagnosis_date", DateType(), True)
    # ])

    health_data_df = spark.read.format('csv').option('header', True).option('inferSchema',True).load(input_path)

    # Applying DATA CHECKS and VALIDATIONS
    #Checking all Mandatory Coloumns present
    #Try to do logging instead of print statements

    def checkMandatoryColoumns(df_cols, actual_cols):
        missing_cols = [col for col in actual_cols if col not in df_cols]
        if missing_cols:
            raise ValueError(f"Missing mandatory columns-> {', '.join(missing_cols)}")

    actual_cols = ["patient_id", "age", "gender", "diagnosis_code", "diagnosis_description", "diagnosis_date"]
    try:
        checkMandatoryColoumns(actual_cols, health_data_df.columns)
        print("All mandatory coloumns present...")
    except Exception as e:
        print("{e}")


    #Checking datatypes of all the coloumns there..
        
    def checkDataType(df_types, actual_types):
        mismatched_cols = [key for key in actual_types if df_types[key]!=actual_types[key]]
        if mismatched_cols:
            raise ValueError(f"Mismatch datatype columns-> {', '.join(mismatched_cols)}")


    df_col_types = dict(health_data_df.dtypes)
    actual_col_types = {'patient_id': 'string', 'age': 'int', 'gender': 'string', 'diagnosis_code': 'string', 'diagnosis_description': 'string', 'diagnosis_date': 'timestamp'}

    try:
        checkDataType(df_col_types, actual_col_types)
        print("All coloumns datatypes matched...")
    except Exception as e:
        print(f"{e}")


    #Handling the missing values in the dataframe
    cleaned_health_data_df = health_data_df.dropna(subset=["age", "gender", "diagnosis_description", "diagnosis_date"])

    #Persisting the dataframe as it is using in several locations
    cleaned_health_data_df = cleaned_health_data_df.persist(StorageLevel.MEMORY_ONLY)

    #Transformations 

    # Adding the Age category col, senior citizen col, and dropping diagonosis code col.. 
    transformed_health_data_df = cleaned_health_data_df.withColumn("age_category", when((col('age')>=30) & (col('age')<=40), '30-40')
                                                                .when((col('age')>=41) & (col('age')<=50), '40-50')
                                                                .when((col('age')>=51) & (col('age')<=60), '50-60')
                                                                .when((col('age')>=61) & (col('age')<=70), '60-70')
                                                                .when((col('age')>=71) & (col('age')<=80), '70-80')
                                                                .when((col('age')>=81) & (col('age')<=90), '80-90')
                                                                )\
                                                        .withColumn("senior_citizen_flag", when((col('age')>=60) & (col('age')<=90), 'senior_citizen')
                                                                                                .otherwise('null')
                                                                    )\
                                                        .drop(col('diagnosis_code'))


    #Loading transformed df into the table...of BigQuery

    dataset_id = "famous-store-413904.health_data_analysis"

    transformed_health_data_df.write \
    .format("bigquery") \
    .option("dataset", dataset_id) \
    .option("table", 'famous-store-413904.health_data_analysis.health_data') \
    .option("temporaryGcsBucket", "yogesh-projects-data") \
    .mode("append")\
    .save()

    print("Data write successful in target table !!")

    # Stop the SparkSession
    spark.stop()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process date argument')
    parser.add_argument('--date', type=str, required=True, help='Date in yyyymmdd format')
    args = parser.parse_args()
    
    main(args.date)