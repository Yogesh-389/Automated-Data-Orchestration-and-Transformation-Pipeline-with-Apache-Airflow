create or replace table famous-store-413904.health_data_analysis.health_data
(
  patient_id string, 
  age int64, 
  gender string, 
  diagnosis_description string, 
  diagnosis_date timestamp, 
  age_category string, 
  senior_citizen_flag string,
  load_time timestamp default current_timestamp()
);


SELECT * FROM famous-store-413904.health_data_analysis.health_data;