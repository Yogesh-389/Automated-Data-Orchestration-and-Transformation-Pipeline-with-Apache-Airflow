-- Problem 1
CREATE MATERIALIZED VIEW famous-store-413904.health_data_analysis.view_disease_gender_ratio AS (
  SELECT
    diagnosis_description ,
    count(case when gender='M' then 1 end) as male_count,  
    count(case when gender='F' then 1 end) as female_count
  FROM
    famous-store-413904.health_data_analysis.health_data
  GROUP BY
    diagnosis_description
);



-- -- Problem 2
CREATE MATERIALIZED VIEW famous-store-413904.health_data_analysis.view_top_disease AS (
  SELECT
    diagnosis_description ,
    count(*) as disease_count,
  FROM
    famous-store-413904.health_data_analysis.health_data
  GROUP BY
    diagnosis_description
);

-- Problem 3

CREATE MATERIALIZED VIEW famous-store-413904.health_data_analysis.view_age_category AS (
  SELECT
    age_category, diagnosis_description,
    count(*) as total_patient,
  FROM
    famous-store-413904.health_data_analysis.health_data
  GROUP BY
    age_category, diagnosis_description
);

-- Problem 4
CREATE MATERIALIZED VIEW famous-store-413904.health_data_analysis.view_daily_disease_trend AS (
  SELECT
    EXTRACT(MONTH FROM diagnosis_date) AS month, EXTRACT(DAY FROM diagnosis_date) AS day, diagnosis_description,
    count(*) as total_patient,
  FROM
    famous-store-413904.health_data_analysis.health_data
  GROUP BY
    EXTRACT(MONTH FROM diagnosis_date), EXTRACT(DAY FROM diagnosis_date), diagnosis_description
);





-- Queries According to problem statements 

-- Problem 1

SELECT diagnosis_description, male_count, female_count, Round(male_count/(male_count+female_count),2) as ratio
FROM famous-store-413904.health_data_analysis.view_disease_gender_ratio;

-- Problem 2

SELECT diagnosis_description, disease_count as disease_count FROM famous-store-413904.health_data_analysis.view_top_disease order by disease_count desc;

-- Problem 3 

SELECT * from famous-store-413904.health_data_analysis.view_age_category order by age_category;

-- 4 Problem 4

SELECT * from famous-store-413904.health_data_analysis.view_daily_disease_trend;
