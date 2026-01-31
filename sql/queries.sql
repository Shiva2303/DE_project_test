-- Sample SQL Queries

-- Query 1: Basic select
SELECT * FROM healthcare_data LIMIT 10;

-- Query 2: Count records
SELECT COUNT(*) as total_records FROM healthcare_data;

-- Query 3: Group by department
SELECT department, COUNT(*) as patient_count 
FROM healthcare_data 
GROUP BY department;

-- Query 4: Data quality check
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN patient_id IS NULL THEN 1 END) as null_patient_ids,
    COUNT(CASE WHEN visit_date IS NULL THEN 1 END) as null_visit_dates
FROM healthcare_data;
