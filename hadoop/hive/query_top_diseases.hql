-- -----------------------------------------------------------------------------
-- Script: query_top_diseases.hql
-- Mô tả: Thống kê Top 10 bệnh phổ biến và chi phí trung bình.
-- -----------------------------------------------------------------------------

USE healthcare_db;

INSERT OVERWRITE DIRECTORY '/healthcare/output/top_diseases'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
    medical_condition as disease_name,
    count(1) as patient_count,
    round(avg(billing_amount), 2) as avg_billing,
    round(count(1) * 100.0 / (SELECT COUNT(*) FROM patients), 2) as percentage
FROM patients
GROUP BY medical_condition
ORDER BY patient_count DESC
LIMIT 10;
