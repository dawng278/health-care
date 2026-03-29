-- -----------------------------------------------------------------------------
-- Script: query_admission.hql
-- Mô tả: Phân tích loại hình nhập viện và thời gian lưu trú (Length of Stay).
-- -----------------------------------------------------------------------------

USE healthcare_db;

INSERT OVERWRITE DIRECTORY '/healthcare/output/admission_stats'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
    admission_type,
    count(1) as count,
    round(avg(datediff(discharge_date, date_of_admission)), 1) as avg_length_of_stay,
    round(count(1) * 100.0 / (SELECT COUNT(*) FROM patients), 2) as percentage
FROM patients
GROUP BY admission_type;
