-- -----------------------------------------------------------------------------
-- Script: query_age_dist.hql
-- Mô tả: Phân tích phân bố độ tuổi bệnh nhân.
-- -----------------------------------------------------------------------------

USE healthcare_db;

-- 1. Tính tổng số record để làm tỷ lệ
-- Note: Subqueries in Hive might need a name
INSERT OVERWRITE DIRECTORY '/healthcare/output/age_distribution'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT 
    t.age_group,
    count(1) as patient_count,
    round(count(1) * 100.0 / (SELECT COUNT(*) FROM patients), 2) as percentage
FROM (
    SELECT 
        CASE 
            WHEN age <= 18 THEN '0-18'
            WHEN age > 18 AND age <= 35 THEN '19-35'
            WHEN age > 35 AND age <= 50 THEN '36-50'
            WHEN age > 50 AND age <= 65 THEN '51-65'
            ELSE '65+'
        END as age_group
    FROM patients
) t
GROUP BY t.age_group
ORDER BY t.age_group;
