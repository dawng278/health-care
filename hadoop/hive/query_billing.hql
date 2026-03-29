-- -----------------------------------------------------------------------------
-- Script: query_billing.hql
-- Mô tả: Phân tích chi tiết hóa đơn theo Admission Type và Insurance.
-- -----------------------------------------------------------------------------

USE healthcare_db;

-- Section 1 & 2: Admission Type & Insurance Provider Stats
INSERT OVERWRITE DIRECTORY '/healthcare/output/billing_stats'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT 'BY_ADMISSION' as section, admission_type as category, 
       min(billing_amount), max(billing_amount), round(avg(billing_amount), 2),
       percentile_approx(billing_amount, 0.5) as median
FROM patients GROUP BY admission_type

UNION ALL

SELECT 'BY_INSURANCE' as section, insurance_provider as category, 
       min(billing_amount), max(billing_amount), round(avg(billing_amount), 2),
       percentile_approx(billing_amount, 0.5) as median
FROM patients GROUP BY insurance_provider;
