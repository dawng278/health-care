-- -----------------------------------------------------------------------------
-- Script: create_tables.hql
-- Mô tả: Khởi tạo database và bảng External Table map với dữ liệu trên HDFS.
-- -----------------------------------------------------------------------------

CREATE DATABASE IF NOT EXISTS healthcare_db;
USE healthcare_db;

DROP TABLE IF EXISTS patients;

CREATE EXTERNAL TABLE IF NOT EXISTS patients (
    name                STRING,
    age                 INT,
    gender              STRING,
    blood_type          STRING,
    medical_condition   STRING,
    date_of_admission   STRING, -- Dạng YYYY-MM-DD
    doctor              STRING,
    hospital            STRING,
    insurance_provider  STRING,
    billing_amount      DOUBLE,
    room_number         INT,
    admission_type      STRING,
    discharge_date      STRING, -- Dạng YYYY-MM-DD
    medication          STRING,
    test_results        STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/healthcare/raw'
TBLPROPERTIES ("skip.header.line.count"="1");

DESCRIBE FORMATTED patients;
