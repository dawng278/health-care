# =============================================================
# config.rb — SHARED CONFIGURATION (Đọc từ biến môi trường)
# Mọi thành viên chỉnh sửa file .env local, KHÔNG sửa file này
# =============================================================

require 'dotenv'
Dotenv.load('.env')  # Load biến môi trường từ .env

module Config
  # --- Hadoop / HDFS ---
  HADOOP_HOME     = ENV.fetch('HADOOP_HOME', '/usr/local/hadoop')
  HADOOP_BIN      = File.join(HADOOP_HOME, 'bin', 'hadoop')
  HIVE_BIN        = ENV.fetch('HIVE_BIN', '/usr/local/hive/bin/hive')
  HDFS_HOST       = ENV.fetch('HDFS_HOST', 'hdfs://localhost:9000')

  # --- HDFS Paths (dùng HDFS_HOST để prefix) ---
  HDFS_RAW_DIR    = "#{HDFS_HOST}/healthcare/raw"
  HDFS_OUTPUT_DIR = "#{HDFS_HOST}/healthcare/output"

  # --- Local Paths ---
  PROJECT_ROOT    = File.expand_path('..', __FILE__)
  LOCAL_DATA_DIR  = File.join(PROJECT_ROOT, 'hadoop', 'data', 'raw')
  LOCAL_OUTPUT_DIR= File.join(PROJECT_ROOT, 'output')
  JSON_OUTPUT_DIR = File.join(LOCAL_OUTPUT_DIR, 'json')
  CHART_OUTPUT_DIR= File.join(LOCAL_OUTPUT_DIR, 'charts')
  HIVE_QUERY_DIR  = File.join(PROJECT_ROOT, 'hadoop', 'hive')

  # --- Dataset ---
  DATASET_FILENAME = 'healthcare_dataset.csv'
  DATASET_LOCAL    = File.join(LOCAL_DATA_DIR, DATASET_FILENAME)

  # --- Analysis Params ---
  TOP_N_DISEASES  = ENV.fetch('TOP_N_DISEASES', '10').to_i
  AGE_BUCKETS     = [0, 18, 35, 50, 65, Float::INFINITY]
  AGE_LABELS      = ['0-18', '19-35', '36-50', '51-65', '65+']
end
