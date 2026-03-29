# -----------------------------------------------------------------------------
# Script: config.rb
# Mô tả: Cấu hình tập trung cho toàn bộ project.
# -----------------------------------------------------------------------------

require 'dotenv'
require 'fileutils'

# Tải biến môi trường
if File.exist?('.env')
  Dotenv.load('.env')
else
  # Chỉ in cảnh báo nếu không tìm thấy .env
  # (Sẽ được validate kỹ hơn trong Config.validate!)
end

module Config
  # --- Hadoop / HDFS Paths ---
  HADOOP_HOME     = ENV.fetch('HADOOP_HOME', '/usr/local/hadoop')
  HADOOP_BIN      = File.join(HADOOP_HOME, 'bin', 'hadoop')
  HIVE_BIN        = ENV.fetch('HIVE_BIN', '/usr/local/hive/bin/hive')
  HDFS_HOST       = ENV.fetch('HDFS_HOST', 'hdfs://localhost:9000')

  HDFS_RAW_DIR    = "#{HDFS_HOST}/healthcare/raw"
  HDFS_OUTPUT_DIR = "#{HDFS_HOST}/healthcare/output"

  # --- Local Path Configuration ---
  PROJECT_ROOT    = File.expand_path('..', __FILE__)
  LOCAL_DATA_DIR  = File.join(PROJECT_ROOT, 'hadoop', 'data', 'raw')
  LOCAL_OUTPUT_DIR= File.join(PROJECT_ROOT, 'output')
  JSON_OUTPUT_DIR = File.join(LOCAL_OUTPUT_DIR, 'json')
  CHART_OUTPUT_DIR= File.join(LOCAL_OUTPUT_DIR, 'charts')
  HIVE_QUERY_DIR  = File.join(PROJECT_ROOT, 'hadoop', 'hive')

  # --- Dataset & ML API ---
  DATASET_LOCAL   = File.join(LOCAL_DATA_DIR, 'healthcare_dataset.csv')
  FLASK_API_URL   = ENV.fetch('FLASK_API_URL', 'http://localhost:5000')
  TOP_N_DISEASES  = ENV.fetch('TOP_N_DISEASES', '10').to_i

  def self.validate!
    puts "[Step 1/5] Validating environment..."
    
    unless File.exist?('.env')
      puts "[WARNING] File .env chưa được tạo. Đang sử dụng giá trị mặc định."
    end

    unless File.exist?(HADOOP_BIN)
        raise "Hadoop binary không tồn tại tại #{HADOOP_BIN}. Hãy kiểm tra HADOOP_HOME trong .env"
    end

    # Tạo các thư mục output local nếu chưa có
    [LOCAL_OUTPUT_DIR, JSON_OUTPUT_DIR, CHART_OUTPUT_DIR].each do |dir|
      FileUtils.mkdir_p(dir) unless Dir.exist?(dir)
    end
    
    puts "  [OK] Environment validated."
  end
end
