#!/usr/bin/env ruby
# -----------------------------------------------------------------------------
# Script: orchestrator.rb
# Mô tả: Tổng quản điều phối toàn bộ pipeline Big Data.
# -----------------------------------------------------------------------------

require_relative 'config'
require_relative 'lib/hadoop_connector'
require_relative 'lib/data_loader'
require_relative 'lib/analytics'  # Đã hoàn thành
require_relative 'lib/visualizer' # Đã hoàn thành

puts "=" * 60
puts "🏥 Healthcare Analytics Pipeline - Orchestrator"
puts "=" * 60

begin
  # Step 1: Validate Environment
  Config.validate!

  # Step 2: Upload Files
  puts "\n[Step 2/5] Uploading dataset to HDFS..."
  if File.exist?(Config::DATASET_LOCAL)
    HadoopConnector.upload(Config::DATASET_LOCAL, Config::HDFS_RAW_DIR)
  else
    raise "Không tìm thấy file dataset tại #{Config::DATASET_LOCAL}"
  end

  # Step 3: Run Hive Analytics
  puts "\n[Step 3/5] Running Hive queries..."
  # create_tables cần chạy trước để khai báo Schema
  HadoopConnector.run_hive(File.join(Config::HIVE_QUERY_DIR, 'create_tables.hql'))
  
  tasks = ['query_age_dist', 'query_top_diseases', 'query_billing', 'query_admission']
  tasks.each do |t|
    hql_path = File.join(Config::HIVE_QUERY_DIR, "#{t}.hql")
    HadoopConnector.run_hive(hql_path)
  end

  # Step 4: Fetch Results
  puts "\n[Step 4/5] Fetching results from HDFS..."
  csv_files = DataLoader.fetch_all_outputs
  puts "  [OK] Toàn bộ dữ liệu đã được tải về #{Config::LOCAL_OUTPUT_DIR}"

  # Step 5: Finalize Visualization
  puts "\n[Step 5/5] Running analytics and generating dashboard..."
  analytics_data = Analytics.run(csv_files)
  html_path      = Visualizer.render(analytics_data)

  puts "\n" + "=" * 60
  puts "[SUCCESS] Pipeline completed successfully!"
  puts "Dashboard Path: #{html_path}"
  puts "=" * 60

rescue => e
  puts "\n" + "!" * 60
  puts "[FATAL ERROR] Pipeline stopped!"
  puts "Message: #{e.message}"
  puts "Gợi ý: Kiểm tra Hadoop/Hive status hoặc file .env của bạn."
  puts "!" * 60
  exit 1
end
