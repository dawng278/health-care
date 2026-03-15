#!/usr/bin/env ruby
# orchestrator.rb — Pipeline Orchestrator
# Chạy: ruby orchestrator.rb

require_relative 'config'
require_relative 'lib/hadoop_connector'
require_relative 'lib/data_loader'
require_relative 'lib/analytics'
require_relative 'lib/visualizer'
require 'fileutils'

puts '=' * 60
puts 'Healthcare Big Data Pipeline — Starting'
puts '=' * 60

# --- STEP 1: Upload raw data lên HDFS ---
puts "\n[Step 1/4] Uploading dataset to HDFS..."
HadoopConnector.upload(
  Config::DATASET_LOCAL,
  Config::HDFS_RAW_DIR
)

# --- STEP 2: Chạy Hive queries (Member 1 đã viết HQL) ---
puts "\n[Step 2/4] Running Hive queries..."
['query_age_dist', 'query_top_diseases', 'query_billing'].each do |q|
  HadoopConnector.run_hive(
    File.join(Config::HIVE_QUERY_DIR, "#{q}.hql")
  )
end

# --- STEP 3: Download output từ HDFS ---
puts "\n[Step 3/4] Fetching output from HDFS..."
age_csv  = File.join(Config::LOCAL_OUTPUT_DIR, 'age_distribution.csv')
HadoopConnector.download(
  "#{Config::HDFS_OUTPUT_DIR}/age_dist", age_csv
)

# --- STEP 4: Phân tích + Visualization ---
puts "\n[Step 4/4] Running analytics and generating charts..."
raw_data   = DataLoader.load_csv(age_csv)         # M2
analytics  = Analytics.run(raw_data)              # M3
Visualizer.render(analytics)                       # M4

puts "\n[DONE] Dashboard: #{Config::CHART_OUTPUT_DIR}/dashboard.html"
