# -----------------------------------------------------------------------------
# Script: analytics.rb
# Mô tả: Cầu nối phân tích dữ liệu giữa CSV thô và định dạng Dashboard.
# -----------------------------------------------------------------------------

require_relative '../config'
require 'json'
require 'net/http'

module Analytics
  # Entry point chính
  def self.run(csv_map)
    puts "  [RUN] Analyzing CSV data results..."
    
    results = {
      age_distribution: process_age_dist(csv_map[:age_distribution]),
      top_diseases:     process_diseases(csv_map[:top_diseases]),
      billing_stats:    process_billing(csv_map[:billing_stats]),
      prediction:       fetch_ai_prediction # Thử nghiệm gọi Flask API
    }
    results
  end

  private

  def self.process_age_dist(path)
    # Hive output Format: group, count, percentage
    rows = CSV.read(path) rescue []
    {
      labels: rows.map { |r| r[0] },
      values: rows.map { |r| r[1].to_i },
      percentages: rows.map { |r| r[2].to_f }
    }
  end

  def self.process_diseases(path)
    # Hive output Format: name, count, avg_billing, percentage
    rows = CSV.read(path) rescue []
    {
      labels: rows.map { |r| r[0] },
      counts: rows.map { |r| r[1].to_i },
      avg_billing: rows.map { |r| r[2].to_f }
    }
  end

  def self.process_billing(path)
    # Hive output Format (via union): section, category, min, max, avg, median
    rows = CSV.read(path) rescue []
    # Chỉ lấy summary tổng quát
    first_row = rows.first || []
    {
      min: first_row[2].to_f,
      max: first_row[3].to_f,
      avg: first_row[4].to_f,
      median: first_row[5].to_f
    }
  end

  def self.fetch_ai_prediction
    # Đây là nơi gọi ML API Flask
    uri = URI("#{Config::FLASK_API_URL}/health")
    begin
        res = Net::HTTP.get_response(uri)
        JSON.parse(res.body)
    rescue
        { status: "unavailable", message: "Flask API offline" }
    end
  end
end
