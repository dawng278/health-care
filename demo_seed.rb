# -----------------------------------------------------------------------------
# Script: demo_seed.rb
# Mô tả: Mock dữ liệu phân tích để chạy Demo Dashboard mà không cần Hadoop thật.
# -----------------------------------------------------------------------------

require_relative 'config'
require_relative 'lib/visualizer'

puts "🎨 [DEMO] Khởi tạo dữ liệu minh họa cho Dashboard..."

# Mock kết quả phân tích giống cấu trúc Member 3/4 quy định
demo_results = {
  age_distribution: {
    labels: ['0-18', '19-35', '36-50', '51-65', '65+'],
    values: [850, 2450, 4100, 3200, 1400],
    percentages: [7.1, 20.4, 34.2, 26.7, 11.6]
  },
  top_diseases: {
    labels: ['Arthritis', 'Diabetes', 'Hypertension', 'Obesity', 'Cancer', 'Asthma'],
    counts: [9308, 9304, 9245, 9231, 9227, 9185],
    avg_billing: [25497.33, 25638.41, 25497.1, 25805.97, 25161.79, 25635.25]
  },
  billing_stats: {
    total_patients: 55500,
    min: 1000.0,
    max: 49999.0,
    avg: 25550.0,
    median: 25700.0
  },
  admission_stats: {
    labels: ['Emergency', 'Elective', 'Urgent'],
    counts: [18600, 18450, 18450],
    percentages: [33.5, 33.2, 33.3]
  },
  prediction: { status: "Demo Mode", message: "Connect to Flask API for real AI results." }
}

# Sinh file dashboard.html
Visualizer.render(demo_results)

puts "\n🚀 [DEMO SUCCESS] Dashboard đã được tạo tại output/charts/dashboard.html"
puts "Bạn có thể mở file này trực tiếp hoặc qua server."
