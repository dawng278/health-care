require_relative '../config'
require 'json'
require 'fileutils'

module Visualizer
  # Entry point — nhận Hash từ Analytics.run()
  def self.render(analytics_result)
    FileUtils.mkdir_p(Config::JSON_OUTPUT_DIR)
    FileUtils.mkdir_p(Config::CHART_OUTPUT_DIR)

    export_json(analytics_result)
    generate_dashboard(analytics_result)
  end

  private

  def self.export_json(data)
    [:age_distribution, :top_diseases, :billing_summary].each do |key|
      path = File.join(Config::JSON_OUTPUT_DIR, "#{key}.json")
      File.write(path, JSON.pretty_generate(data[key]))
      puts "  [JSON] Exported: #{path}"
    end
  end

  def self.generate_dashboard(data)
    # TODO (Member 4): Tạo HTML embed Chart.js
    # Đọc JSON đã xuất và nhúng vào dashboard.html
    path = File.join(Config::CHART_OUTPUT_DIR, 'dashboard.html')
    File.write(path, build_html(data))
    puts "  [HTML] Dashboard: #{path}"
  end

  def self.build_html(data)
    # TODO (Member 4): Implement template HTML + Chart.js
    # Bạn có thể copy HTML từ template ở mục 7.2 vào đây
    raise NotImplementedError, 'Member 4: implement build_html'
  end
end
