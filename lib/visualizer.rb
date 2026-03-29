# -----------------------------------------------------------------------------
# Script: visualizer.rb (Member 4 - Visualization Expert)
# -----------------------------------------------------------------------------

require 'erb'
require 'json'
require 'fileutils'
require_relative '../config'

module Visualizer
  # Entry point chính của visualization
  def self.render(results)
    puts "  [RENDER] Finalizing visualization artifacts..."

    # 1. Xuất file JSON dữ liệu thô phục vụ các nhu cầu khác
    export_json(results)

    # 2. Đọc file template HTML
    template_path = File.join(Config::PROJECT_ROOT, 'output', 'charts', 'dashboard_template.html')
    unless File.exist?(template_path)
      raise "Dashboard template not found at #{template_path}"
    end
    
    html_template = File.read(template_path)

    # 3. Sử dụng ERB để inject data vào JavaScript biến 'DATA'
    # 'results' sẽ có sẵn trong scope của ERB
    renderer = ERB.new(html_template)
    final_html = renderer.result(binding)

    # 4. Lưu ra file dashboard.html chính thức
    output_path = File.join(Config::CHART_OUTPUT_DIR, 'dashboard.html')
    FileUtils.mkdir_p(File.dirname(output_path))
    File.write(output_path, final_html)
    
    puts "  [OK] Dashboard fully generated: #{output_path}"
    output_path
  end

  # Xuất các file JSON riêng lẻ vào output/json/
  def self.export_json(data)
    FileUtils.mkdir_p(Config::JSON_OUTPUT_DIR)
    
    data.each do |key, value|
      path = File.join(Config::JSON_OUTPUT_DIR, "#{key}.json")
      File.write(path, JSON.pretty_generate(value))
      puts "    -> JSON Exported: #{File.basename(path)}"
    end
  end
end
