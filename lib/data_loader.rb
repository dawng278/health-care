# -----------------------------------------------------------------------------
# Script: data_loader.rb
# Mô tả: Đọc file CSV, cast kiểu dữ liệu và tải output từ HDFS.
# -----------------------------------------------------------------------------

require 'csv'
require 'date'
require_relative 'hadoop_connector'

module DataLoader
  # Đọc CSV local và chuyển đổi kiểu dữ liệu
  def self.load_csv(local_path)
    unless File.exist?(local_path)
      puts "  [WARNING] File không tồn tại: #{local_path}"
      return []
    end

    data = []
    CSV.foreach(local_path, headers: true, header_converters: :symbol) do |row|
      processed_row = row.to_h
      
      # Cast kiểu dữ liệu
      processed_row[:age] = processed_row[:age].to_i if processed_row[:age]
      processed_row[:room_number] = processed_row[:room_number].to_i if processed_row[:room_number]
      processed_row[:billing_amount] = processed_row[:billing_amount].to_f if processed_row[:billing_amount]
      
      # Xử lý Date và Length of Stay
      if processed_row[:date_of_admission] && processed_row[:discharge_date]
        begin
          admission = Date.parse(processed_row[:date_of_admission])
          discharge = Date.parse(processed_row[:discharge_date])
          processed_row[:date_of_admission] = admission
          processed_row[:discharge_date] = discharge
          processed_row[:length_of_stay] = (discharge - admission).to_i
        rescue
          processed_row[:length_of_stay] = 0
        end
      end

      data << processed_row
    end
    data
  end

  # Tải toàn bộ kết quả phân tích từ HDFS về local
  def self.fetch_all_outputs
    outputs = {
      age_distribution: File.join(Config::LOCAL_OUTPUT_DIR, 'age_distribution.csv'),
      top_diseases:     File.join(Config::LOCAL_OUTPUT_DIR, 'top_diseases.csv'),
      billing_stats:    File.join(Config::LOCAL_OUTPUT_DIR, 'billing_stats.csv'),
      admission_stats:  File.join(Config::LOCAL_OUTPUT_DIR, 'admission_stats.csv')
    }

    begin
      HadoopConnector.download("#{Config::HDFS_OUTPUT_DIR}/age_distribution", outputs[:age_distribution])
      HadoopConnector.download("#{Config::HDFS_OUTPUT_DIR}/top_diseases",     outputs[:top_diseases])
      HadoopConnector.download("#{Config::HDFS_OUTPUT_DIR}/billing_stats",    outputs[:billing_stats])
      HadoopConnector.download("#{Config::HDFS_OUTPUT_DIR}/admission_stats",  outputs[:admission_stats])
    rescue => e
      puts "  [ERROR] Lỗi khi tải dữ liệu từ HDFS: #{e.message}"
    end

    outputs
  end
end
