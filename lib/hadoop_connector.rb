# -----------------------------------------------------------------------------
# Script: hadoop_connector.rb
# Mô tả: Lớp kết nối trung gian gọi các lệnh Hadoop/Hive qua Open3.
# -----------------------------------------------------------------------------

require 'open3'
require_relative '../config'

module HadoopConnector
  def self.upload(local_path, hdfs_path)
    execute!("#{Config::HADOOP_BIN} fs -put -f #{local_path} #{hdfs_path}", "Upload to HDFS")
  end

  def self.run_hive(hql_file)
    execute!("#{Config::HIVE_BIN} -f #{hql_file}", "Hive Query Execution: #{File.basename(hql_file)}")
  end

  def self.download(hdfs_path, local_target)
    # Dùng getmerge để gom các file part-* thành 1 file CSV duy nhất
    execute!("#{Config::HADOOP_BIN} fs -getmerge #{hdfs_path} #{local_target}", "Download from HDFS")
  end

  def self.hdfs_exists?(path)
    _, _, status = Open3.capture3("#{Config::HADOOP_BIN} fs -test -e #{path}")
    status.success?
  end

  def self.list_output(hdfs_dir)
    stdout, _, _ = execute!("#{Config::HADOOP_BIN} fs -ls #{hdfs_dir}", "List HDFS Directory")
    stdout
  end

  private

  def self.execute!(cmd, label)
    puts "  [EXEC] #{label}..."
    
    # Đảm bảo JAVA_HOME được truyền vào nếu có trong ENV
    env = {}
    env["JAVA_HOME"] = ENV["JAVA_HOME"] if ENV["JAVA_HOME"]

    stdout, stderr, status = Open3.capture3(env, cmd)

    if status.success?
      puts "  [OK] #{label} completed."
      return [stdout, status]
    else
      puts "  [ERROR] #{label} failed!"
      puts "  Details: #{stderr}"
      raise "Command failed: #{cmd}\nError: #{stderr}"
    end
  end
end
