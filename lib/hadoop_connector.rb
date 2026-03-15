require_relative '../config'
require 'open3'
require 'fileutils'

module HadoopConnector
  # Upload file lên HDFS
  def self.upload(local_path, hdfs_path)
    cmd = "#{Config::HADOOP_BIN} fs -put -f #{local_path} #{hdfs_path}"
    execute!(cmd, "Uploading #{File.basename(local_path)} to HDFS")
  end

  # Chạy Hive query file (.hql)
  def self.run_hive(hql_file)
    cmd = "#{Config::HIVE_BIN} -f #{hql_file}"
    execute!(cmd, "Running Hive: #{File.basename(hql_file)}")
  end

  # Download output từ HDFS về local
  def self.download(hdfs_path, local_path)
    FileUtils.mkdir_p(File.dirname(local_path))
    cmd = "#{Config::HADOOP_BIN} fs -getmerge #{hdfs_path} #{local_path}"
    execute!(cmd, "Downloading #{hdfs_path}")
  end

  private

  def self.execute!(cmd, label)
    puts "[Hadoop] #{label}"
    stdout, stderr, status = Open3.capture3(cmd)
    unless status.success?
      raise "[ERROR] #{label} failed:\n#{stderr}"
    end
    puts "[OK] Done"
    stdout
  end
end
