require_relative '../config'

module Analytics
  # Entry point duy nhất — nhận Array of Hash, trả về Hash chuẩn
  def self.run(raw_data)
    {
      age_distribution: age_distribution(raw_data[:age_data]),
      top_diseases:     top_diseases(raw_data[:disease_data]),
      billing_summary:  billing_stats(raw_data[:billing_data])
    }
  end

  private

  def self.age_distribution(age_data)
    # TODO (Member 3): Implement
    # Input: [{ age_group:, count:, percentage: }, ...]
    # Output: { labels: [], values: [], percentages: [] }
    raise NotImplementedError, 'Member 3: implement age_distribution'
  end

  def self.top_diseases(disease_data, n = Config::TOP_N_DISEASES)
    # TODO (Member 3): Lấy top N bệnh, sắp xếp theo count giảm dần
    raise NotImplementedError, 'Member 3: implement top_diseases'
  end

  def self.billing_stats(billing_data)
    # TODO (Member 3): Tính min/max/avg/median
    raise NotImplementedError, 'Member 3: implement billing_stats'
  end
end
