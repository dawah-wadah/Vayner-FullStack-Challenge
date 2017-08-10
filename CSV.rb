require 'csv'
require 'benchmark'
require 'json'
require 'byebug'

source1 = CSV.read('source1.csv', headers: true, header_converters: :symbol)
source2 = CSV.read('source2.csv', headers: true, header_converters: :symbol)

def print_time_spent
  time = Benchmark.realtime do
    yield
  end

  puts "Time taken to compute: #{time.round(6)} secs"
end

csv_hash = {}
source_hash = {}

print_time_spent do
  source1.each do |row|
    csv_hash.fetch(row[:campaign_id].to_sym) { |k| csv_hash[k] = {} }
            .fetch(:impressions) { |k| csv_hash[row[:campaign_id].to_sym][k] = 0 }
    csv_hash[row[:campaign_id].to_sym][:impressions] += row[:impressions].to_i

    row_hash = {}
    row_hash[:campaign_id] = row[:campaign_id].to_sym
    attributes = %i[state hair demographic]
    aud = row[:audience].split('_')
    aud2 = {}
    0.upto(2) do |i|
      aud2[attributes[i]] = aud[i]
    end
    row_hash[:audience] = aud2
    csv_hash[row[:campaign_id].to_sym].merge!(row_hash)
    csv_hash[row[:campaign_id].to_sym][:dates] = {}
  end
  source2.each do |row|
    entry = csv_hash[row[:campaign_id].to_sym][:dates]
    entry.fetch(row[:date]) { |k| entry[k] = {} }
         .fetch(row[:ad_type]) { |k| entry[row[:date]][k] = { spend: 0 } }
    entry[row[:date]][row[:ad_type]][:spend] += row[:spend].to_i
    csv_hash[row[:campaign_id].to_sym].fetch(:total_spend) { |k| csv_hash[row[:campaign_id].to_sym][k] = 0 }
    csv_hash[row[:campaign_id].to_sym][:total_spend] += row[:spend].to_i
    actions = {}
    JSON.parse(row[:actions]).each do |el|
      arr = el.flatten
      reporter = arr[0].to_sym
      type = arr[3].to_sym
      value = arr[1].to_i

      actions.fetch(reporter) { |k| actions[k] = {} }
             .fetch(type) { |k| actions[reporter][k] = {} }
             .fetch(:offset) { |k| actions[reporter][type][k] = 0 }
      actions.fetch(reporter) { |k| actions[k] = {} }
             .fetch(type) { |k| actions[reporter][k] = {} }
             .fetch(:value) { |k| actions[reporter][type][k] = 0 }
      if actions[reporter][type][:value] > 0
        actions[reporter][type][:offset] += 1
      end

      if type == :views
        csv_hash[row[:campaign_id].to_sym].fetch(:views) { |k| csv_hash[row[:campaign_id].to_sym][k] = 0 }
        csv_hash[row[:campaign_id].to_sym][:views] += value
      end
      actions[reporter][type][:value] += value

      source_hash.fetch(reporter) { |source| source_hash[source] = {} }
                 .fetch(type) { |type| source_hash[reporter][type] = 0 }
      source_hash[reporter][type] += value

      action_entry = entry[row[:date]][row[:ad_type]]
      action_entry.fetch(:actions) { |k| action_entry[k] = {} }
                  .fetch(reporter) { |k| action_entry[:actions][k] = {} }
      action_entry[:actions][reporter].merge!(actions[reporter])
    end
  end

  # testing
  # puts csv_hash['d55a01d7-cc8b-4a31-b573-b80660efbbea'.to_sym][:total_spend]
  # puts csv_hash['9b66b4a3-e20a-4a48-949c-93252a42b88d'.to_sym]
  # puts csv_hash['058dc542-baec-4ddb-be47-2e31ff08a942'.to_sym]

  spend = 0
  clicks = 0
  print_time_spent do
    csv_hash.select { |_k, row| row[:audience][:hair] == 'purple' }
            .each do |_el, entry|
              entry[:dates].each do |_k, value|
                value.values.each do |type|
                  spend += type[:spend]
                end
              end
            end
    puts "Total Spent on Purple Haired People : $#{spend}"
  end

  campaigns = 0
  print_time_spent do
    campaigns = csv_hash.select { |_k, v| v[:dates].length > 4 }.length
    puts "There were #{campaigns} campaigns that operated on more than 4 days"
  end
  print_time_spent do
    csv_hash.each do |_k, v|
      v[:dates].each do |_key, ad_type|
        ad_type.each do |_key, value|
          if value[:actions][:H] && value[:actions][:H].key?(:clicks)
            clicks += 1
            clicks += value[:actions][:H][:clicks][:offset]
          end
        end
      end
    end
    puts "Source 'H' has submitted #{clicks} reports on clicks"
  end

  #
  junk_reporters = []
  print_time_spent do
    source_hash.keys.each do |reporter|
      junk_reporters << reporter if source_hash[reporter.to_sym][:junk] > source_hash[reporter.to_sym][:noise]
    end
    puts "The following sources reported more junk than noise #{junk_reporters.join(', ')}"
  end

  cost = 0
  views = 0

  print_time_spent do
    csv_hash.each do |_k, camp_entry|
      camp_entry[:dates].each do |_key, date|
        next unless date['video']
        date['video'][:actions].each do |_k, value|
          if value[:views]
            views += value[:views][:value]
            cost += date['video'][:spend]
          end
        end
      end
    end
    puts "The cost per view is $#{(views.to_f / cost.to_f).round(2)}"
  end
  # puts "cost is #{cost}"
  # puts "View count is #{views}"

  b_conversions = 0
  print_time_spent do
    newyork_campaigns = []
    csv_hash.select { |_k, v| v[:audience][:state] == 'NY' }
            .each do |k, v|
      newyork_campaigns << k
      v[:dates].each do |_key, ad_type|
        ad_type.each do |_key, dates|
          dates[:actions].select { |reporter, report| reporter == :B && report[:conversions] }
                         .each { |_e, convo| b_conversions += convo[:conversions][:value] }
        end
      end
    end
    puts "Source B reported on #{b_conversions} conversions in NY"
  end

  print_time_spent do
    best_state = nil
    best_hair = nil
    best_CPM = nil
    csv_hash.each do |_k, entry|
      next if entry[:total_spend].to_i == 0 # a few campaigns had no spending, so i decided not include them, because the win by default
      next unless best_CPM.nil? || (entry[:total_spend].to_f / entry[:impressions].to_f * 1000) < best_CPM
      best_state = entry[:audience][:state]
      best_hair = entry[:audience][:hair]
      best_CPM = entry[:total_spend].to_f / entry[:impressions].to_f * 1000
    end

    puts "The best CPM combination comes from #{best_state} with the hair color of #{best_hair} with the lowest CPM of $#{best_CPM.round(4)}"
  end
end
