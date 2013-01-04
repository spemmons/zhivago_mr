require 'time'

def extract_stops(filename)
  File.open(filename) do |file|
    line_number,state = 0,{}
    while line = file.gets
      line_number += 1
      
      splits = line.chomp.gsub(/\t/,',').split(',')
      raise "parsing error at line: #{line_number}" if splits.length != 7
      
      check_reading(state,
          imei:       splits[0],
          timestamp:   parse_normalized_time(splits[1]),
          latitude:   splits[2].to_f,
          longitude:  splits[3].to_f,
          ignition:   (splits[4] && splits[4].to_i == 1),
          #speed:      splits[5].to_i,
          #count:      splits[6].to_i,
      )
    end
    output_stop(state)
  end
end

def check_reading(state,reading)
  if state[:imei]
    state[:last_timestamp] = reading[:timestamp] unless imei_change = state[:imei] != reading[:imei]
    output_stop(state) if imei_change or reading[:ignition]
    check_reading(state,reading) if imei_change # recursion assumes stat is reset by output_stop
  elsif not reading[:ignition]
    state.merge!(reading)
  end
end

def output_stop(state)
  puts [state[:imei],state[:latitude],state[:longitude],(state[:last_timestamp] - state[:timestamp]).to_i / 60].join("\t") if state[:last_timestamp]
  state.clear
end

def parse_normalized_time(time_string)
  time_value = Time.parse(time_string)
  Time.utc(time_value.year,time_value.month,time_value.day,time_value.hour,time_value.min)
end

#puts $*.inspect
extract_stops($*[0])