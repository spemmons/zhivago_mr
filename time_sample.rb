require 'time'

def build_sampled_readings(filename)
  File.open(filename) do |file|
    line_number,last_reading = 0,{}
    while line = file.gets
      line_number += 1
      
      splits = line.chomp.gsub(/\t/,',').split(',')
      raise "parsing error at line: #{line_number}" if splits.length != 7
      
      reading = {
        imei:       splits[0],
        timestep:   parse_normalized_time(splits[1]),
        latitude:   splits[2].to_f,
        longitude:  splits[3].to_f,
        ignition:   (splits[4] && splits[4].to_i == 1),
        speed:      splits[5].to_i,
        count:      splits[6].to_i,
      }

      output_reading_span(last_reading,reading) if reading[:imei] == last_reading[:imei] and reading[:timestep] != last_reading[:timestep]

      last_reading = reading
    end

    output_reading(last_reading) if last_reading[:imei]
  end
end

def output_reading_span(first_reading,last_reading,time_delta = 60)
  output_reading(first_reading.merge(actual: 1))
  total_steps = (last_reading[:timestep] - first_reading[:timestep]) / time_delta
#puts "#{first_reading[:timestep]} - #{last_reading[:timestep]} - #{total_steps}"
  (1..total_steps - 1).each do |step|
    if first_reading[:speed] == 0
      output_reading(first_reading.merge(timestep: first_reading[:timestep] + time_delta * step))
    elsif last_reading[:speed] == 0
      output_reading(last_reading.merge(timestep: first_reading[:timestep] + time_delta * step))
    else
      output_reading(
          imei:       first_reading[:imei],
          timestep:   first_reading[:timestep] + time_delta * step,
          latitude:   compute_stepped_value(first_reading[:latitude],last_reading[:latitude],step,total_steps),
          longitude:  compute_stepped_value(first_reading[:longitude],last_reading[:longitude],step,total_steps),
          ignition:   true,
          speed:      compute_stepped_value(first_reading[:speed],last_reading[:speed],step,total_steps),
      )
    end
  end
end

def compute_stepped_value(first,last,step,total_steps)
  (first * (total_steps - step) + last * step) / total_steps
end

def output_reading(reading)
  puts [
      #reading[:imei],
      #reading[:timestep].strftime("%Y/%m/%d %H:%M:%S"),
      #format('%0.3f',reading[:latitude]),
      #format('%0.3f',reading[:longitude]),
      reading[:ignition] || (reading[:ignition].nil? && reading[:speed].to_i > 0) ? '1' : '0',
      reading[:speed].to_i,
      reading[:actual] || 0,
  ].join("\t")
end

def parse_normalized_time(time_string)
  time_value = Time.parse(time_string)
  Time.utc(time_value.year,time_value.month,time_value.day,time_value.hour,time_value.min)
end

#puts $*.inspect
build_sampled_readings($*[0])
