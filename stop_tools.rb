def process_stop_file(filename,&block)
  File.open(filename) do |file|
    line_number = 0
    while line = file.gets
      line_number += 1

      splits = line.chomp.split("\t")
      raise "parsing error at line: #{line_number}" if splits.length != 4
      
      block.call(splits[0],splits[1].to_f,splits[2].to_f,splits[3].to_i)
    end
  end
end

def collect_lat_lng_min_max(min_max_hash,lat,lng)
  min_max_hash[:min_lat] = lat if min_max_hash[:min_lat].nil? or lat < min_max_hash[:min_lat]
  min_max_hash[:min_lng] = lng if min_max_hash[:min_lng].nil? or lng < min_max_hash[:min_lng]
  min_max_hash[:max_lat] = lat if min_max_hash[:max_lat].nil? or lat > min_max_hash[:max_lat]
  min_max_hash[:max_lng] = lng if min_max_hash[:max_lng].nil? or lng > min_max_hash[:max_lng]
  min_max_hash
end

def process_locations(locations,min_max_hash,grid_size,&block)
  if locations.any?
    lat_rng = min_max_hash[:max_lat] - (min_lat = min_max_hash[:min_lat])
    lng_rng = min_max_hash[:max_lng] - (min_lng = min_max_hash[:min_lng])
  end

  locations.each do |pair|
    lat_index = begin lat_rng == 0.0 ? 0 : [((pair.first - min_lat) / lat_rng * grid_size).to_i,grid_size - 1].min; rescue; -1; end
    lng_index = begin lng_rng == 0.0 ? 0 : [((pair.last  - min_lng) / lng_rng * grid_size).to_i,grid_size - 1].min; rescue; -1; end

    block.call(lat_index,lng_index)
  end
end