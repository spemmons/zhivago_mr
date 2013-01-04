load 'stop_tools.rb'

def stop_stats(filename)
  stats = {}
  range = {}
  process_stop_file(filename) do |imei,lat,lng,duration|
    results = stats[imei] ||= {stops: 0,locations: {},durations: {}}
    results[:stops] += 1

    collect_lat_lng_min_max(range,lat,lng)
    collect_lat_lng_min_max(results,lat,lng)

    results[:locations][lat_lng = "#{lat},#{lng}"] ||= 0
    results[:locations][lat_lng] += 1

    results[:durations][duration] ||= 0
    results[:durations][duration] += 1
  end

  stats.keys.sort.each do |imei|
    outputs = [imei]

    results = stats[imei]
    outputs << results[:stops]

    locations = results[:locations].keys.collect{|lat_lng| lat_lng.split(',').collect(&:to_f)}
    outputs << locations.length

    #26.downto(4).each do |grid_size|
    [25,20,15,10,5].each do |grid_size|
      buckets = {}
      process_locations(locations,results,grid_size) do |lat_index,lng_index|
      #process_locations(locations,range,grid_size) do |lat_index,lng_index|
        buckets["#{lat_index},#{lng_index}"] = true
      end
      outputs << buckets.keys.length
    end

    #durations = results[:durations].keys.collect(&:to_i).sort
    #outputs << durations.first
    #outputs << durations.last

    puts outputs.join("\t")
  end
end

#puts $*.inspect
stop_stats($*[0])