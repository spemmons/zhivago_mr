load 'stop_tools.rb'

#              01234567890123456789012345
LAT_SYMBOLS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
LNG_SYMBOLS = 'abcdefghijklmnopqrstuvwxyz'

COMBINED_SYMBOLS = LAT_SYMBOLS + LNG_SYMBOLS + '0123456789'

def stop_strings(args)
  filename = args[0]

  min_duration = (args[1] && args[1].to_i) || 24*60

  grid_size = (args[2] && args[2].to_i) || 26
  raise "grid size of #{grid_size} invalid" unless grid_size.between?(4,LAT_SYMBOLS.length)

  data = {}
  process_stop_file(filename) do |imei,lat,lng,duration|
    results = data[imei] ||= {locations: []}
    collect_lat_lng_min_max(results,lat,lng)
    results[:locations] << [lat,lng] if duration >= min_duration
  end

  data.keys.sort.each do |imei|

    results = data[imei]

    lat_lng_symbols,buckets = [],{}
    last_lat_index,last_lng_index = nil,nil
    process_locations(results[:locations],results,grid_size) do |lat_index,lng_index|
      unless lat_index == last_lat_index and lng_index == last_lng_index #and false
        lat_symbol = (lat_index < 0 ? '#' : LAT_SYMBOLS[lat_index]) || '*'
        lng_symbol = (lng_index < 0 ? '%' : LNG_SYMBOLS[lng_index]) || '@'

        lat_lng_key = lat_symbol + lng_symbol

        #buckets[lat_lng_key] = true

        lat_lng_symbols << lat_lng_key

        last_lat_index,last_lng_index = lat_index,lng_index
      end
    end

    key_index = -1
    string = lat_lng_symbols.collect{|key| COMBINED_SYMBOLS[buckets[key] ||= (key_index += 1)] || '*'}
    #buckets.keys.sort.each_with_index{|key,index| buckets[key] = index}
    #string = lat_lng_symbols.collect{|key| COMBINED_SYMBOLS[buckets[key]] || '*'}
    #string = lat_lng_symbols

    #next unless string.length > 0 and string.length >= buckets.keys.length * 2

    print imei
    print "\t"
    print buckets.keys.length
    print "\t"
    print string.length
    print "\t"
    puts string.join
  end
end

#puts $*.inspect
stop_strings($*)