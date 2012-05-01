#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module DeviceCount

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      imei,type,first_location,second_location,remainder = *args
      yield [imei,1]
    end
  end

  class Reducer < Wukong::Streamer::CountingReducer
  end

end

# Execute the script
Wukong.run(DeviceCount::Mapper,DeviceCount::Reducer)