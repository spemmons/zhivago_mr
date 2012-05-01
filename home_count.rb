#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module HomeLocations

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      imei,type,first_location,second_location,remainder = *args
      yield [first_location,1] if type == 'h'
    end
  end

  class Reducer < Wukong::Streamer::CountingReducer
  end

end

# Execute the script
Wukong.run(HomeLocations::Mapper,HomeLocations::Reducer)