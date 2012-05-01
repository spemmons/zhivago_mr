#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module AdjancencyList

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      imei,type,from_location,to_location,remainder = *args
      yield [from_location,to_location] if type == 'e' and from_location and to_location
    end

  end

  require 'wukong/streamer/set_reducer'
  class Reducer < Wukong::Streamer::SetReducer

    def accumulate(*args)
      from_location,to_location,remainder = *args
      values << to_location
    end

    def finalize
      value_array = Array(values)
      yield [key,value_array.length,*value_array]
    end

  end
end

Wukong.run(AdjancencyList::Mapper,AdjancencyList::Reducer)