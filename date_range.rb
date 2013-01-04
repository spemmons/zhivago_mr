#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module DateRange

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      imei,date,remainder = args.first.split(',')
      yield [imei,date]
    end

  end

  require 'wukong/streamer/set_reducer'
  class Reducer < Wukong::Streamer::SetReducer

    def accumulate(*args)
      imei,date,remainder = *args
      values << date
    end

    def finalize
      value_array = Array(values)
      yield [key,value_array.length,value_array.min,value_array.max]
    end

  end
end

Wukong.run(DateRange::Mapper,DateRange::Reducer)