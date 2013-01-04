#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module HourCounts

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      imei,date,remainder = args[0].split(',')
      lat,lng,ign,spd,remainder = args[1].split(',')

      time = Time.parse(date)
      time_string = Time.gm(time.year,time.month,time.day,time.hour).strftime('%Y-%m-%d %H:%M')

      yield ign.to_i == 1 ? [time_string,1,0] : [time_string,0,1]
    end

  end


  class Reducer < Wukong::Streamer::AccumulatingReducer

    def start!(*args)
      @moving_count,@stopped_count = 0,0
    end

    def accumulate(*args)
      key,moving,stopped,remainder = *args
      @moving_count += moving.to_i
      @stopped_count += stopped.to_i
    end

    def finalize
      minutes = Time.parse(key)
      yield [key,minutes.to_i / 60 / 60,@moving_count,@stopped_count]
    end

  end

end

Wukong.run(HourCounts::Mapper,HourCounts::Reducer)