#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module RsPrep

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      time_index    = args[0].to_i
      moving_count  = args[1]
      stopped_count = args[2]
      ((1..20).collect{|exp| 2**exp} + [60,60*60,60*60*24,60*60*24*7]).each do |scale_index|
        yield ["#{scale_index},#{time_index / scale_index}",moving_count,stopped_count]
      end
    end

  end

  class Reducer < Wukong::Streamer::AccumulatingReducer

    def start!(*args)
      @moving_count,@stopped_count,@sample_count = 0,0,0
    end

    def accumulate(*args)
      key,moving,stopped,remainder = *args
      @moving_count += moving.to_i
      @stopped_count += stopped.to_i
      @sample_count += 1
    end

    def finalize
      yield [key,@moving_count,@stopped_count,@sample_count]
    end

  end

end

Wukong.run(RsPrep::Mapper,RsPrep::Reducer)