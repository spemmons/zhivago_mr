#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module FrequencyCounter

  class Mapper < Wukong::Streamer::RecordStreamer

    def process(*args)
      location,off_count,stopped_count,moving_count,speed_min,speed_max,speed_ave,error_count,remainder = *args
      yield [off_count,'off'] if off_count.to_i > 0
      yield [stopped_count,'stopped'] if stopped_count.to_i > 0
      yield [moving_count,'moving',speed_ave ? speed_ave.to_i : speed_min] if moving_count.to_i > 0
      yield [error_count,'error'] if error_count.to_i > 0
    end

  end

  class Reducer < Wukong::Streamer::AccumulatingReducer

    def start!(*args)
      @off_count,@stopped_count,@moving_count,@error_count,@unknown_count,@speed_counts = 0,0,0,0,0,[0]
    end

    def accumulate(*args)
      key,type,speed,remainder = *args
      case type
        when 'off'
          @off_count += 1
        when 'stopped'
          @stopped_count += 1
        when 'moving'
          @moving_count += 1
          speed = speed.to_i
          @speed_counts[0] = speed if speed > @speed_counts[0]
          @speed_counts[speed] = (@speed_counts[speed] || 0) + 1
        when 'error'
          @error_count += 1
        else
          @unknown_count += 1
      end
    end

    def finalize
      yield [key,@off_count,@stopped_count,@moving_count,@error_count,@unknown_count,*@speed_counts]
    end

  end

end

Wukong.run(FrequencyCounter::Mapper,FrequencyCounter::Reducer)