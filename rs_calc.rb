#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module RsCalc

  class Mapper < Wukong::Streamer::Base

    def process(*args)
      scale_index,time_index  = args[0].split(',')
      moving_count            = args[1]
      stopped_count           = args[2]
      sample_count            = args[3]
      yield ['%07d' % scale_index,moving_count,stopped_count,sample_count]
    end

  end

  class Reducer < Wukong::Streamer::AccumulatingReducer

    def start!(*args)
      @moving_counts,@stopped_counts,@total_counts = [],[],[]
    end

    def accumulate(*args)
      collect_counts(@moving_counts,moving_counts = args[1].to_i)
      collect_counts(@stopped_counts,stopped_counts = args[2].to_i)
      collect_counts(@total_counts,moving_counts + stopped_counts)
    end

    def finalize
      shift_count = [@total_counts.length / 20,10].min
      yield [key] + calc_rs(@moving_counts) + calc_rs(@stopped_counts) + calc_rs(@total_counts)
      #yield [key] + calc_shifts(@moving_counts,20,shift_count) + calc_shifts(@stopped_counts,20,shift_count) + calc_shifts(@total_counts,20,shift_count)
    end
    
    def collect_counts(counts,value)
      counts << value.to_f if value > 0
    end

    def calc_shifts(counts,range_size,shift_count)
      shift_count.times.collect{|multiplier| calc_rs(counts[multiplier*range_size,range_size]).last}
    end
    
    def calc_rs(counts)
      return [nil,nil,nil,nil,nil,nil] if counts.nil? or counts.length == 0

      divisor = counts.length
      total_value = 0
      squared_value = 0
      counts.each do |value|
        total_value += value
        squared_value += value**2
      end

      ave_value = total_value / divisor
      std_value = Math.sqrt(squared_value / divisor - ave_value**2)

      mean_adjusted_counts = counts.collect{|value| value - ave_value}

      prior_sum,cumulative_deviate_counts = 0,[]
      mean_adjusted_counts.each_with_index{|value,index| cumulative_deviate_counts[index] = (prior_sum += value)}

      cumulative_deviate_counts << 0
      min_value,max_value = cumulative_deviate_counts.min,cumulative_deviate_counts.max
      rs_value = std_value == 0.0 ? 0.0 : (max_value - min_value) / std_value

      [divisor,'%0.1f' % min_value,'%0.1f' % max_value,'%0.1f' % ave_value,'%0.1f' % std_value,'%0.1f' % rs_value]
    end

  end

end

Wukong.run(RsCalc::Mapper,RsCalc::Reducer)