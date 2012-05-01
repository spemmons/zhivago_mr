#!/usr/bin/env ruby
require 'rubygems'
require 'wukong/script'

module DailyLocations

  class Mapper < Wukong::Streamer::LineStreamer

    def initialize
      @parser = ZhivagoParser.new # keep in UTC instead of PDT to approximate daily cycle
    end

    def process(line)
      @parser.each_type(:reading,line) do
        latitude,longitude = @parser[:latitude].to_f,@parser[:longitude].to_f
        return if latitude == 0.0 and longitude == 0.0
        return unless @parser[:ignition] == '0' and (device = @parser.device_list[@parser[:device_index]]) and (imei = device[:imei])

        day = @parser[:created_at].strftime('%Y-%m-%d')
        time = @parser[:created_at].strftime('%H:%M:%S')
        yield ["#{imei},#{day}",format('%s@%0.3f,%0.3f',time,latitude,longitude)]
      end
    end

  end

  class Reducer < Wukong::Streamer::ListReducer

    def accumulate(*record)
      values << record.last # should just grab the time@location value
    end

    def finalize
      return if values.empty?

      imei,day = key.split(',')
      path = values.sort.collect{|value| time,location = value.split('@'); location}
      yield [imei,'h',path.first] if path.first == path.last

      last_location = path.first
      path[1..-1].each do |next_location|
        next if next_location == last_location

        yield [imei,'e',last_location,next_location]
        last_location = next_location
      end
    end

  end

  # TODO figure out how to keep the following in a separate file -- in the meantime, carefully propogate changes

  class ZhivagoParser

    require 'csv'

    HEADERS = ['v=0.1','a|g:name','d:name:imei:account_id:gateway_id','e:name:gateway_id','r:device_id:event_id:latitude:longitude:ignition:speed:created_at']
    DATE_TOO_OLD = Time.mktime(2000,1,1)
    DATE_TOO_NEW = Time.now

    attr_reader :timezone,:logger,:line_number,:account_list,:gateway_list,:event_list,:device_list,:type

    def initialize(timezone = 'UTC',logger = Logger.new(STDERR))
      @timezone,@logger,@line_number,@account_list,@gateway_list,@event_list,@device_list = timezone,logger,0,[nil],[nil],[nil],[nil]
    end

    def [](key)
      @attributes[key]
    end

    def each_type(type,line,&block)
      each(line){ block.call(type,@attributes) if @type == type}
    end

    def each(line,&block)
      @line_number += 1
      CSV.parse(line) do |tokens|
        case tokens[0]
          when 'a'
            note_typed_list(:account,:name => tokens[1],&block)
          when 'e'
            note_typed_list(:event,:name => tokens[1],:gateway_index => tokens[2].to_i,&block)
          when 'g'
            note_typed_list(:gateway,:name => tokens[1],&block)
          when 'd'
            note_typed_list(:device,:name => tokens[1],:imei => tokens[2],:account_index => tokens[3].to_i,:gateway_index => tokens[4].to_i,&block)
          when 'r'
            if tokens[7].blank? or (created_at = Time.parse("#{tokens[7]} #{@timezone}").utc) < DATE_TOO_OLD or created_at > DATE_TOO_NEW
              log "invalid date: #{tokens[7]}"
            else
              set_state_and_call_block(:reading,:device_index => tokens[1].to_i,:event_index => tokens[2].to_i,:latitude => tokens[3],:longitude => tokens[4],:ignition => tokens[5],:speed => tokens[6],:created_at => created_at,&block)
            end
          when 'f'
            #log "final reading ID:#{tokens[1]}"
          else
            if @line_number != 1
              log "unexpected entry: #{tokens.inspect}"
            elsif tokens != HEADERS
              raise 'headers do not match'
            end
        end
      end
    rescue
      log "unexpected error: #{$!}"
    end

    def note_typed_list(type,attributes,&block)
      eval("#{type}_list") << attributes
      set_state_and_call_block(type,attributes,&block)
    end

    def set_state_and_call_block(type,attributes,&block)
      @type,@attributes = type,attributes
      block.call(type,attributes)
    end

    def log(output)
      logger.info "#{@line_number} - #{output}"
    end

  end

end

Wukong.run(DailyLocations::Mapper,DailyLocations::Reducer)