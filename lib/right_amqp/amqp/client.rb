require 'right_support'
require File.expand_path('../frame', __FILE__)

module AMQP
  class Error < StandardError; end

  module BasicClient
    def process_frame frame
      if mq = channels[frame.channel]
        mq.process_frame(frame)
        return
      end

      case frame
      when Frame::Method
        case method = frame.payload
        when Protocol::Connection::Start
          send Protocol::Connection::StartOk.new({:platform => 'Ruby/EventMachine',
                                                  :product => 'AMQP',
                                                  :information => 'http://github.com/tmm1/amqp',
                                                  :version => VERSION},
                                                 'AMQPLAIN',
                                                 {:LOGIN => @settings[:user],
                                                  :PASSWORD => @settings[:pass]},
                                                 'en_US')

        when Protocol::Connection::Tune
          send Protocol::Connection::TuneOk.new(:channel_max => 0,
                                                :frame_max => 131072,
                                                :heartbeat => @settings[:heartbeat] || 0)

          send Protocol::Connection::Open.new(:virtual_host => @settings[:vhost],
                                              :capabilities => '',
                                              :insist => @settings[:insist])

        when Protocol::Connection::OpenOk
          succeed(self)

        when Protocol::Connection::Close
          # raise Error, "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]}"
          STDERR.puts "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]}"

        when Protocol::Connection::CloseOk
          @on_disconnect.call if @on_disconnect
        end

      when Frame::Heartbeat
        @last_server_heartbeat = Time.now

      end

      # Make callback now that handshake with the broker has completed
      # The 'connected' status callback happens before the handshake is done and if it results in
      # a lot of activity it might prevent EM from being able to call the code handling the
      # incoming handshake packet in a timely fashion causing the broker to close the connection
      @connection_status.call(:ready) if @connection_status && frame.payload.is_a?(AMQP::Protocol::Connection::Start)
    end
  end

  def self.client
    @client ||= BasicClient
  end

  def self.client= mod
    mod.__send__ :include, AMQP
    @client = mod
  end

  module Client
    include EM::Deferrable
    include RightSupport::Log::Mixin

    def initialize opts = {}
      @settings = opts
      extend AMQP.client

      @on_disconnect ||= proc{ @connection_status.call(:failed) if @connection_status }

      timeout @settings[:timeout] if @settings[:timeout]
      errback{ @on_disconnect.call } unless @reconnecting

      @connected = false
    end

    def connection_completed
      start_tls if @settings[:ssl]
      log 'connected'
      # @on_disconnect = proc{ raise Error, 'Disconnected from server' }
      unless @closing
        @on_disconnect = method(:disconnected)
        @reconnecting = false
      end

      @connected = true
      @connection_status.call(:connected) if @connection_status

      @buf = Buffer.new
      send_data HEADER
      send_data [1, 1, VERSION_MAJOR, VERSION_MINOR].pack('C4')

      if heartbeat = @settings[:heartbeat]
        init_heartbeat if (@settings[:heartbeat] = heartbeat.to_i) > 0
      end
    end

    def init_heartbeat
      @last_server_heartbeat = Time.now

      @timer.cancel if @timer
      @timer = EM::PeriodicTimer.new(@settings[:heartbeat]) do
        if connected?
          if @last_server_heartbeat < (Time.now - (@settings[:heartbeat] * 2))
            log "Reconnecting due to missing server heartbeats"
            logger.warn("Reconnecting to broker #{@settings[:identity]} due to missing server heartbeats")
            reconnect(true)
          else
            @last_server_heartbeat = Time.now
            send AMQP::Frame::Heartbeat.new, :channel => 0
          end
        end
      end
    end

    def connected?
      @connected
    end

    def unbind
      log 'disconnected'
      @connected = false
      EM.next_tick{ @on_disconnect.call }
    end

    def add_channel mq
      (@_channel_mutex ||= Mutex.new).synchronize do
        channels[ key = (channels.keys.max || 0) + 1 ] = mq
        key
      end
    end

    def channels
      @channels ||= {}
    end

    # Catch exceptions that would otherwise cause EM to stop or be in a bad
    # state if a top level EM error handler was setup. Instead close the connection and leave EM
    # alone.
    # Don't log an error if the environment variable IGNORE_AMQP_FAILURES is set (used in the
    # enroll script)
    def receive_data data
      begin
        # log 'receive_data', data
        @buf << data

        while frame = Frame.parse(@buf)
          log 'receive', frame
          process_frame frame
        end
      rescue Exception => e
        logger.exception("Exception caught while processing AMQP frame, closing connection", e, :trace) unless ENV['IGNORE_AMQP_FAILURES']
        close_connection
      end
    end

    def process_frame frame
      # this is a stub meant to be
      # replaced by the module passed into initialize
    end

    def send data, opts = {}
      channel = opts[:channel] ||= 0
      data = data.to_frame(channel) unless data.is_a? Frame
      data.channel = channel

      log 'send', data
      send_data data.to_s
    end

    #:stopdoc:
    # def send_data data
    #   log 'send_data', data
    #   super
    # end
    #:startdoc:

    def close &on_disconnect
      if on_disconnect
        @closing = true
        @on_disconnect = proc{
          on_disconnect.call
          @closing = false
        }
      end

      callback{ |c|
        if c.channels.any?
          c.channels.each do |ch, mq|
            mq.close
          end
        else
          send Protocol::Connection::Close.new(:reply_code => 200,
                                               :reply_text => 'Goodbye',
                                               :class_id => 0,
                                               :method_id => 0)
        end
      }
    end

    def reconnect force = false
      if @reconnecting and not force
        # Wait after first reconnect attempt and in between each subsequent attempt
        EM.add_timer(@settings[:reconnect_interval] || 5) { reconnect(true) }
        return
      end

      unless @reconnecting
        @deferred_status = nil
        initialize(@settings)

        mqs = @channels
        @channels = {}
        mqs.each{ |_,mq| mq.reset } if mqs

        @reconnecting = true

        again = @settings[:reconnect_delay]
        again = again.call if again.is_a?(Proc)
        if again.is_a?(Numeric)
          # Wait before making initial reconnect attempt
          EM.add_timer(again) { reconnect(true) }
          return
        elsif ![nil, true].include?(again)
          raise ::AMQP::Error, "Could not interpret :reconnect_delay => #{again.inspect}; expected nil, true, or Numeric"
        end
      end

      logger.warn("Attempting to reconnect to #{@settings[:identity]}")
      log 'reconnecting'
      EM.reconnect(@settings[:host], @settings[:port], self)
    end

    def self.connect opts = {}
      opts = AMQP.settings.merge(opts)
      EM.connect opts[:host], opts[:port], self, opts
    end

    def connection_status &blk
      @connection_status = blk
    end

    private

    def disconnected
      @connection_status.call(:disconnected) if @connection_status
      reconnect
    end

    def log *args
      return unless @settings[:logging] or AMQP.logging
      require 'pp'
      pp args
      puts
    end
  end
end
