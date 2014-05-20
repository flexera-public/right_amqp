require 'right_support'
require File.expand_path('../frame', __FILE__)

module AMQP
  class Error < StandardError; end

  module BasicClient
    def process_frame frame
      @last_data_received = Time.now
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
          logger.debug("[amqp] Received open completion from broker #{@settings[:identity]}")
          succeed(self)

        when Protocol::Connection::Close
          # raise Error, "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]}"
          STDERR.puts "#{method.reply_text} in #{Protocol.classes[method.class_id].methods[method.method_id]}"

        when Protocol::Connection::CloseOk
          logger.debug("[amqp] Received close completion from broker #{@settings[:identity]}")
          @on_disconnect.call if @on_disconnect
        end

      when Frame::Heartbeat
        logger.debug("[amqp] Received heartbeat from broker #{@settings[:identity]}")
        @last_heartbeat_received = @last_data_received
      end

      # Make callback now that handshake with the broker has completed
      # The 'connected' status callback happens before the handshake is done and if it results in
      # a lot of activity it might prevent EM from being able to call the code handling the
      # incoming handshake packet in a timely fashion causing the broker to close the connection
      @connection_status.call(:ready) if @connection_status && frame.payload.is_a?(AMQP::Protocol::Connection::OpenOk)
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

    def self.included(base)
      base.extend(RightSupport::Log::Mixin::ClassMethods)
    end

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
      logger.debug("[amqp] Connected to broker #{@settings[:identity]}")
      @connection_status.call(:connected) if @connection_status

      @buf = Buffer.new
      send_data HEADER
      send_data [1, 1, VERSION_MAJOR, VERSION_MINOR].pack('C4')

      if heartbeat = @settings[:heartbeat]
        init_heartbeat if (@settings[:heartbeat] = heartbeat.to_i) > 0
      end
    end

    def init_heartbeat
      # Randomly offset start of heartbeat timer to help separate heartbeat
      # activity when there are multiple broker connections active
      EM.add_timer(rand(@settings[:heartbeat])) do
        begin
          # While connected, a heartbeat or some other data is expected to be received from
          # the broker at least every 2 x :heartbeat seconds, otherwise the connection is
          # assumed to be broken and therefore is closed to cause an automatic reconnect.
          # While connected, this client will send a heartbeat every :heartbeat
          # seconds regardless of any other send activity. The RabbitMQ broker will behave
          # similarly and drop the connection if it does not receive a heartbeat or other
          # data in time.
          logger.info("[amqp] Initializing heartbeat for broker #{@settings[:identity]} to #{@settings[:heartbeat]} sec")

          timeout_factor = 2
          @heartbeat_timer.cancel if @heartbeat_timer
          @heartbeat_timer = EM::PeriodicTimer.new(@settings[:heartbeat]) do
            begin
              if connected?
                now = Time.now
                if @last_data_received && @last_data_received < (now - (@settings[:heartbeat] * timeout_factor))
                  data_received = (now - @last_data_received).to_i
                  heartbeat_received = (now - @last_heartbeat_received).to_i if @last_heartbeat_received
                  heartbeat_sent = (now - @last_heartbeat_sent).to_i if @last_heartbeat_sent
                  logger.info("[amqp] Reconnecting to broker #{@settings[:identity]} due to heartbeat timeout: " +
                              "last data received #{data_received.inspect} sec ago, " +
                              "last heartbeat received #{heartbeat_received.inspect} sec ago, " +
                              "last heartbeat sent #{heartbeat_sent.inspect} sec ago")
                  close_connection # which results in an unbind and an automatic reconnect
                else
                  logger.debug("[amqp] Sending heartbeat to broker #{@settings[:identity]}")
                  send AMQP::Frame::Heartbeat.new, :channel => 0
                  @last_heartbeat_sent = Time.now
                end
              else
                logger.debug("[amqp] Skipping heartbeat check for broker #{@settings[:identity]} because disconnected")
              end
            rescue Exception => e
              logger.error("[amqp] Failed heartbeat check (#{e})\n" + e.backtrace.join("\n"))
            end
          end
        rescue Exception => e
          logger.error("[amqp] Failed heartbeat initialization (#{e})\n" + e.backtrace.join("\n"))
        end
      end
    end

    def connected?
      @connected
    end

    def unbind
      log 'disconnected'
      @connected = false
      logger.debug("[amqp] Disconnected from broker #{@settings[:identity]}")
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
        logger.exception("[amqp] Failed processing frame, closing connection", e, :trace) unless ENV['IGNORE_AMQP_FAILURES']
        failed
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
      @heartbeat_timer.cancel if @heartbeat_timer
      @heartbeat_timer = nil
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

      log 'reconnecting'
      logger.info("[amqp] Attempting to reconnect to broker #{@settings[:identity]}")
      EM.reconnect(@settings[:host], @settings[:port], self)
    rescue Exception => e
      logger.exception("[amqp] Failed to reconnect", e, :trace)
      failed
    end

    def self.connect opts = {}
      opts = AMQP.settings.merge(opts)
      EM.connect opts[:host], opts[:port], self, opts
    end

    def connection_status &blk
      @connection_status = blk
    end

    def failed
      @connection_status.call(:failed) if @connection_status
      @has_failed = true
      close_connection
    end

    private

    def disconnected
      unless @has_failed
        @connection_status.call(:disconnected) if @connection_status
        reconnect
      end
    end

    def log *args
      return unless @settings[:logging] or AMQP.logging
      require 'pp'
      pp args
      puts
    end
  end
end
