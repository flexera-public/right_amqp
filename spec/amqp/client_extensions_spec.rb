#
# Copyright (c) 2009-2012 RightScale Inc
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec_helper'))
require File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'lib', 'right_amqp'))

describe AMQP::Client do

  include RightAMQP::SpecHelper
  include FlexMock::ArgumentTypes

  context 'reconnect' do

    class SUT
      include AMQP::Client

      attr_accessor :reconnecting, :settings, :channels
    end

    before(:each) do
      setup_logger
      @sut = flexmock(SUT.new)
      @sut.reconnecting = false
      @sut.settings = {:host => 'testhost', :port=>'12345'}
      @sut.channels = {}

      @sut.should_receive(:initialize)
    end

    context 'with no :reconnect_delay' do
      it 'should reconnect immediately' do
        flexmock(EM).should_receive(:reconnect).once
        flexmock(EM).should_receive(:add_timer).never

        @sut.reconnect()
      end
    end

    context 'with a :reconnect_delay of true' do
      it 'should reconnect immediately' do
        @sut.settings[:reconnect_delay] = true

        flexmock(EM).should_receive(:reconnect).once
        flexmock(EM).should_receive(:add_timer).never

        @sut.reconnect()
      end
    end

    context 'with a :reconnect_delay of 15 seconds' do
      it 'should schedule a reconnect attempt in 15s' do
        @sut.settings[:reconnect_delay] = 15

        flexmock(EM).should_receive(:reconnect).never
        flexmock(EM).should_receive(:add_timer).with(15, Proc).once

        @sut.reconnect()
      end
    end

    context 'with a :reconnect_delay containing a Proc that returns 30' do
      it 'should schedule a reconnect attempt in 30s' do
        @sut.settings[:reconnect_delay] = Proc.new {30}

        flexmock(EM).should_receive(:reconnect).never
        flexmock(EM).should_receive(:add_timer).with(30, Proc).once

        @sut.reconnect()
      end
    end

    context 'with a :reconnect_interval of 5 seconds'  do
      it 'should schedule reconnect attempts on a 5s interval' do
        @sut.reconnecting = true
        @sut.settings[:reconnect_delay] = 15
        @sut.settings[:reconnect_interval] = 5

        flexmock(EM).should_receive(:reconnect).never
        flexmock(EM).should_receive(:add_timer).with(5, Proc).once

        @sut.reconnect()
      end
    end

  end

  context "heartbeat" do

    class SUT
      include AMQP::Client

      attr_accessor :connected, :reconnecting, :settings, :channels, :heartbeat_timer
      attr_accessor :last_data_received, :last_heartbeat_received, :last_heartbeat_sent
    end

    before(:each) do
      setup_logger
      @sut = flexmock(SUT.new)
      @sut.reconnecting = false
      @sut.channels = {}
      @sut.should_receive(:initialize)
      @sut.should_receive(:send_data)

      @tune_payload = flexmock("tune payload", :class => AMQP::Protocol::Connection::Tune)
      @method_frame = flexmock("method frame", :class => AMQP::Frame::Method, :channel => 0, :payload => @tune_payload)
      @heartbeat_frame = flexmock("heartbeat frame", :class => AMQP::Frame::Heartbeat, :channel => 0)
    end

    context "with no :heartbeat setting" do
      it "should never initialize heartbeat" do
        @sut.should_receive(:init_heartbeat).never
        @sut.connection_completed
      end

      it "should send :heartbeat setting of 0 to broker" do
        flexmock(AMQP::Protocol::Connection::TuneOk).should_receive(:new).with(hsh(:heartbeat => 0))
        @sut.process_frame(@method_frame)
      end
    end

    context "with a :heartbeat setting of 0" do
      before(:each) do
        @sut.settings = {:heartbeat => 0}
      end

      it "should never initialize heartbeat" do
        @sut.should_receive(:init_heartbeat).never
        @sut.connection_completed
      end

      it "should send :heartbeat setting of 0 to broker" do
        flexmock(AMQP::Protocol::Connection::TuneOk).should_receive(:new).with(hsh(:heartbeat => 0))
        @sut.process_frame(@method_frame)
      end
    end

    context "with a :heartbeat setting > 0" do

      before(:each) do
        @now = Time.at(1000000)
        flexmock(Time).should_receive(:now).and_return(@now).by_default
        @periodic_timer = flexmock("periodic timer")
        flexmock(EM::PeriodicTimer).should_receive(:new).and_return(@periodic_timer).and_yield.by_default
        flexmock(EM::PeriodicTimer).should_receive(:cancel).by_default
        flexmock(EM).should_receive(:add_timer).and_yield.by_default

        @heartbeat = 30
        @sut.settings = {:heartbeat => @heartbeat, :identity => "test-heartbeat"}
        @sut.connected = true
        @sut.last_data_received = @now
        @sut.last_heartbeat_received = nil
        @sut.last_heartbeat_sent = nil
      end

      context "and opened connection is being tuned" do
        it "should send :heartbeat setting to broker" do
          flexmock(AMQP::Protocol::Connection::TuneOk).should_receive(:new).with(hsh(:heartbeat => @heartbeat))
          @sut.process_frame(@method_frame)
        end
      end

      context "and connection has completed" do
        it "should initialize heartbeat" do
          @sut.should_receive(:init_heartbeat).once
          @sut.connection_completed
        end

        it "should initialize timer to start at a random :heartbeat time" do
          flexmock(EM).should_receive(:add_timer).with(on { |arg| arg >= 0 && arg < @heartbeat }, Proc).once
          @sut.connection_completed
        end
      end

      context "and the heartbeat timer initialization fails" do
        it "should log exception" do
          @logger.should_receive(:info).with(/Initializing heartbeat for broker/).once
          @logger.should_receive(:error).with(/Failed heartbeat initialization/).once
          flexmock(EM::PeriodicTimer).should_receive(:new).and_raise(Exception)
          @sut.init_heartbeat
        end
      end

      context "and the timer fires when not connected" do
        it "should log a debug message but not perform check" do
          @logger.should_receive(:debug).with(/Skipping heartbeat check for broker/).once
          flexmock(EM::PeriodicTimer).should_receive(:new).and_yield
          @sut.connected = false
          @sut.init_heartbeat
        end
      end

      context "and the timer fires when connected" do

        context "and no data has been received in 2 x :heartbeat interval" do
          it "should close connection to force reconnect" do
            @sut.last_data_received = @now - (@heartbeat * 2) - 1
            @logger.should_receive(:info).with(/Initializing heartbeat for broker/).once
            @logger.should_receive(:info).with(/Reconnecting to broker/).once
            @sut.should_receive(:close_connection).once
            @sut.init_heartbeat
          end
        end

        context "and timeout check passes" do
          it "should send heartbeat" do
            @logger.should_receive(:debug).with(/Sending heartbeat to broker/).once
            @sut.should_receive(:send).with(AMQP::Frame::Heartbeat, :channel => 0).once
            @sut.init_heartbeat
          end
        end

        context "and the heartbeat check fails" do
          it "should log exception" do
            @logger.should_receive(:error).with(/Failed heartbeat check/).once
            @sut.should_receive(:send).and_raise(Exception)
            @sut.init_heartbeat
          end
        end

      end

      context "and connection is being closed" do
        it "should cancel heartbeat timer" do
          @periodic_timer.should_receive(:cancel).once
          @sut.connection_completed
          @sut.heartbeat_timer.should_not be_nil
          @sut.close
          @sut.heartbeat_timer.should be_nil
        end
      end

    end

  end

end
