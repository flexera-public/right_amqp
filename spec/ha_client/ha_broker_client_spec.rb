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

require 'json'

require File.expand_path(File.join(File.dirname(__FILE__), '..', 'spec_helper'))
require File.expand_path(File.join(File.dirname(__FILE__), '..', '..', 'lib', 'right_amqp'))

class PushMock; end
class RequestMock; end

describe RightAMQP::HABrokerClient do

  include FlexMock::ArgumentTypes
  include RightAMQP::SpecHelper

  before(:each) do
    setup_logger
    @exceptions = RightSupport::Stats::Exceptions
    @non_deliveries = RightSupport::Stats::Activity
    @message = "message"
    @packet = flexmock("packet", :class => RequestMock, :to_s => true, :version => [12, 12]).by_default
    @serializer = flexmock("serializer")
    @serializer.should_receive(:dump).and_return(@message).by_default
    @serializer.should_receive(:load).with(@message).and_return(@packet).by_default
  end

  describe "Context" do

    before(:each) do
      @packet1 = flexmock("packet1", :class => RequestMock, :name => "request", :type => "type1",
                          :from => "from1", :token => "token1", :one_way => false)
      @packet2 = flexmock("packet2", :class => FlexMock, :name => "flexmock")
      @brokers = ["broker"]
      @options = {:option => "option"}
    end

    it "should initialize context" do
      context = RightAMQP::HABrokerClient::Context.new(@packet1, @options, @brokers)
      context.name.should == "request"
      context.type.should == "type1"
      context.from.should == "from1"
      context.token.should == "token1"
      context.one_way.should be_false
      context.options.should == @options
      context.brokers.should == @brokers
      context.failed.should == []
    end

    it "should treat type, from, token, and one_way as optional members of packet but default one_way to true" do
      context = RightAMQP::HABrokerClient::Context.new(@packet2, @options, @brokers)
      context.name.should == "flexmock"
      context.type.should be_nil
      context.from.should be_nil
      context.token.should be_nil
      context.one_way.should be_true
      context.options.should == @options
      context.brokers.should == @brokers
      context.failed.should == []
    end

    it "should store identity of failed brokers" do
      context = RightAMQP::HABrokerClient::Context.new(@packet1, @options, @brokers)
      context.record_failure("rs-broker-1-1")
      context.record_failure("rs-broker-2-2")
      context.failed.should == ["rs-broker-1-1", "rs-broker-2-2"]
    end

  end

  describe "Caching" do

    require 'digest/md5'

    before(:each) do
      @now = Time.at(1000000)
      @max_age = RightAMQP::HABrokerClient::Published::MAX_AGE
      flexmock(Time).should_receive(:now).and_return(@now).by_default
      @published = RightAMQP::HABrokerClient::Published.new
      @message1 = JSON.dump({:data => "a message"})
      @key1 = Digest::MD5.hexdigest(@message1)
      @message2 = JSON.dump({:data => "another message"})
      @key2 = Digest::MD5.hexdigest(@message2)
      @message3 = JSON.dump({:data => "yet another message"})
      @key3 = Digest::MD5.hexdigest(@message3)
      @packet1 = flexmock("packet1", :class => RequestMock, :name => "request", :type => "type1",
                          :from => "from1", :token => "token1", :one_way => false)
      @packet2 = flexmock("packet2", :class => RequestMock, :name => "request", :type => "type2",
                          :from => "from2", :token => "token2", :one_way => false)
      @packet3 = flexmock("packet3", :class => PushMock, :name => "push", :type => "type3",
                          :from => "from3", :token => "token3", :one_way => true)
      @brokers = ["broker"]
      @options = {:option => "option"}
      @context1 = RightAMQP::HABrokerClient::Context.new(@packet1, @options, @brokers)
      @context2 = RightAMQP::HABrokerClient::Context.new(@packet2, @options, @brokers)
      @context3 = RightAMQP::HABrokerClient::Context.new(@packet3, @options, @brokers)
    end

    it "should use message signature as cache hash key if it has one" do
      @published.identify(@message1).should == @key1
      @published.identify(@message2).should == @key2
      @published.identify(@message3).should == @key3
    end

    it "should store message info" do
      @published.store(@message1, @context1)
      @published.instance_variable_get(:@cache)[@key1].should == [@now.to_i, @context1]
      @published.instance_variable_get(:@lru).should == [@key1]
    end

    it "should update timestamp and lru list when store to existing entry" do
      @published.store(@message1, @context1)
      @published.instance_variable_get(:@cache)[@key1].should == [@now.to_i, @context1]
      @published.instance_variable_get(:@lru).should == [@key1]
      @published.store(@message2, @context2)
      @published.instance_variable_get(:@lru).should == [@key1, @key2]
      flexmock(Time).should_receive(:now).and_return(Time.at(@now + @max_age - 1))
      @published.store(@message1, @context1)
      @published.instance_variable_get(:@cache)[@key1].should == [(@now + @max_age - 1).to_i, @context1]
      @published.instance_variable_get(:@lru).should == [@key2, @key1]
    end

    it "should remove old cache entries when store new one" do
      @published.store(@message1, @context1)
      @published.store(@message2, @context2)
      (@published.instance_variable_get(:@cache).keys - [@key1, @key2]).should == []
      @published.instance_variable_get(:@lru).should == [@key1, @key2]
      flexmock(Time).should_receive(:now).and_return(Time.at(@now + @max_age + 1))
      @published.store(@message3, @context3)
      @published.instance_variable_get(:@cache).keys.should == [@key3]
      @published.instance_variable_get(:@lru).should == [@key3]
    end

    it "should fetch message info and make it the most recently used" do
      @published.store(@message1, @context1)
      @published.store(@message2, @context2)
      @published.instance_variable_get(:@lru).should == [@key1, @key2]
      @published.fetch(@message1).should == @context1
      @published.instance_variable_get(:@lru).should == [@key2, @key1]
    end

    it "should fetch empty hash if entry not found" do
      @published.fetch(@message1).should be_nil
      @published.store(@message1, @context1)
      @published.fetch(@message1).should_not be_nil
      @published.fetch(@message2).should be_nil
    end

  end # Published

  context "when initializing" do

    before(:each) do
      @identity = "rs-broker-localhost-5672"
      @address = {:host => "localhost", :port => 5672, :index => 0}
      @broker = flexmock("broker_client", :identity => @identity, :usable? => true)
      @broker.should_receive(:update_status).by_default
      flexmock(RightAMQP::BrokerClient).should_receive(:new).and_return(@broker).by_default
    end

    it "should create a broker client for default host and port" do
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity, @address, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(@broker).once
      ha = RightAMQP::HABrokerClient.new(@serializer)
      ha.brokers.should == [@broker]
    end

    it "should create broker clients for specified hosts and ports and assign index in order of creation" do
      address1 = {:host => "first", :port => 5672, :index => 0}
      broker1 = flexmock("broker_client1", :identity => "rs-broker-first-5672", :usable? => true)
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with("rs-broker-first-5672", address1, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(broker1).once
      address2 = {:host => "second", :port => 5672, :index => 1}
      broker2 = flexmock("broker_client2", :identity => "rs-broker-second-5672", :usable? => true)
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with("rs-broker-second-5672", address2, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(broker2).once
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second", :port => 5672)
      ha.brokers.should == [broker1, broker2]
    end

    it "should setup to receive status updates from each broker client" do
      broker = flexmock("broker_client", :identity => "rs-broker-first-5672")
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with("rs-broker-first-5672", Hash, @serializer,
              @exceptions, @non_deliveries, on { |arg| arg[:update_status_callback].is_a?(Proc) }, nil).and_return(broker).once
      RightAMQP::HABrokerClient.new(@serializer, :host => "first", :port => 5672)
    end

    it "should setup to receive returned messages from each broker client" do
      broker = flexmock("broker_client", :identity => "rs-broker-first-5672")
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with("rs-broker-first-5672", Hash, @serializer,
              @exceptions, @non_deliveries, on { |arg| arg[:return_message_callback].is_a?(Proc) }, nil).and_return(broker).once
      RightAMQP::HABrokerClient.new(@serializer, :host => "first", :port => 5672)
    end

  end # when initializing

  context "when parsing user_data" do

    it "should extra host list from RS_rn_url and RS_rn_host" do
      RightAMQP::HABrokerClient.parse_user_data("RS_rn_url=rs@first/right_net&RS_rn_host=:0,second:1").should ==
        ["first:0,second:1", nil]
    end

    it "should extra port list from RS_rn_port" do
      RightAMQP::HABrokerClient.parse_user_data("RS_rn_url=rs@host/right_net&RS_rn_host=:1,host:0&RS_rn_port=5673:1,5672:0").should ==
        ["host:1,host:0", "5673:1,5672:0"]
    end

    it "should raise an exception if there is no user data" do
      lambda { RightAMQP::HABrokerClient.parse_user_data(nil) }.should raise_error(RightAMQP::HABrokerClient::NoUserData)
      lambda { RightAMQP::HABrokerClient.parse_user_data("") }.should raise_error(RightAMQP::HABrokerClient::NoUserData)
    end

    it "should raise an exception if there are no broker hosts defined in the data" do
      lambda { RightAMQP::HABrokerClient.parse_user_data("blah") }.should raise_error(RightAMQP::HABrokerClient::NoBrokerHosts)
    end

    it "should translate old host name to standard form" do
      RightAMQP::HABrokerClient.parse_user_data("RS_rn_url=rs@broker.rightscale.com/right_net").should ==
        ["broker1-1.rightscale.com", nil]
    end

    it "should ignore repeated variables in the user data" do
      user_data = "RS_rn_url=amqp://49c7a840eb:5c9c0823cb@broker4-2.rightscale.com/right_net&RS_rn_id=6067794001&RS_server=my.rightscale.com&RS_sketchy=sketchy12-13.rightscale.com&RS_rn_host=:1,broker4-1.rightscale.com:0&RS_rn_url=amqp://49c7a840eb:5c9c0823cb@broker4-1.rightscale.com/right_net&RS_rn_id=6067770001&RS_server=my.rightscale.com&RS_sketchy=sketchy12-13.rightscale.com&RS_rn_host=:0,broker4-2.rightscale.com:1"
      RightAMQP::HABrokerClient.parse_user_data(user_data).should ==
        ["broker4-2.rightscale.com:1,broker4-1.rightscale.com:0", nil]
    end

  end # when parsing user_data

  context "when addressing" do

    it "should form list of broker addresses from specified hosts and ports" do
      RightAMQP::HABrokerClient.addresses("first,second", "5672, 5674").should ==
        [{:host => "first", :port => 5672, :index => 0}, {:host => "second", :port => 5674, :index => 1}]
    end

    it "should form list of broker addresses from specified hosts and ports and use ids associated with hosts" do
      RightAMQP::HABrokerClient.addresses("first:1,second:2", "5672, 5674").should ==
        [{:host => "first", :port => 5672, :index => 1}, {:host => "second", :port => 5674, :index => 2}]
    end

    it "should form list of broker addresses from specified hosts and ports and use ids associated with ports" do
      RightAMQP::HABrokerClient.addresses("host", "5672:0, 5674:2").should ==
        [{:host => "host", :port => 5672, :index => 0}, {:host => "host", :port => 5674, :index => 2}]
    end

    it "should use default host and port for broker identity if none provided" do
      RightAMQP::HABrokerClient.addresses(nil, nil).should == [{:host => "localhost", :port => 5672, :index => 0}]
    end

    it "should use default port when ports is an empty string" do
      RightAMQP::HABrokerClient.addresses("first, second", "").should ==
        [{:host => "first", :port => 5672, :index => 0}, {:host => "second", :port => 5672, :index => 1}]
    end

    it "should use default host when hosts is an empty string" do
      RightAMQP::HABrokerClient.addresses("", "5672, 5673").should ==
        [{:host => "localhost", :port => 5672, :index => 0}, {:host => "localhost", :port => 5673, :index => 1}]
    end

    it "should reuse host if there is only one but multiple ports" do
      RightAMQP::HABrokerClient.addresses("first", "5672, 5674").should ==
        [{:host => "first", :port => 5672, :index => 0}, {:host => "first", :port => 5674, :index => 1}]
    end

    it "should reuse port if there is only one but multiple hosts" do
      RightAMQP::HABrokerClient.addresses("first, second", 5672).should ==
        [{:host => "first", :port => 5672, :index => 0}, {:host => "second", :port => 5672, :index => 1}]
    end

    it "should apply ids associated with host" do
      RightAMQP::HABrokerClient.addresses("first:0, third:2", 5672).should ==
        [{:host => "first", :port => 5672, :index => 0}, {:host => "third", :port => 5672, :index => 2}]
    end

    it "should not allow mismatched number of hosts and ports" do
      runner = lambda { RightAMQP::HABrokerClient.addresses("first, second", "5672, 5673, 5674") }
      runner.should raise_exception(ArgumentError)
    end

  end # when addressing

  context "when identifying" do

    before(:each) do
      @address1 = {:host => "first", :port => 5672, :index => 0}
      @identity1 = "rs-broker-first-5672"
      @broker1 = flexmock("broker_client1", :identity => @identity1, :usable? => true,
                          :alias => "b0", :host => "first", :port => 5672, :index => 0)
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity1, @address1, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(@broker1).by_default

      @address2 = {:host => "second", :port => 5672, :index => 1}
      @identity2 = "rs-broker-second-5672"
      @broker2 = flexmock("broker_client2", :identity => @identity2, :usable? => true,
                          :alias => "b1", :host => "second", :port => 5672, :index => 1)
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity2, @address2, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(@broker2).by_default

      @address3 = {:host => "third", :port => 5672, :index => 2}
      @identity3 = "rs-broker-third-5672"
      @broker3 = flexmock("broker_client3", :identity => @identity3, :usable? => true,
                          :alias => "b2", :host => "third", :port => 5672, :index => 2)
      flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity3, @address3, @serializer,
              @exceptions, @non_deliveries, Hash, nil).and_return(@broker3).by_default
    end

    it "should use host and port to uniquely identity broker in AgentIdentity format" do
      RightAMQP::HABrokerClient.identity("localhost", 5672).should == "rs-broker-localhost-5672"
      RightAMQP::HABrokerClient.identity("10.21.102.23", 1234).should == "rs-broker-10.21.102.23-1234"
    end

    it "should replace '-' with '~' in host names when forming broker identity" do
      RightAMQP::HABrokerClient.identity("9-1-1", 5672).should == "rs-broker-9~1~1-5672"
    end

    it "should use default port when forming broker identity" do
      RightAMQP::HABrokerClient.identity("10.21.102.23").should == "rs-broker-10.21.102.23-5672"
    end

    it "should list broker identities" do
      RightAMQP::HABrokerClient.identities("first,second", "5672, 5674").should ==
        ["rs-broker-first-5672", "rs-broker-second-5674"]
    end

    it "should convert identities into aliases" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.aliases([@identity3]).should == ["b2"]
      ha.aliases([@identity3, @identity1]).should == ["b2", "b0"]
    end

    it "should convert identities into nil alias when unknown" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.aliases(["rs-broker-second-5672", nil]).should == [nil, nil]
    end

    it "should convert identity into alias" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.alias_(@identity3).should == "b2"
    end

    it "should convert identity into nil alias when unknown" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.alias_("rs-broker-second-5672").should == nil
    end

    it "should convert identity into parts" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.identity_parts(@identity3).should == ["third", 5672, 2, 1]
    end

    it "should convert an alias into parts" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.identity_parts("b2").should == ["third", 5672, 2, 1]
    end

    it "should convert unknown identity into nil parts" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.identity_parts("rs-broker-second-5672").should == [nil, nil, nil, nil]
    end

    it "should get identity from identity" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.get(@identity1).should == @identity1
      ha.get("rs-broker-second-5672").should be_nil
      ha.get(@identity3).should == @identity3
    end

    it "should get identity from an alias" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first:0, third:2", :port => 5672)
      ha.get("b0").should == @identity1
      ha.get("b1").should be_nil
      ha.get("b2").should == @identity3
    end

    it "should generate host:index list" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "second:1, first:0, third:2", :port => 5672)
      ha.hosts.should == "second:1,first:0,third:2"
    end

    it "should generate port:index list" do
      ha = RightAMQP::HABrokerClient.new(@serializer, :host => "second:1, third:2, first:0", :port => 5672)
      ha.ports.should == "5672:1,5672:2,5672:0"
    end

  end # when identifying

  context "when" do

    before(:each) do
      # Generate mocking for three BrokerClients
      # key index host     alias
      { 1 => [0, "first",  "b0"],
        2 => [1, "second", "b1"],
        3 => [2, "third",  "b2"] }.each do |k, v|
              i, h,         a = v
        eval("@identity#{k} = 'rs-broker-#{h}-5672'")
        eval("@address#{k} = {:host => '#{h}', :port => 5672, :index => #{i}}")
        eval("@broker#{k} = flexmock('broker_client#{k}', :identity => @identity#{k}, :alias => '#{a}', " +
                                     ":host => '#{h}', :port => 5672, :index => #{i})")
        eval("@broker#{k}.should_receive(:status).and_return(:connected).by_default")
        eval("@broker#{k}.should_receive(:usable?).and_return(true).by_default")
        eval("@broker#{k}.should_receive(:connected?).and_return(true).by_default")
        eval("@broker#{k}.should_receive(:subscribe).and_return(true).by_default")
        eval("flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity#{k}, @address#{k}, " +
                       "@serializer, @exceptions, @non_deliveries, Hash, nil).and_return(@broker#{k}).by_default")
      end
    end
  
    context "connecting" do

      it "should connect and add a new broker client to the end of the list" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first", :port => 5672)
        ha.brokers.size.should == 1
        ha.brokers[0].alias == "b0"
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity2, @address2, @serializer,
                 @exceptions, @non_deliveries, Hash, nil).and_return(@broker2).once
        res = ha.connect("second", 5672, 1)
        res.should be_true
        ha.brokers.size.should == 2
        ha.brokers[1].alias == "b1"
      end

      it "should reconnect an existing broker client after closing it if it is not connected" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:close).and_return(true).once
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity1, @address1, @serializer,
                 @exceptions, @non_deliveries, Hash, ha.brokers[0]).and_return(@broker1).once
        res = ha.connect("first", 5672, 0)
        res.should be_true
        ha.brokers.size.should == 2
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b1"
      end

      it "should not do anything except log a message if asked to reconnect an already connected broker client" do
        @logger.should_receive(:info).with(/Ignored request to reconnect/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        @broker1.should_receive(:status).and_return(:connected).once
        @broker1.should_receive(:close).and_return(true).never
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity1, @address1, @serializer,
                 @exceptions, @non_deliveries, Hash, ha.brokers[0]).and_return(@broker1).never
        res = ha.connect("first", 5672, 0)
        res.should be_false
        ha.brokers.size.should == 2
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b1"
      end

      it "should reconnect already connected broker client if force specified" do
        @logger.should_receive(:info).with(/Ignored request to reconnect/).never
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        @broker1.should_receive(:close).and_return(true).once
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity1, @address1, @serializer,
                 @exceptions, @non_deliveries, Hash, ha.brokers[0]).and_return(@broker1).once
        res = ha.connect("first", 5672, 0, nil, force = true)
        res.should be_true
        ha.brokers.size.should == 2
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b1"
      end

      it "should be able to change host and port of an existing broker client" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        @broker1.should_receive(:close).and_return(true).once
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity3, @address3.merge(:index => 0),
                 @serializer, @exceptions, @non_deliveries, Hash, nil).and_return(@broker3).once
        res = ha.connect("third", 5672, 0)
        res.should be_true
        ha.brokers.size.should == 2
        ha.brokers[0].alias == "b0"
        ha.brokers[0].identity == @address3
        ha.brokers[1].alias == "b1"
        ha.brokers[1].identity == @address_b
      end

      it "should slot broker client into specified priority position when at end of list" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        res = ha.connect("third", 5672, 2, 2)
        res.should be_true
        ha.brokers.size.should == 3
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b1"
        ha.brokers[2].alias == "b2"
      end

      it "should slot broker client into specified priority position when already is a client in that position" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        ha.brokers.size.should == 2
        res = ha.connect("third", 5672, 2, 1)
        res.should be_true
        ha.brokers.size.should == 3
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b2"
        ha.brokers[2].alias == "b1"
      end

      it "should slot broker client into nex priority position if specified priority would leave a gap" do
        @logger.should_receive(:info).with(/Reduced priority setting for broker/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first")
        ha.brokers.size.should == 1
        res = ha.connect("third", 5672, 2, 2)
        res.should be_true
        ha.brokers.size.should == 2
        ha.brokers[0].alias == "b0"
        ha.brokers[1].alias == "b2"
      end

      it "should yield to the block provided with the newly connected broker identity" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first")
        ha.brokers.size.should == 1
        ha.brokers[0].alias == "b0"
        identity = nil
        res = ha.connect("second", 5672, 1) { |i| identity = i }
        res.should be_true
        identity.should == @identity2
        ha.brokers.size.should == 2
        ha.brokers[1].alias == "b1"
      end

    end # connecting

    context "subscribing" do

      it "should subscribe on all usable broker clients and return their identities" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:subscribe).never
        @broker2.should_receive(:subscribe).and_return(true).once
        @broker3.should_receive(:subscribe).and_return(true).once
        result = ha.subscribe({:name => "queue"}, {:type => :direct, :name => "exchange"})
        result.should == [@identity2, @identity3]
      end

      it "should not return the identity if subscribe fails" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:subscribe).never
        @broker2.should_receive(:subscribe).and_return(true).once
        @broker3.should_receive(:subscribe).and_return(false).once
        result = ha.subscribe({:name => "queue"}, {:type => :direct, :name => "exchange"})
        result.should == [@identity2]
      end

      it "should subscribe only on specified brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:subscribe).never
        @broker2.should_receive(:subscribe).and_return(true).once
        @broker3.should_receive(:subscribe).never
        result = ha.subscribe({:name => "queue"}, {:type => :direct, :name => "exchange"},
                              :brokers => [@identity1, @identity2])
        result.should == [@identity2]
      end

    end # subscribing

    context "unsubscribing" do

      before(:each) do
        @timer = flexmock("timer", :cancel => true).by_default
        flexmock(EM::Timer).should_receive(:new).and_return(@timer).by_default
        @queue_name = "my_queue"
        @queue = flexmock("queue", :name => @queue_name)
        @queues = [@queue]
        @broker1.should_receive(:queues).and_return(@queues).by_default
        @broker1.should_receive(:unsubscribe).and_return(true).and_yield.by_default
        @broker2.should_receive(:queues).and_return(@queues).by_default
        @broker2.should_receive(:unsubscribe).and_return(true).and_yield.by_default
        @broker3.should_receive(:queues).and_return(@queues).by_default
        @broker3.should_receive(:unsubscribe).and_return(true).and_yield.by_default
      end

      it "should unsubscribe from named queues on all usable broker clients" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:unsubscribe).never
        @broker2.should_receive(:unsubscribe).and_return(true).once
        @broker3.should_receive(:unsubscribe).and_return(true).once
        ha.unsubscribe([@queue_name]).should be_true
      end

      it "should yield to supplied block after unsubscribing" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.subscribe({:name => @queue_name}, {:type => :direct, :name => "exchange"})
        called = 0
        ha.unsubscribe([@queue_name]) { called += 1 }
        called.should == 1
      end

      it "should yield to supplied block if timeout before finish unsubscribing" do
        flexmock(EM::Timer).should_receive(:new).with(10, Proc).and_return(@timer).and_yield.once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.subscribe({:name => @queue_name}, {:type => :direct, :name => "exchange"})
        called = 0
        ha.unsubscribe([@queue_name], 10) { called += 1 }
        called.should == 1
      end

      it "should cancel timer if finish unsubscribing before timer fires" do
        @timer.should_receive(:cancel).once
        flexmock(EM::Timer).should_receive(:new).with(10, Proc).and_return(@timer).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.subscribe({:name => @queue_name}, {:type => :direct, :name => "exchange"})
        called = 0
        ha.unsubscribe([@queue_name], 10) { called += 1 }
        called.should == 1
      end

      it "should yield to supplied block after unsubscribing even if no queues to unsubscribe" do
        @broker1.should_receive(:queues).and_return([])
        @broker2.should_receive(:queues).and_return([])
        @broker3.should_receive(:queues).and_return([])
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        called = 0
        ha.unsubscribe([@queue_name]) { called += 1 }
        called.should == 1
      end

      it "should yield to supplied block once after unsubscribing all queues" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.subscribe({:name => @queue_name}, {:type => :direct, :name => "exchange"})
        called = 0
        ha.unsubscribe([@queue_name]) { called += 1 }
        called.should == 1
      end

    end # unsubscribing

    context "declaring" do

      it "should declare exchange on all usable broker clients and return their identities" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:declare).never
        @broker2.should_receive(:declare).and_return(true).once
        @broker3.should_receive(:declare).and_return(true).once
        result = ha.declare(:exchange, "x", :durable => true)
        result.should == [@identity2, @identity3]
      end

      it "should not return the identity if declare fails" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:declare).never
        @broker2.should_receive(:declare).and_return(true).once
        @broker3.should_receive(:declare).and_return(false).once
        result = ha.declare(:exchange, "x", :durable => true)
        result.should == [@identity2]
      end

      it "should declare exchange only on specified brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:declare).never
        @broker2.should_receive(:declare).and_return(true).once
        @broker3.should_receive(:declare).never
        result = ha.declare(:exchange, "x", :durable => true, :brokers => [@identity1, @identity2])
        result.should == [@identity2]
      end

    end # declaring

    context "checking status" do

      before(:each) do
        @timeout = 10
        @timer = flexmock("timer", :cancel => true).by_default
        flexmock(EM::Timer).should_receive(:new).with(@timeout, Proc).and_return(@timer).by_default
        @queue_name = "my_queue"
        @queue = flexmock("queue", :name => @queue_name)
        @queues = [@queue]
        @broker1.should_receive(:queues).and_return(@queues).by_default
        @broker1.should_receive(:connected?).and_return(true).by_default
        @broker1.should_receive(:queue_status).and_return(true).by_default
        @broker2.should_receive(:queues).and_return(@queues).by_default
        @broker2.should_receive(:connected?).and_return(true).by_default
        @broker2.should_receive(:queue_status).and_return(true).by_default
        @ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
      end

      it "should not check status if there are no queues" do
        @ha.queue_status([]).should be_true
      end

      it "should make block call with empty status if there are no queues" do
        called = 0
        @ha.queue_status(["my_other_queue"], @timeout) { |status| status.should == {}; called += 1 }.should be_true
        called.should == 1
      end

      it "should wait to make callback until the status of all queues is obtained" do
        @broker1.should_receive(:queue_status).with([@queue_name], Proc).and_return(true).and_yield(@queue_name, 0, 1).once
        @broker2.should_receive(:queue_status).with([@queue_name], Proc).and_return(true).and_yield(@queue_name, 1, 2).once
        called = 0
        @ha.queue_status([@queue_name], @timeout) do |status|
          status.should == {@queue_name => {@broker1.identity => {:messages => 0, :consumers => 1},
                                            @broker2.identity => {:messages => 1, :consumers => 2}}}
          called += 1
        end.should be_true
        called.should == 1
      end

      it "should account for queues for which status cannot be obtained" do
        called = 0
        @broker1.should_receive(:queue_status).with([@queue_name], Proc).and_return(false).once
        @broker2.should_receive(:queue_status).with([@queue_name], Proc).and_return(true).and_yield(@queue_name, 1, 2).once
        @ha.queue_status([@queue_name], @timeout) do |status|
          status.should == {@queue_name => {@broker2.identity => {:messages => 1, :consumers => 2}}}
          called += 1
        end.should be_true
        called.should == 1
      end

    end # checking status

    context "publishing" do

      before(:each) do
        @broker1.should_receive(:publish).and_return(true).by_default
        @broker2.should_receive(:publish).and_return(true).by_default
        @broker3.should_receive(:publish).and_return(true).by_default
      end

      it "should serialize message, publish it, and return list of broker identifiers" do
        @serializer.should_receive(:dump).with(@packet).and_return(@message).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange", :options => {:durable => true}},
                   @packet, :persistent => true).should == [@identity1]
      end

      it "should try other broker clients if a publish fails" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:publish).and_return(false)
        ha.publish({:type => :direct, :name => "exchange"}, @packet).should == [@identity2]
      end

      it "should publish to a randomly selected broker if random requested" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        srand(100)
        ha.publish({:type => :direct, :name => "exchange"}, @packet, :order => :random,
                   :brokers =>[@identity1, @identity2, @identity3]).should == [@identity2]
      end

      it "should publish to all connected brokers if fanout requested" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet, :fanout => true,
                   :brokers =>[@identity1, @identity2]).should == [@identity1, @identity2]
      end

      it "should publish only using specified brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet,
                   :brokers =>[@identity1, @identity2]).should == [@identity1]
      end

      it "should log an error if a selected broker is unknown but still publish with any remaining brokers" do
        @logger.should_receive(:error).with(/Invalid broker identity "rs-broker-fourth-5672"/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet,
                   :brokers =>["rs-broker-fourth-5672", @identity1]).should == [@identity1]
      end

      it "should raise an exception if all available brokers fail to publish" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:publish).and_return(false)
        @broker2.should_receive(:publish).and_return(false)
        @broker3.should_receive(:publish).and_return(false)
        lambda { ha.publish({:type => :direct, :name => "exchange"}, @packet) }.
                should raise_error(RightAMQP::HABrokerClient::NoConnectedBrokers)
      end

      it "should not serialize the message if it is already serialized" do
        @serializer.should_receive(:dump).with(@packet).and_return(@message).never
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet, :no_serialize => true).should == [@identity1]
      end

      it "should store message info for use by message returns if :mandatory specified" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet, :mandatory => true).should == [@identity1]
        ha.instance_variable_get(:@published).instance_variable_get(:@cache).size.should == 1
      end

      it "should not store message info for use by message returns if message already serialized" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet, :no_serialize => true).should == [@identity1]
        ha.instance_variable_get(:@published).instance_variable_get(:@cache).size.should == 0
      end

      it "should not store message info for use by message returns if mandatory not specified" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.publish({:type => :direct, :name => "exchange"}, @packet).should == [@identity1]
        ha.instance_variable_get(:@published).instance_variable_get(:@cache).size.should == 0
      end

    end # publishing

    context "returning" do

      before(:each) do
        @broker1.should_receive(:publish).and_return(true).by_default
        @broker2.should_receive(:publish).and_return(true).by_default
        @broker3.should_receive(:publish).and_return(true).by_default
      end

      context "when non-delivery" do

        it "should store non-delivery block for use by return handler" do
          ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
          non_delivery = lambda {}
          ha.non_delivery(&non_delivery)
          ha.instance_variable_get(:@non_delivery).should == non_delivery
        end

      end

      context "when handling return" do

        before(:each) do
          @options = {}
          @brokers = [@identity1, @identity2]
          @ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
          @context = RightAMQP::HABrokerClient::Context.new(@packet, @options, @brokers)
          flexmock(@ha.instance_variable_get(:@published)).should_receive(:fetch).with(@message).and_return(@context).by_default
        end

        it "should republish using a broker not yet tried if possible and log that re-routing" do
          @logger.should_receive(:info).with(/RE-ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          @broker2.should_receive(:publish).and_return(true).once
          @context.record_failure(@identity1)
          @ha.__send__(:handle_return, @identity1, "to", "reason", @message)
        end

        it "should republish to same broker without mandatory if message is persistent and no other brokers available" do
          @logger.should_receive(:info).with(/RE-ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          @context.record_failure(@identity1)
          @context.record_failure(@identity2)
          @packet.should_receive(:persistent).and_return(true)
          @broker1.should_receive(:publish).and_return(true).once
          @ha.__send__(:handle_return, @identity2, "to", "NO_CONSUMERS", @message)
        end

        it "should republish to same broker without mandatory if message is one-way and no other brokers available" do
          @logger.should_receive(:info).with(/RE-ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          @context.record_failure(@identity1)
          @context.record_failure(@identity2)
          @packet.should_receive(:one_way).and_return(true)
          @broker1.should_receive(:publish).and_return(true).once
          @ha.__send__(:handle_return, @identity2, "to", "NO_CONSUMERS", @message)
        end

        it "should update status to :stopping if message returned because access refused" do
          @logger.should_receive(:info).with(/RE-ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          @context.record_failure(@identity1)
          @broker2.should_receive(:publish).and_return(true).once
          @broker1.should_receive(:update_status).with(:stopping).and_return(true).once
          @ha.__send__(:handle_return, @identity1, "to", "ACCESS_REFUSED", @message)
        end

        it "should log info and make non-delivery call even if persistent when returned because of no queue" do
          @logger.should_receive(:info).with(/NO ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          called = 0
          @ha.non_delivery { |reason, type, token, from, to| called += 1 }
          @context.record_failure(@identity1)
          @context.record_failure(@identity2)
          @packet.should_receive(:persistent).and_return(true)
          @broker1.should_receive(:publish).and_return(true).never
          @broker2.should_receive(:publish).and_return(true).never
          @ha.__send__(:handle_return, @identity2, "to", "NO_QUEUE", @message)
          called.should == 1
        end

        it "should log info and make non-delivery call if no route can be found" do
          @logger.should_receive(:info).with(/NO ROUTE/).once
          @logger.should_receive(:info).with(/RETURN reason/).once
          called = 0
          @ha.non_delivery { |reason, type, token, from, to| called += 1 }
          @context.record_failure(@identity1)
          @context.record_failure(@identity2)
          @broker1.should_receive(:publish).and_return(true).never
          @broker2.should_receive(:publish).and_return(true).never
          @ha.__send__(:handle_return, @identity2, "to", "any reason", @message)
          called.should == 1
        end

        it "should log info if no message context available for re-routing it" do
          @logger.should_receive(:info).with(/Dropping/).once
          flexmock(@ha.instance_variable_get(:@published)).should_receive(:fetch).with(@message).and_return(nil).once
          @ha.__send__(:handle_return, @identity2, "to", "any reason", @message)
        end

      end

    end # returning

    context "deleting" do

      it "should delete queue on all usable broker clients and return their identities" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:delete).never
        @broker2.should_receive(:delete).and_return(true).once
        @broker3.should_receive(:delete).and_return(true).once
        ha.delete("queue").should == [@identity2, @identity3]
      end

      it "should not return the identity if delete fails" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:delete).never
        @broker2.should_receive(:delete).and_return(true).once
        @broker3.should_receive(:delete).and_return(false).once
        ha.delete("queue").should == [@identity2]
      end

      it "should delete queue from cache on all usable broker clients and return their identities" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(false)
        @broker1.should_receive(:delete_amqp_resources).never
        @broker2.should_receive(:delete_amqp_resources).and_return(true).once
        @broker3.should_receive(:delete_amqp_resources).and_return(true).once
        ha.delete_amqp_resources("queue").should == [@identity2, @identity3]
      end

    end # deleting

    context "removing" do

      it "should remove broker client after disconnecting and pass identity to block" do
        @logger.should_receive(:info).with(/Removing/).once
        @broker2.should_receive(:close).with(true, true, false).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        identity = nil
        result = ha.remove("second", 5672) { |i| identity = i }
        result.should == @identity2
        identity.should == @identity2
        ha.get(@identity1).should_not be_nil
        ha.get(@identity2).should be_nil
        ha.get(@identity3).should_not be_nil
        ha.brokers.size.should == 2
      end

      it "should remove broker when no block supplied but still return a result" do
        @logger.should_receive(:info).with(/Removing/).once
        @broker2.should_receive(:close).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        result = ha.remove("second", 5672)
        result.should == @identity2
        ha.get(@identity1).should_not be_nil
        ha.get(@identity2).should be_nil
        ha.get(@identity3).should_not be_nil
        ha.brokers.size.should == 2
      end

      it "should remove last broker if requested" do
        @logger.should_receive(:info).with(/Removing/).times(3)
        @broker1.should_receive(:close).once
        @broker2.should_receive(:close).once
        @broker3.should_receive(:close).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        result = ha.remove("second", 5672)
        result.should == @identity2
        ha.get(@identity2).should be_nil
        result = ha.remove("third", 5672)
        result.should == @identity3
        ha.get(@identity3).should be_nil
        ha.brokers.size.should == 1
        identity = nil
        result = ha.remove("first", 5672) { |i| identity = i }
        result.should == @identity1
        identity.should == @identity1
        ha.get(@identity1).should be_nil
        ha.brokers.size.should == 0
      end

      it "should return nil and not execute block if broker is unknown" do
        @logger.should_receive(:info).with(/Ignored request to remove/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.remove("fourth", 5672).should be_nil
        ha.brokers.size.should == 3
      end

      it "should close connection and mark as failed when told broker is not usable" do
        @broker2.should_receive(:close).with(true, false, false).once
        @broker3.should_receive(:close).with(true, false, false).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        result = ha.declare_unusable([@identity2, @identity3])
        ha.brokers.size.should == 3
      end

      it "should raise an exception if broker that is declared not usable is unknown" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        lambda { ha.declare_unusable(["rs-broker-fourth-5672"]) }.should raise_error(Exception, /Cannot mark unknown/)
        ha.brokers.size.should == 3
      end

    end # removing

    context "monitoring" do

      before(:each) do
        @timer = flexmock("timer")
        flexmock(EM::Timer).should_receive(:new).and_return(@timer).by_default
        @timer.should_receive(:cancel).by_default
        @identity = "rs-broker-localhost-5672"
        @address = {:host => "localhost", :port => 5672, :index => 0}
        @broker = flexmock("broker_client", :identity => @identity, :alias => "b0", :host => "localhost",
                            :port => 5672, :index => 0)
        @broker.should_receive(:status).and_return(:connected).by_default
        @broker.should_receive(:usable?).and_return(true).by_default
        @broker.should_receive(:connected?).and_return(true).by_default
        @broker.should_receive(:subscribe).and_return(true).by_default
        flexmock(RightAMQP::BrokerClient).should_receive(:new).and_return(@broker).by_default
        @broker1.should_receive(:failed?).and_return(false).by_default
        @broker2.should_receive(:failed?).and_return(false).by_default
        @broker3.should_receive(:failed?).and_return(false).by_default
      end

      [:usable, :connected].each do |status|
        status_query = "#{status}?".to_sym

        it "should give access to or list #{status} brokers" do
          ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
          aliases = []
          res = ha.__send__(:each, status) { |b| aliases << b.alias }
          aliases.should == ["b0", "b1", "b2"]
          res.size.should == 3
          res[0].alias.should == "b0"
          res[1].alias.should == "b1"
          res[2].alias.should == "b2"

          @broker1.should_receive(status_query).and_return(true)
          @broker2.should_receive(status_query).and_return(false)
          @broker3.should_receive(status_query).and_return(false)
          aliases = []
          res = ha.__send__(:each, status) { |b| aliases << b.alias }
          aliases.should == ["b0"]
          res.size.should == 1
          res[0].alias.should == "b0"
        end

        it "should give access to each selected #{status} broker" do
          ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
          @broker2.should_receive(status_query).and_return(true)
          @broker3.should_receive(status_query).and_return(false)
          aliases = []
          res = ha.__send__(:each, status, [@identity2, @identity3]) { |b| aliases << b.alias }
          aliases.should == ["b1"]
          res.size.should == 1
          res[0].alias.should == "b1"
        end
      end

      it "should give list of unusable brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:usable?).and_return(true)
        @broker2.should_receive(:usable?).and_return(false)
        @broker3.should_receive(:usable?).and_return(false)
        ha.unusable.should == [@identity2, @identity3]
      end

      it "should tell whether a broker is connected" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker2.should_receive(:connected?).and_return(false)
        @broker3.should_receive(:connected?).and_return(true)
        ha.connected?(@identity2).should be_false
        ha.connected?(@identity3).should be_true
        ha.connected?("rs-broker-fourth-5672").should be_nil
      end

      it "should give list of all brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.all.should == [@identity1, @identity2, @identity3]
      end

      it "should give list of failed brokers" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:failed?).and_return(false)
        @broker3.should_receive(:failed?).and_return(true)
        ha.failed.should == [@identity1, @identity3]
      end

      it "should give broker client status list" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:summary).and_return("summary1")
        @broker2.should_receive(:summary).and_return("summary2")
        @broker3.should_receive(:summary).and_return("summary3")
        ha.status.should == ["summary1", "summary2", "summary3"]
      end

      it "should give broker client statistics" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:stats).and_return("stats1")
        @broker2.should_receive(:stats).and_return("stats2")
        @broker3.should_receive(:stats).and_return("stats3")
        ha.stats.should == {"brokers" => ["stats1", "stats2", "stats3"],
                            "exceptions" => nil,
                            "heartbeat" => nil,
                            "non-deliveries" => nil,
                            "returns" => nil}
      end

      it "should omit exceptions from client statistics if accumulated usin :exception_stats option" do
        stats = RightSupport::Stats::Exceptions.new
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third", :exception_stats => stats)
        @broker1.should_receive(:stats).and_return("stats1")
        @broker2.should_receive(:stats).and_return("stats2")
        @broker3.should_receive(:stats).and_return("stats3")
        ha.stats.should == {"brokers" => ["stats1", "stats2", "stats3"],
                            "heartbeat" => nil,
                            "non-deliveries" => nil,
                            "returns" => nil}
      end

      it "should log broker client status update if there is a change" do
        @logger.should_receive(:info).with(/Broker b0 is now connected/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.__send__(:update_status, @broker1, false)
      end

      it "should not log broker client status update if there is no change" do
        @logger.should_receive(:info).with(/Broker b0 is now connected/).never
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.__send__(:update_status, @broker1, true)
      end

      it "should log broker client status update when become disconnected" do
        @logger.should_receive(:info).with(/Broker b0 is now disconnected/).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        @broker1.should_receive(:status).and_return(:disconnected)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
      end

      it "should provide connection status callback when cross 0/1 connection boundary" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        connected = 0
        disconnected = 0
        ha.connection_status do |status|
          if status == :connected
            (ha.brokers[0].status == :connected ||
             ha.brokers[1].status == :connected).should be_true
            connected += 1
          elsif status == :disconnected
            (ha.brokers[0].status == :disconnected &&
             ha.brokers[1].status == :disconnected).should be_true
            disconnected += 1
          end
        end
        ha.__send__(:update_status, @broker1, false)
        connected.should == 0
        disconnected.should == 0
        @broker1.should_receive(:status).and_return(:disconnected)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
        connected.should == 0
        disconnected.should == 0
        @broker2.should_receive(:status).and_return(:disconnected)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, true)
        connected.should == 0
        disconnected.should == 1
        # TODO fix this test so that also checks crossing boundary as become connected
      end

      it "should provide connection status callback when cross n/n-1 connection boundary when all specified" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        connected = 0
        disconnected = 0
        ha.connection_status(:boundary => :all) do |status|
          if status == :connected
            (ha.brokers[0].status == :connected &&
             ha.brokers[1].status == :connected).should be_true
            connected += 1
          elsif status == :disconnected
            (ha.brokers[0].status == :disconnected ||
             ha.brokers[1].status == :disconnected).should be_true
            disconnected += 1
          end
        end
        ha.__send__(:update_status, @broker1, false)
        connected.should == 1
        disconnected.should == 0
        @broker1.should_receive(:status).and_return(:disconnected)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
        connected.should == 1
        disconnected.should == 1
        @broker2.should_receive(:status).and_return(:disconnected)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, true)
        connected.should == 1
        disconnected.should == 1
        # TODO fix this test so that also checks crossing boundary as become disconnected
      end

      it "should provide connection status callback for specific broker set" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        connected = 0
        disconnected = 0
        ha.connection_status(:brokers => [@identity2, @identity3]) do |status|
          if status == :connected
            (ha.brokers[1].status == :connected ||
             ha.brokers[2].status == :connected).should be_true
            connected += 1
          elsif status == :disconnected
            (ha.brokers[1].status == :disconnected &&
             ha.brokers[2].status == :disconnected).should be_true
            disconnected += 1
          end
        end
        ha.__send__(:update_status, @broker1, false)
        connected.should == 0
        disconnected.should == 0
        @broker1.should_receive(:status).and_return(:disconnected)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
        connected.should == 0
        disconnected.should == 0
        @broker2.should_receive(:status).and_return(:disconnected)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, true)
        connected.should == 0
        disconnected.should == 0
        @broker3.should_receive(:status).and_return(:disconnected)
        @broker3.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker3, true)
        connected.should == 0
        disconnected.should == 1
      end

      it "should provide connection status callback only once when one-off is requested" do
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity, @address, @serializer,
                @exceptions, @non_deliveries, Hash, nil).and_return(@broker).once
        ha = RightAMQP::HABrokerClient.new(@serializer)
        called = 0
        ha.connection_status(:one_off => 10) { |_| called += 1 }
        ha.__send__(:update_status, @broker, false)
        called.should == 1
        @broker.should_receive(:status).and_return(:disconnected)
        @broker.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker, true)
        called.should == 1
      end

      it "should use connection status timer when one-off is requested" do
        flexmock(EM::Timer).should_receive(:new).and_return(@timer).once
        @timer.should_receive(:cancel).once
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity, @address, @serializer,
                @exceptions, @non_deliveries, Hash, nil).and_return(@broker).once
        ha = RightAMQP::HABrokerClient.new(@serializer)
        called = 0
        ha.connection_status(:one_off => 10) { |_| called += 1 }
        ha.__send__(:update_status, @broker, false)
        called.should == 1
      end

      it "should give timeout connection status if one-off request times out" do
        flexmock(EM::Timer).should_receive(:new).and_return(@timer).and_yield.once
        @timer.should_receive(:cancel).never
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity, @address, @serializer,
                @exceptions, @non_deliveries, Hash, nil).and_return(@broker).once
        ha = RightAMQP::HABrokerClient.new(@serializer)
        called = 0
        ha.connection_status(:one_off => 10) { |status| called += 1; status.should == :timeout }
        called.should == 1
      end

      it "should be able to have multiple connection status callbacks" do
        flexmock(RightAMQP::BrokerClient).should_receive(:new).with(@identity, @address, @serializer,
                @exceptions, @non_deliveries, Hash, nil).and_return(@broker).once
        ha = RightAMQP::HABrokerClient.new(@serializer)
        called1 = 0
        called2 = 0
        ha.connection_status(:one_off => 10) { |_| called1 += 1 }
        ha.connection_status(:boundary => :all) { |_| called2 += 1 }
        ha.__send__(:update_status, @broker, false)
        @broker.should_receive(:status).and_return(:disconnected)
        @broker.should_receive(:connected?).and_return(false)
        @broker.should_receive(:failed?).and_return(false)
        ha.__send__(:update_status, @broker, true)
        called1.should == 1
        called2.should == 2
      end

      it "should provide failed connection status callback when all broker connections fail with :any option" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        connected = disconnected = failed = 0
        ha.connection_status(:boundary => :any) do |status|
          if status == :connected
            connected += 1
          elsif status == :disconnected
            disconnected += 1
          elsif status == :failed
            (ha.brokers[0].failed? &&
             ha.brokers[1].failed?).should be_true
            failed += 1
          end
        end
        @broker2.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, true)
        connected.should == 0
        disconnected.should == 0
        failed.should == 0
        @broker1.should_receive(:failed?).and_return(true)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
        connected.should == 0
        disconnected.should == 0
        failed.should == 1
      end

      it "should provide failed connection status callback when all broker connections fail with :all option" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second")
        connected = disconnected = failed = 0
        ha.connection_status(:boundary => :all) do |status|
          if status == :connected
            connected += 1
          elsif status == :disconnected
            disconnected += 1
          elsif status == :failed
            (ha.brokers[0].failed? &&
             ha.brokers[1].failed?).should be_true
            failed += 1
          end
        end
        @broker2.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, true)
        connected.should == 0
        disconnected.should == 1
        failed.should == 0
        @broker1.should_receive(:failed?).and_return(true)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, true)
        connected.should == 0
        disconnected.should == 1
        failed.should == 1
      end

      it "should provide failed connection status callback when all brokers fail to connect" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        connected = disconnected = failed = 0
        ha.connection_status(:boundary => :all) do |status|
          if status == :connected
            connected += 1
          elsif status == :disconnected
            disconnected += 1
          elsif status == :failed
            (ha.brokers[0].failed? &&
             ha.brokers[1].failed? &&
             ha.brokers[2].failed?).should be_true
            failed += 1
          end
        end
        @broker1.should_receive(:failed?).and_return(true)
        @broker1.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker1, false)
        connected.should == 0
        disconnected.should == 0
        failed.should == 0
        @broker2.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, false)
        connected.should == 0
        disconnected.should == 0
        failed.should == 0
        @broker3.should_receive(:failed?).and_return(true)
        @broker3.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker3, false)
        connected.should == 0
        disconnected.should == 0
        failed.should == 1
      end

      it "should provide failed connection status callback when brokers selected and all brokers fail to connect" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        connected = disconnected = failed = 0
        ha.connection_status(:boundary => :all, :brokers => [@broker2.identity, @broker3.identity]) do |status|
          if status == :connected
            connected += 1
          elsif status == :disconnected
            disconnected += 1
          elsif status == :failed
            (ha.brokers[0].failed? &&
             ha.brokers[1].failed?).should be_true
            failed += 1
          end
        end
        @broker1.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:failed?).and_return(true)
        @broker2.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker2, false)
        connected.should == 0
        disconnected.should == 0
        failed.should == 0
        @broker3.should_receive(:failed?).and_return(true)
        @broker3.should_receive(:connected?).and_return(false)
        ha.__send__(:update_status, @broker3, false)
        connected.should == 0
        disconnected.should == 0
        failed.should == 1
      end

    end # monitoring

    context "closing" do

      it "should close all broker connections and execute block after all connections are closed" do
        @broker1.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        @broker2.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        @broker3.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        called = 0
        ha.close { called += 1 }
        called.should == 1
      end

      it "should close broker connections when no block supplied" do
        @broker1.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        @broker2.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        @broker3.should_receive(:close).with(false, Proc).and_return(true).and_yield.once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.close
      end

      it "should close all broker connections even if encounter an exception" do
        @logger.should_receive(:error).with(/Failed to close/).once
        @broker1.should_receive(:close).and_return(true).and_yield.once
        @broker2.should_receive(:close).and_raise(StandardError).once
        @broker3.should_receive(:close).and_return(true).and_yield.once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        called = 0
        ha.close { called += 1 }
        called.should == 1
      end

      it "should close an individual broker connection" do
        @broker1.should_receive(:close).with(true).and_return(true).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.close_one(@identity1)
      end

      it "should not propagate connection status change if requested not to" do
        @broker1.should_receive(:close).with(false).and_return(true).once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        ha.close_one(@identity1, propagate = false)
      end

      it "should close an individual broker connection and execute block if given" do
        @broker1.should_receive(:close).with(true, Proc).and_return(true).and_yield.once
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        called = 0
        ha.close_one(@identity1) { called += 1 }
        called.should == 1
      end

      it "should raise exception if unknown broker" do
        ha = RightAMQP::HABrokerClient.new(@serializer, :host => "first, second, third")
        lambda { ha.close_one("rs-broker-fourth-5672") }.should raise_error(Exception, /Cannot close unknown broker/)
      end

    end # closing

  end # when

end # RightAMQP::HABrokerClient
