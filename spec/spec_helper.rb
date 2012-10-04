#
# Copyright (c) 2012 RightScale Inc
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

require 'rubygems'
require 'bundler/setup'

require 'flexmock'
require 'rspec'

RSpec.configure do |c|
  c.mock_with(:flexmock)
end

$TESTING = true
$VERBOSE = nil # Disable constant redefined warning

module RightAMQP

  module SpecHelper

    # Setup mocking of logger such that need to override :error and :warn
    # in specs that are expected to require use of these methods
    # Do not mock :exception because that gets eaten by Log::Mixin and results
    # in :error call
    def setup_logger
      @logger = flexmock("logger")
      @logger.should_receive(:level).and_return(:info).by_default
      @logger.should_receive(:exception).by_default.and_return { |m| raise m }
      @logger.should_receive(:error).by_default.and_return { |m| raise m }
      @logger.should_receive(:warn).by_default.and_return { |m| raise m }
      @logger.should_receive(:info).by_default
      @logger.should_receive(:debug).by_default
      RightSupport::Log::Mixin.default_logger = @logger
    end

  end

end
