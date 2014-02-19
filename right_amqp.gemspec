# -*-ruby-*-
# Copyright: Copyright (c) 2012 RightScale, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# 'Software'), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
# CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
# TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'rubygems'

Gem::Specification.new do |spec|
  spec.name      = 'right_amqp'
  spec.version   = '0.8.2'
  spec.date      = '2014-02-19'
  spec.authors   = ['Lee Kirchhoff']
  spec.email     = 'lee@rightscale.com'
  spec.homepage  = 'https://github.com/rightscale/right_amqp'
  spec.platform  = Gem::Platform::RUBY
  spec.summary   = 'Client for interfacing to RightScale RabbitMQ broker using AMQP'
  spec.has_rdoc  = true
  spec.rdoc_options = ["--main", "README.rdoc", "--title", "RightAMQP"]
  spec.extra_rdoc_files = ["README.rdoc"]
  spec.required_ruby_version = '>= 1.8.7'
  spec.require_path = 'lib'

  spec.add_dependency('right_support', ['>= 1.2', '< 3.0'])
  spec.add_dependency('eventmachine', ['>= 0.12.10', '< 2.0'])

  spec.description = <<-EOF
RightAMQP provides a high availability client for interfacing with the
RightScale RabbitMQ broker using the AMQP protocol. The AMQP version on which
this gem is based is 0.6.7 but beyond that it contains a number of bug fixes and
enhancements including reconnect, message return, heartbeat, and UTF-8 support.
The high availability is achieved by maintaining multiple broker connections
such that failed connections automatically reconnect and only connected
brokers are used when routing a message. Although the HABrokerClient class
is the intended primary means for accessing RabbitMQ services with this gem,
alternatively the underlying AMQP services may be used directly.
EOF

  candidates = Dir.glob("{lib,spec}/**/*") +
               ["LICENSE", "README.rdoc", "Rakefile", "right_amqp.gemspec"]
  spec.files = candidates.sort
end
