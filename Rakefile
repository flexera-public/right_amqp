#--  -*-ruby-*-
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
#++

require 'rubygems'
require 'bundler/setup'

require 'rake'
require 'rdoc/task'
require 'rubygems/package_task'
require 'rake/clean'
require 'rspec/core/rake_task'
require 'bacon'

desc "Run unit tests"
task :default => 'spec'

desc "Run unit tests"
RSpec::Core::RakeTask.new do |t|
  t.rspec_opts = ['--options', "\"#{File.dirname(__FILE__)}/spec/spec.opts\""]
end

namespace :spec do
  desc "Regenerate AMQP unit test code"
  task :codegen do
    sh 'ruby protocol/codegen.rb > lib/right_amqp/amqp/spec.rb'
    sh 'ruby lib/right_amqp/amqp/spec.rb'
  end

  desc "Run AMQP unit tests"
  task :amqp do
    sh 'bacon lib/right_amqp/amqp.rb'
  end
end

desc 'Generate documentation for the right_amqp gem'
Rake::RDocTask.new(:rdoc) do |rdoc|
  rdoc.rdoc_dir = 'doc/rdocs'
  rdoc.title = 'RightAMQP'
  rdoc.options << '--line-numbers' << '--inline-source'
  rdoc.rdoc_files.include('README.rdoc')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
CLEAN.include('doc/rdocs')

desc "Build right_amqp gem"
Gem::PackageTask.new(Gem::Specification.load("right_amqp.gemspec")) do |package|
  package.need_zip = true
  package.need_tar = true
end
CLEAN.include('pkg')
