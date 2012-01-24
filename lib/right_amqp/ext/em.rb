begin
  require 'eventmachine'
rescue LoadError
  require 'rubygems'
  require 'eventmachine'
end

require File.expand_path('../emfork', __FILE__)
