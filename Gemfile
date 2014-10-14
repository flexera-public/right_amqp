source 'https://rubygems.org'

gemspec

gem 'rake',             '>= 0.9.2.2'

###
### Test-only gems
###
group :test do
  gem 'json',            '~> 1.4'
  gem 'rspec',           '~> 2.13.0'
  gem 'flexmock',        '~> 1.0'
  gem 'right_develop',   '~> 3.1'
  gem 'simplecov'
  gem 'bacon'
end

###
### Development-only gems (not available in CI).
### No version or Ruby VM constraints; assume these are always compatible with
### whatever Ruby version we're using, until we discover otherwise.
###
group :development do
  gem 'ruby-debug', :platforms => [:ruby_18]
  gem 'pry', :platforms => [:ruby_19, :ruby_20, :ruby_21]
  gem 'pry-byebug', :platforms => [:ruby_20, :ruby_21]
end
