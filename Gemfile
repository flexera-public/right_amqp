source :gemcutter
source 'http://gems.github.com'

gemspec

gem 'right_support',      '~> 1.2', :git => 'git@github.com:rightscale/right_support.git',
                          :branch => 'azure_11744_right_amqp'

# Lock eventmachine to a published and well-tested version to avoid picking up
# proprietary builds that happen to be installed locally
gem 'eventmachine', '0.12.10'

group :development do
  gem "rspec",       "~> 2.5"
  gem "flexmock",    "~> 0.9"
  gem "rake",        ">= 0.8.7"
  gem "ruby-debug",  ">= 0.10"
  gem "rspec",       "~> 2.5"
  gem "json",        "~> 1.6"
  gem "bacon"
end
