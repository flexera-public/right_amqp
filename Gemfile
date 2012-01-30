source :gemcutter
source 'http://gems.github.com'
source 'http://s3.amazonaws.com/rightscale_rightlink_gems_dev'

gemspec

gem 'right_support', '~> 1.2', :git => 'git@github.com:rightscale/right_support.git',
                     :branch => 'master'

# Lock eventmachine to a published and well-tested version to avoid picking up
# proprietary builds that happen to be installed locally
group :custom do
  gem "eventmachine", "0.12.11.6"
end

group :development do
  gem "rake",        ">= 0.8.7"
  gem "ruby-debug",  ">= 0.10"
  gem 'rdoc',        ">= 2.4.2"
  gem "rspec",       "~> 2.5"
  gem "flexmock",    "~> 0.9"
  gem "bacon"
end
