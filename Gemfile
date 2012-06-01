source :gemcutter
source 'http://gems.github.com'
source 'http://s3.amazonaws.com/rightscale_rightlink_gems_dev'

gemspec

gem 'right_support', '~> 1.2', :git => 'git@github.com:rightscale/right_support.git',
                     :branch => 'master'

# Lock eventmachine to a published and well-tested version to avoid picking up
# proprietary builds that happen to be installed locally
group :custom do
  gem "eventmachine", "1.0.0.beta.4", :git => 'https://github.com/eventmachine/eventmachine.git', :ref => '9bb885c035ac75e80d74da30829c9d2449dcf78d'
end

group :development do
  gem "rake",         "0.9.2.2"
  gem "ruby-debug19", :platforms => "mri_19"
  gem "rdoc",         "~> 3.12"
  gem "rspec",        "~> 2.8"
  gem "flexmock",     "~> 0.9"
  gem "bacon"
end
