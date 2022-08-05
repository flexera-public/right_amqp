ARG BRANCH=latest
FROM rightscale/ops_os_base:${BRANCH}

#FROM rightscale/ops_ruby21x_build
# replace shell with bash so we can source files
RUN rm /bin/sh && ln -s /bin/bash /bin/sh

# update the repository sources list
# and install dependencies
RUN apt-get update \
    && apt-get install -y curl \
    && apt-get -y autoclean

RUN apt-get -qq update && \
    apt-get install --no-install-recommends -y \
    libsasl2-dev libmysqlclient-dev mysql-client wamerican nodejs npm putty gnupg2 libxslt-dev libxml2-dev && \
    npm config set strict-ssl false && \
    npm install --no-optional --production -g grunt-cli    

ENV RUBY_VERS=2.1.6

#ENV PATH $NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH
ENV PATH /usr/local/rvm/rubies/ruby-$RUBY_VERS/bin:$PATH
ENV PATH /usr/local/rvm/gems/ruby-$RUBY_VERS/bin:/usr/local/rvm/gems/ruby-$RUBY_VERS@global/bin:$PATH
ENV PATH $PATH:/usr/local/rvm/bin

#install RVM
RUN curl -sSL https://rvm.io/mpapis.asc | gpg2 --import -
RUN curl -sSL https://rvm.io/pkuczynski.asc | gpg2 --import -
RUN curl -L get.rvm.io | bash -s stable
RUN echo "source /etc/profile.d/rvm.sh" >> ~/.bashrc

#install ruby
RUN /bin/bash -l -c "rvm install $RUBY_VERS && \
        rvm use $RUBY_VERS --default"

ENV GEM_PATH /usr/local/rvm/gems/ruby-$RUBY_VERS:/usr/local/rvm/gems/ruby-$RUBY_VERS@global
ENV GEM_HOME /usr/local/rvm/gems/ruby-$RUBY_VERS

RUN gem install bundler -v '< 2'

RUN curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
RUN chmod +x /usr/local/bin/docker-compose
RUN ln -s /usr/local/bin/docker-compose /usr/bin/docker-compose

# install docker
RUN apt-get install apparmor --yes
RUN apt-get -yqq update
VOLUME ["/var/run/docker.sock"]
RUN apt-get -yqq install docker.io

# install git
RUN apt-get install git --yes

# update git configuration
RUN git config --global --add url.ssh://git@github.com/rightscale/.insteadof https://github.com/rightscale/

# Setup for github
RUN mkdir /root/.ssh/
RUN touch /root/.ssh/id_rsa
RUN chmod 700 /root/.ssh/id_rsa

# make sure your domain is accepted
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

# Create dir for right_amqp
RUN mkdir -p right_amqp

# Make right_amqp the working directory
WORKDIR /right_amqp