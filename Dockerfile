#Use ruby-2.1.5 official image
FROM ruby:2.1.5

MAINTAINER Tushar Dwivedi  <tushar@octo.ai>

#Install dependencies:
RUN apt-get update

#Set ENV variable to store app inside the image
ENV INSTALL_PATH  /apps
RUN mkdir -p $INSTALL_PATH

#Ensure gems are cached and only get updated when they change.
WORKDIR /tmp
COPY  Gemfile /tmp/Gemfile
RUN bundle install

#Copy application code from workstation to the working directory
COPY  . $INSTALL_PATH

#Entry Point
WORKDIR $INSTALL_PATH
CMD ["bash", "-c", "rake resque:scheduler && rake resque:worker"]
#CMD rake resque:scheduler && rake resque:worker