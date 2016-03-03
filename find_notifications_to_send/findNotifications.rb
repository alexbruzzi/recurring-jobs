#!/usr/bin/env ruby
#

require 'Date'
require 'cassandra'
require 'resque'
require 'gcm'
require 'apns'


module Notifications

  DEBUG = true

  KEYSPACE = 'octo'

  class TextGenerator

    TEMPLATES = [
      "Check out this cool new %{ name } for just Rs %{ price }.",
      "%{ name } is trending in %{ category } right now. Check it out",
      "You should totally see this %{ name } in %{ category }. It's just for %{ price }"
    ]

    def generate(product)
      pHash = {
        name: product['name'],
        category: product['categories'].join(','),
        price: product['price']
      }
      TEMPLATES.sample % pHash
    end
  end

  class Sender
    def self.establish_connection
      @@cluster = Cassandra.cluster
      @@session = @@cluster.connect(KEYSPACE)
    end

    def perform(uid, pid, eid)

      msgArgs = {}

      # get the user tokens
      args = [eid, uid]
      pushCql = "SELECT pushtoken, userid, pushtype \
      FROM push_tokens \
      WHERE enterpriseid = ? \
      AND userid = ?"
      res = @@session.execute(pushCql, arguments: args)
      if res.length > 0
        r = res.first
        msgArgs[:pushToken] = r['pushtoken']
        msgArgs[:pushType] = r['pushtype']
      end

      # get the enterprise token
      enterpriseCql = "SELECT pushtype, pushkey \
      FROM push_keys WHERE enterpriseid = ?"
      res = @@session.execute(enterpriseCql, arguments: [eid])
      if res.length > 0
        r = res.first
        msgArgs[:enterprisePushKey] = r['pushkey']
        msgArgs[:enterprisePushType] = r['pushtype']
      end

      # get the product to be sent
      productCql = "SELECT name, categories, tags, price \
      FROM products WHERE enterpriseid = ? AND id = ? LIMIT 1"
      res = @@session.execute(productCql, arguments: [eid, pid])
      if res.length == 1
        msgArgs[:product] = res.first
      end

      # generate the text to be sent
      text = TextGenerator.generate(msgArgs[:product])
      msgArgs[:text] = text

      # send the actual message to the user
      sendMessage(msgArgs)

    end

    # Sends the actual message to the user
    def sendMessage(msg)

    end
  end


  class Finder


    # Establishes data base connection and sessions
    def self.establish_connection
      @@cluster = Cassandra.cluster
      @@session = @@cluster.connect(KEYSPACE)
    end

    def initialize
      @enterprises = []
      @users = []
    end

    # Parses a timestamp value to return the dayOfWeek,
    #   timeOfDay and week values
    # @param [Time] tm The time value to be parses
    # @return [Array<Fixnum>] The dayOfWeek, timeOfDay and week
    #   value in that order.
    def parseDate(tm)
      tm.strftime('%w::%H%M::%U').split('::').map(&:to_i)
    end

    # Finds the notifications to be send and dispatch
    #   them in the notifications queue
    def find_and_dispatch

      # find users who have high probability of opening
      # app in the coming minute
      forTime = Time.now + (1 * 60)
      ts = parseDate(forTime)[1]

      findNotificationToSend(forTime)

      findUsers(ts)

    end

    private

    # Prepare statements
    def prepareStatements
      @fetchTimeSlotsStmt = @@session.prepare(
        "SELECT enterpriseid, timeofday, userid \
        FROM notification_time_slots \
        WHERE enterpriseif = ? AND \
        timeofday = ?"
      )
      @fetchTrendingProductStmt = @@session.prepare(
        "SELECT enterpriseid, productid, divergence \
        FROM kldivergence WHERE enterpriseid = ? \
        AND ts = ?"
      )
    end

    # Finds all the enterprises. This uses kong's consumer
    #   table to find all the enterprises.
    # @return [Array<String>] IDs of enterprises
    def findAllEnterprises
      findEnterprisesCQL = "SELECT id FROM kong.consumers"
      res = @@session.execute(findEnterprisesCQL)
      res.each do | r|
        @enterprises << r['id']
      end
      @enterprises
    end

    # Find the notification to send at a given timestamp
    # @param [Time] ts The time for which notifications would be generated
    def findNotificationToSend(ts)
      @trendingProducts = {}
      @enterprises.each do |eid|
        @trendingProducts[eid] = {}
        args = [eid, ts]
        res = @@session.execute(@fetchTrendingProductStmt, arguments: args)
        res.each do |r|
          pid = r['productid']
          div = r['divergence']
          @trendingProducts[eid].merge({ pid => div })
        end
        @trendingProducts[eid] = Hash[@trendingProducts[eid].sort_by { |k,v| -v }]
      end

      DEBUG ? $stdout.puts("Trending Products: #{ @trendingProducts }") : nil

    end

    # Find all users to whom the messages need to be sent
    def findUsers(ts)
      @enterprises.each do |eid|
        args = [eid, ts]
        res = @@session.execute(@fetchTimeSlotsStmt, arguments: args)
        res.each do |r|
          @users << r['userid']
        end
      end

      DEBUG ? $stdout.puts("Users Hash: #{ @users }") : nil
    end

    # Enqueue notifications for @users w.r.t. @trendingProducts
    def enqueueNotifications
      unless @trendingProducts.empty?
        @users.each do |eid, uids|
          uids.each do |uid|
            Resque.enque(Sender,
                         uid,
                         @trendingProducts[eid].shuffle[0],
                         eid)
          end
        end
      end
    end
  end

  class Scheduler

    def self.perform
      Finder.establish_connection
      finder = Finder.new
      finder.find_and_dispatch
    end
  end
end

def main
  Notifications::Scheduler.perform
end

if __FILE__ == $0
  main
end
