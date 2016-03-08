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
      "Check out this cool new %{name} for just Rs %{price}.",
      "%{name} is trending in %{category} right now. Check it out",
      "You should totally see this %{name} in %{category}. It's just for %{price}"
    ]

    def self.generate(product)
      pHash = {
        name: product['name'],
        category: product[:categories].shuffle[0],
        price: product['price'].round(2)
      }
      TEMPLATES.sample % pHash
    end
  end

  class Sender

    @queue = :push_notification

    def self.establish_connection
      @@cluster = Cassandra.cluster
      @@session = @@cluster.connect(KEYSPACE)
    end

    def self.perform(uid, pid, eid)

      msgArgs = {}

      eid = Cassandra::Uuid.new(eid)


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

      # get the categories for the product
      msgArgs[:product].delete('categories').each do |catId|
        categoriesCql = "SELECT cat_text FROM categories_rev \
        WHERE id = ?"
        res = @@session.execute(categoriesCql, arguments: [catId])
        if res.length == 1
          r = res.first
          msgArgs[:product][:categories] = msgArgs[:product].fetch(:categories, []) << r['cat_text']
        end
      end

      # get the tags for the product
      tagCql = @@session.prepare("SELECT tag_text FROM tags_rev WHERE id = ?")
      msgArgs[:product].delete('tags').each do |tagId|
        res = @@session.execute(tagCql, arguments: [tagId])
        if res.length == 1
          r = res.first
          msgArgs[:product][:tags] = msgArgs[:product].fetch(:tags, []) << r['tag_text']
        end
      end

      # generate the text to be sent
      text = TextGenerator.generate(msgArgs[:product])
      msgArgs[:text] = text

      # send the actual message to the user
      sendMessage(msgArgs, eid)

    end

    # Sends the actual message to the user
    def self.sendMessage(msg, eid)
      if msg.has_key?(:pushToken)

        gcm = GCM.new(msg[:enterprisePushKey])
        registration_ids = [msg[:pushToken]]
        notification = {
          title: 'Check this out',
          body: msg[:text]
        }
        options = {data: {score: '0.98' }, notification: notification}
        response = gcm.send(registration_ids, options)

        # do something with response
        DEBUG ? $stdout.puts(response) : nil
      end

    end
  end

  # Helper class to find the floor and ceil of current time
  # for a window
  class TimeHelper

    # Find floor time
    # @param [Time] t Time for which floor needs to be found
    # @param [Fixnum] n The interval for time window. Defaults to 5
    def self.floor_to_n_minutes(t, n=5)
      sec = n * 60
      rounded = Time.at((t.to_time.to_i / sec).round * sec)
      t.is_a?(DateTime) ? rounded.to_datetime : rounded
    end

    # Find ceil time
    # @param [Time] t Time for which floor needs to be found
    # @param [Fixnum] n The interval for time window. Defaults to 5
    def self.ceil_to_n_minutes(t, n=5)
      sec = n * 60
      rounded = Time.at((1 + (t.to_time.to_i / sec)).round * sec)
      t.is_a?(DateTime) ? rounded.to_datetime : rounded
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
      @users = {}
      prepareStatements
      findAllEnterprises
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
      timeForTrendingProducts = TimeHelper.floor_to_n_minutes(Time.now, 1)
      ts = parseDate(TimeHelper.ceil_to_n_minutes(Time.now, 1))[1]

      findNotificationToSend(timeForTrendingProducts)

      findUsers(ts)

      enqueueNotifications

    end

    private

    # Prepare statements
    def prepareStatements
      @fetchTimeSlotsStmt = @@session.prepare(
        "SELECT enterpriseid, timeofday, userid \
        FROM notification_time_slots \
        WHERE enterpriseid = ? AND \
        timeofday = ?"
      )
      @fetchTrendingProductStmt = @@session.prepare(
        "SELECT enterpriseid, productid, rank \
        FROM trending_products WHERE enterpriseid = ? \
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
      DEBUG ? $stdout.puts("For fetch notification" + ts.gmtime.to_s) : nil
      @trendingProducts = {}
      @enterprises.each do |eid|
        @trendingProducts[eid] = {}
        args = [eid, ts]
        res = @@session.execute(@fetchTrendingProductStmt, arguments: args)
        res.each do |r|
          pid = r['productid']
          rank = r['rank']
          @trendingProducts[eid] = @trendingProducts[eid].merge({ pid => rank })
        end
        @trendingProducts[eid] = Hash[@trendingProducts[eid].sort_by { |k,v| v }].keys
      end

      DEBUG ? $stdout.puts("** Trending Products: #{ @trendingProducts }") : nil

    end

    # Find all users to whom the messages need to be sent
    def findUsers(ts)
      @enterprises.each do |eid|
        @users[eid] = []
        args = [eid, ts]
        res = @@session.execute(@fetchTimeSlotsStmt, arguments: args)
        res.each do |r|
          @users[eid] = @users[eid] << r['userid']
        end
      end

      DEBUG ? $stdout.puts("** Users : #{ @users }") : nil
    end

    # Enqueue notifications for @users w.r.t. @trendingProducts
    def enqueueNotifications
      DEBUG ? $stdout.puts("Trending products #{ @trendingProducts }") : nil

      @trendingProducts.each do |eid, products|
        if products.length > 0
          @users[eid].each do |uid|
            pid = products.shuffle[0]
            Resque.enqueue(Sender, uid, pid, eid)
          end
        end
      end
    end
  end

  class Scheduler

    @queue = :find_push_notifications

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
