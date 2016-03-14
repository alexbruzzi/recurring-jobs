#!/usr/bin/env ruby
#

require 'Date'
require 'cassandra'
require 'resque'
require 'gcm'
require 'apns'
require 'json'

module Notifications

  DEBUG = true

  KEYSPACE = 'octo'

  class TextGenerator

    TEMPLATES = [
      "Check out this cool new %{name} for just Rs %{price}.",
      "%{name} is trending in %{category} right now. Check it out",
      "You should totally see this %{name} in %{category}. It's just for %{price}"
    ]

    def self.generate(product, template=nil)

      pHash = {
        name: product['name'],
        category: product[:categories].shuffle[0],
        price: product['price'].round(2)
      }
      if template.nil?
        TEMPLATES.sample % pHash
      else
        template % pHash
      end
    end
  end

  class Sender

    @queue = :push_notification
    SCORE = 0.98

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
        msgArgs[:userToken] = {}
        res.each do |r|
          msgArgs[:userToken][r['pushtype']] = r['pushtoken']
        end
#        msgArgs[:pushToken] = r['pushtoken']
#        msgArgs[:pushType] = r['pushtype']
      end

      # get the enterprise token
      enterpriseCql = "SELECT pushtype, pushkey \
      FROM push_keys WHERE enterpriseid = ?"
      res = @@session.execute(enterpriseCql, arguments: [eid])
      if res.length > 0
        msgArgs[:pushKey] = {}
        res.each do |r2|
          msgArgs[:pushKey] = msgArgs[:pushKey].merge!( { r2['pushtype'] => r2['pushkey'] })
        end
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

      # get template defined for the client, if any
      trendingTemplate = nil
      trendingcql = "SELECT id FROM template_categories WHERE category_type = 'trending'"
      res = @@session.execute(trendingcql)
      if res.length > 0
        templateCql = "SELECT template_text FROM templates WHERE enterpriseid = ? AND tcid = ?"
        res2 = @@session.execute(templateCql, arguments: [eid, res.first['id']])
        if res2.length > 0
          trendingTemplate = res2.first['template_text']
        end
      end

      # generate the text to be sent
      text = TextGenerator.generate(msgArgs[:product], trendingTemplate)
      msgArgs[:text] = text

      # send the actual message to the user
      gcmresponse = sendMessage(msgArgs, eid)


      if gcmresponse
        # store the response
        self.store(gcmresponse, eid, uid)
      end


    end

    # Sends the actual message to the user
    def self.sendMessage(msg, eid)

      # generate the notification object
      notification = {
        title: 'Check this out',
        body: msg[:text]
      }

      # some random score to be sent
      score = { score: SCORE }
      if msg.has_key?(:userToken)
        msg[:userToken].each do |pushtype, pushtoken|
          if pushtype == 2
            # Check env and set the host accordingly
            #APNS.host = 'gateway.push.apple.com'
            APNS.host = 'gateway.sandbox.push.apple.com'
            APNS.pem  = self.getPEMLocationForClient(eid)
            apnsresponse = APNS.send_notification(pushtoken, :alert => notification, :other => score )
          elsif [0, 1].include?(pushtype)
              gcmClientKey = msg[:pushKey][pushtype]
              gcm = GCM.new(gcmClientKey)
              registration_ids = [pushtoken]
              options = {data: score, notification: notification, content_available: true, priority: 'high'}
              gcmresponse = gcm.send(registration_ids, options)
          end
        end
      end
    end

    # Generates the location where PEM file for
    #   APNS is stored
    def self.getPEMLocationForClient(eid)
      '/Users/pranav/Desktop/Certificates10.pem'
    end



    # Store the GCM response into DB
    def self.store(gcmresponse, eid, uid)

      rBody = JSON.parse(gcmresponse[:body])['results']
      res = rBody.first
      gcmid = res['message_id']
      args = [gcmid, eid, uid, SCORE, false, Time.now]


      gcmCql = "INSERT INTO gcm_notifications \
      (gcmid, enterpriseid, userid, score, ack, sent_at) \
      VALUES (?, ?, ?, ?, ?)"

      res = @@session.execute(gcmCql, arguments: args)

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
