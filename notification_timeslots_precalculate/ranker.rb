#!/usr/bin/env ruby

require 'Date'
require 'cassandra'

module Ranker

  DEBUG = true

  class Ranker

    KEYSPACE = 'octo'

    # Maximum number of slots for which a user is eligible.
    # Generally this translates to how many times the user
    # can get notifications.
    MAX_SLOTS_PER_USER = 2

    # Establishes data base connection and sessions
    def self.establish_connection
      @@cluster = Cassandra.cluster
      @@session = @@cluster.connect(KEYSPACE)
    end

    # Initialize the ranker. While ranker provides a great
    #   flexibility for when and how should it be invoked,
    #   It is best to use it with conjugation with the time
    #   of recurring job. So, if you want a recurring ranker
    #   job to be scheduled at 0010 hrs everyday for today,
    #   you should have a delta of 0. If you want a recurring
    #   ranker at 2350 hrs everyday, you should have a delta
    #   of 1.
    # @param [Fixnum] delta The day on which ranker should work.
    #   If set to 1, will rank for 'tomorrow'. If set to -1, will
    #   rank for 'yesterday' and so on.
    # @param [Date] dt The date on which ranker is invoked.
    #   By default, it is today. But can be changed to some
    #   other date as well.
    def initialize(delta=1, dt = Date.today)
      @dt = dt + delta
      @dayOfWeek = parseDate(dt.to_time)[0]

      if !@@session
        self.establish_connection
      end

      @enterprises = []

      prepareStatements
      findAllEnterprises
    end

    # Rank user-timings to find out the users for whom the
    #   notifications would be sent
    def rank

      # fetch baseline
      baseline = fetchCurrentBaseline

      # fetch global baseline
      globalBaseline = fetchGlobalBaseline

      # create time slots
      timeSlots = createTimeSlots(baseline)

      # Store time slots in db
      updateTimeSlots(timeSlots)
    end

    private

    # Prepares the CQL Statements
    def prepareStatements
      @fetchCurrBaselineStmt = @@session.prepare(
        "SELECT enterpriseid, userid, probability, timeofday \
        FROM user_appopen_baseline \
        WHERE enterpriseid = ? AND dayofweek = ?"
      )
      @fetchGlobalBaselineStmt = @@session.prepare(
        "SELECT enterpriseid, timeofday, probability \
        FROM global_appopen_baseline \
        WHERE enterpriseid = ? AND dayofweek = ?"
      )
      @updateNotificationSlotsStmt = @@session.prepare(
        "INSERT INTO notification_time_slots \
        (enterpriseid, userid, timeofday, updated_at) \
        VALUES (?, ?, ?, ?)"
      )
    end

    # Parses a timestamp value to return the dayOfWeek,
    #   timeOfDay and week values
    # @param [Time] tm The time value to be parses
    # @return [Array<Fixnum>] The dayOfWeek, timeOfDay and week
    #   value in that order.
    def parseDate(tm)
      tm.strftime('%w::%H%M::%U').split('::').map(&:to_i)
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

    # Fetch current baseline
    def fetchCurrentBaseline
      currBaseline = {}
      keys = ['enterpriseid', 'userid', 'probability', 'timeofday']


      @enterprises.each do |eid|
        currBaseline[eid] = {}
        args = [eid, @dayOfWeek]
        res = @@session.execute(@fetchCurrBaselineStmt, arguments: args)

        res.each do |r|
          enterpriseid, uid, prob, ts = r.values_at(*keys)
          _userBaseline = currBaseline[enterpriseid].fetch(uid, {})
          _userBaseline[ts] = prob
          currBaseline[eid][uid] = _userBaseline
        end
      end

      DEBUG ? $stdout.puts("** CurrBaseLine: #{ currBaseline }") : nil
      @currBaseline = currBaseline
      currBaseline
    end

    # Fetch global baseline
    def fetchGlobalBaseline
      @globalBaseline = {}
      @enterprises.each do |eid|
        @globalBaseline[eid] = {}
        args = [eid, @dayOfWeek]
        res = @@session.execute(@fetchGlobalBaselineStmt, arguments: args)
        res.each do |r|
          tod = r['timeofday']
          prob = r['probability']
          @globalBaseline[eid][tod] = prob
        end
      end
      @globalBaseline
    end

    # Find time slots for a baseline
    def createTimeSlots(baseline)
      timeSlots = {}
      baseline.each do |eid, users|
        timeSlots[eid] = {}
        users.each do |uid, tsProb|
          slots = Hash[tsProb.sort_by { |k,v| v }].keys[0...MAX_SLOTS_PER_USER]
          unless slots.empty?
            slots.each do |slotTs|
              currUsersForTimeSlot = timeSlots.fetch(slotTs, [])
              currUsersForTimeSlot << uid
              timeSlots.merge({slotTs => currUsersForTimeSlot })
            end
          end
        end
      end

      DEBUG ? $stdout.puts("TimeSlots: #{ timeSlots }") : nil
      timeSlots
    end

    # Updates time slots into the database
    def updateTimeSlots(timeSlots)
      timeSlots.each do |eid, tsUsers|
        tsUsers.each do |ts, users|
          users.each do |uid|
            args = [eid, uid, ts, Time.now]
            @@session.execute(@updateNotificationSlotsStmt, arguments: args)
          end
        end
      end
    end

  end

  class JobHandler

    def self.perform
      Ranker.establish_connection
      ranker = Ranker.new(-2, Date.today)
      ranker.rank

    end
  end

  def self.main
    JobHandler.perform
  end

end

if __FILE__ == $0
  Ranker.main
end
