#!/usr/bin/env ruby

require 'Date'
require 'Time'
require 'cassandra'


DEBUG = true

# Calculates the new base line for app.init event
#   per user.
class UserBaselineCalculator

  KEYSPACE = 'octo'

  # Establishes data base connection and sessions
  def self.establish_connection
    @@cluster = Cassandra.cluster
    @@session = @@cluster.connect(KEYSPACE)
  end

  # Initialise the class. By default it calculates
  #   for today, although date can be specified
  # @param [Date] dt The date for which baseline is to
  #   be calculated.
  def initialize(dt = Date.today)
    @ts = dt
    @enterprises = []
    prepareStatements
  end

  def update

    # find all the enterprises
    enterprises = findAllEnterprises
    DEBUG ? $stdout.puts("Enterprises: #{ enterprises }") : nil

    # Fetch all the user app inits happening today
    # Update the user_appopen_values table
    enterprises.each do |eid|
      updateAppInits(eid)
    end

    # Calculate the new baseline
    newBaseline = calculateNewBaseline
    existingBaseline = fetchCurrentBaseline

    # Update the new baseline in db
    updateBaseline(newBaseline)

    # Calculate divergances
    divergences = kldivergence(existingBaseline, newBaseline)

    # Update divergances in the db
    updateDivergences(divergences)
  end

  private

  # Prepares some CQL statements
  def prepareStatements
    @fetchAppInitStmt = @@session.prepare(
      "SELECT enterpriseid, userid, created_at \
      FROM app_init WHERE enterpriseid = ? AND \
      created_at > ? AND created_at < ?"
    )
    @insertAppOpenStmt = @@session.prepare(
      "INSERT INTO user_app_open \
      (enterpriseid, userid, dayofweek, timeofday, week, a, updated_at) \
      VALUES ( ?, ?, ?, ?, ?, 1, ?)"
    )
    @fetchAppOpenStmt = @@session.prepare(
      "SELECT enterpriseid, userid, week, timeofday \
      FROM user_app_open WHERE \
      enterpriseid = ? AND week IN (?) AND dayofweek = ?"
    )
    @fetchCurrBaselineStmt = @@session.prepare(
      "SELECT enterpriseid, userid, probability \
      FROM user_appopen_baseline \
      WHERE enterpriseid = ? AND dayofweek = ?"
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

  # Parses a timestamp value to return the dayOfWeek,
  #   timeOfDay and week values
  # @param [Time] tm The time value to be parses
  # @return [Array<Fixnum>] The dayOfWeek, timeOfDay and week
  #   value in that order.
  def parseDate(tm)
    tm.strftime('%w::%H%M::%U').split('::').map(&:to_i)
  end

  # Fetches all the app.init events happened yesterday
  #   for a given enterpriseid
  # @param [String] eid Enterprise Id of the enterprise
  def updateAppInits(eid)
    beginPeriod = @ts
    endPeriod = @ts + 1

    dayOfWeek = 0
    timeOfDay = 0

    args = [eid, beginPeriod.to_time.gmtime, endPeriod.to_time.gmtime]
    DEBUG ? $stdout.puts(args.to_s) : nil
    res = @@session.execute(@fetchAppInitStmt, arguments: args)
    res.each do | r |
      dayOfWeek, timeOfDay, week = parseDate(r['created_at'])
      argsx = [eid, r['userid'], dayOfWeek, timeOfDay, week, Time.now ]
      DEBUG ? $stdout.puts("#{ r['userid'] }, #{ dayOfWeek }, #{ timeOfDay }") : nil
      @@session.execute(@insertAppOpenStmt, arguments: argsx)
    end
  end

  # Calculates and returns the new baseline of app.init
  #   for all users for the day
  def calculateNewBaseline

    @enterprises.each do | eid |
      weeks = (0..TIME_WINDOW).collect do |d|
        Date.today - (d * 7)
      end.map(&:cweek)
      args = [eid, weeks, dayOfWeek]
      res = @@session.execute(@fetchAppOpenStmt, arguments: args)
    end

  end

  # Fetch current baseline
  def fetchCurrentBaseline
    @enterprises.each do |eid|
      args = [eid, dayOfWeek]
      res = @@session.execute(@fetchCurrBaselineStmt, arguments: args)
    end

  end

  # Update the db with baseline
  # @param [Hash] baseline The baseline which is to be updated
  def updateBaseline(baseline)

  end

  # Finds KL-Divergence between the existingBaseline and the newBaseline
  def kldivergence(existingBaseline, newBaseline)

  end

  # Updates the divergences
  def updateDivergences(divergences)

  end

end

class JobHandler

  def self.perform
    UserBaselineCalculator.establish_connection
    bsCalc = UserBaselineCalculator.new
    bsCalc.update
  end
end

def main
  $stdout.sync = true
  JobHandler.perform

end

if __FILE__ == $0
  main
end
