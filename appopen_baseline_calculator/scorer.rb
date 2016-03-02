#!/usr/bin/env ruby

require 'Date'
require 'cassandra'


DEBUG = true

# Calculates the new base line for app.init event
#   per user.
class Scorer

  KEYSPACE = 'octo'
  TIME_WINDOW = 7

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

    # Per user baseline
    @nextBaseLine = {}
    @currBaseline = {}

    # Global baseline is defined as the baseline
    # of all users for an enterprise
    @globalBaseline = {}

    # Contains total app opens happening for the day
    #   per enterprise
    @appOpenStats = {}

    prepareStatements
  end

  # Calculates the new baseline and updates the db with
  #   the new baseline as well as the divergences
  def update

    # find all the enterprises
    enterprises = findAllEnterprises
    DEBUG ? $stdout.puts("** Enterprises: #{ enterprises }") : nil

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

    # Update the new global baseline in db
    updateGlobalBaseline

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
      enterpriseid = ? AND week IN ? AND dayofweek = ?"
    )
    @fetchCurrBaselineStmt = @@session.prepare(
      "SELECT enterpriseid, userid, probability, timeofday \
      FROM user_appopen_baseline \
      WHERE enterpriseid = ? AND dayofweek = ?"
    )
    @updateBaselineStmt = @@session.prepare(
      "INSERT INTO user_appopen_baseline \
      (enterpriseid, dayofweek, userid, timeofday, probability, updated_at) \
      VALUES (?, ?, ?, ?, ?, ? )"
    )
    @updateDivergenceStmt = @@session.prepare(
      "INSERT INTO user_appopen_baseline_div \
      (enterpriseid, userid, dayofweek, timeofday, divergence) \
      VALUES ( ?, ?, ?, ?, ?)"
    )
    @updateGlobalBaselineStmt = @@session.prepare(
      "INSERT INTO global_appopen_baseline \
      (enterpriseid, dayofweek, timeofday, probability, updated_at) \
      VALUES (?, ?, ?, ?, ?)"
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

    res = @@session.execute(@fetchAppInitStmt, arguments: args)
    res.each do | r |
      dayOfWeek, timeOfDay, week = parseDate(r['created_at'])
      argsx = [eid, r['userid'], dayOfWeek, timeOfDay, week, Time.now ]
      @@session.execute(@insertAppOpenStmt, arguments: argsx)
    end
  end

  # Calculates and returns the new baseline of app.init
  #   for all users for the day
  # @return [Hash<Cassandra::Uuid => Hash<Fixnum => Hash<Fixnum => Float>>>]
  #   The calculated baseline
  def calculateNewBaseline

    # Create the data structure for storing app open time per user
    @enterprises.each do | eid |

      @globalBaseline[eid]  = {}
      @nextBaseLine[eid]    = {}

      weeks = (0..TIME_WINDOW).collect do |d|
        Date.today - (d * 7)
      end.map(&:cweek)

      dayOfWeek = parseDate(Date.today.prev_day.to_time)[0]
      args = [eid, weeks, dayOfWeek]
      res = @@session.execute(@fetchAppOpenStmt, arguments: args)

      res.each do |r|

        uid = r['userid']
        tod = r['timeofday']

        # update per user baseline
        uidStats = @nextBaseLine[eid].fetch(uid, {})
        todVal = uidStats.fetch(tod, 0) + 1
        uidStats[tod] = todVal

        uidStats.fetch(uid, {}).merge(uidStats)

        @nextBaseLine[eid][uid] = uidStats

        # update global baseline
        todVal = @globalBaseline[eid].fetch(tod, 0) + 1
        @globalBaseline[eid][tod] = todVal
      end
    end

    DEBUG ? $stdout.puts("** Absolute Vals of new base line: #{ @nextBaseLine }") : nil

    # Calculate the new per user baseline
    @nextBaseLine.each do | eid, users |
      users.each do |uid, tsFreq|
      sum = 1.0 * tsFreq.values.reduce{ |x,y| x+y }
        tsFreq.each do |ts, freq|
          @nextBaseLine[eid][uid][ts] = freq/sum
        end
      end
    end

    # Calculate the new global baseline
    @globalBaseline.each do |eid, tsFreq|
      unless tsFreq.empty?
        sum = 1.0 * tsFreq.values.reduce { |x,y| x+y }
        @appOpenStats[eid] = sum
        tsFreq.each do |ts, freq|
          @globalBaseline[eid][ts] = freq/sum
        end
      end
    end

    if DEBUG
      $stdout.puts "** New BaseLine:\n #{ @nextBaseLine }"
      $stdout.puts "** New Global BaseLine:\n #{ @globalBaseline }"
      $stdout.puts "** App Open Stats: #{ @appOpenStats }"
    end

    @nextBaseLine
  end

  # Fetch current baseline
  def fetchCurrentBaseline
    currBaseline = {}
    keys = ['enterpriseid', 'userid', 'probability', 'timeofday']
    dayOfWeek = parseDate(Date.today.prev_day.to_time)[0]

    @enterprises.each do |eid|
      currBaseline[eid] = {}
      args = [eid, dayOfWeek]
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

  # Update the db with baseline
  # @param [Hash] baseline The baseline which is to be updated
  def updateBaseline(baseline)
    dayOfWeek = parseDate(Date.today.prev_day.to_time)[0]
    @nextBaseLine.each do | eid, users |
      users.each do |uid, tsProb|
        tsProb.each do |ts, prob|
          args = [eid, dayOfWeek , uid, ts, prob, Time.now]
          @@session.execute(@updateBaselineStmt, arguments: args)
        end
      end
    end

  end

  # Calculates the KL-Divergance of two probabilities
  # https://en.wikipedia.org/wiki/Kullback–Leibler_divergence
  # @param [Float] p The first or observed probability
  # @param [Float] q The second or believed probability. Must be non-zero
  # @return [Float] KL-Divergance score
  def _klDivergence(p, q)
    1.0 * p * Math.log(p/q)
  end

  # Finds KL-Divergence between new baseline and current baseline
  # @param [Hash] newBaseline the new (calculated) baseline
  # @param [Hash] currBaseline the current (believed) baseline
  def kldivergence(newBaseline, currBaseline)
    divergenceScores = {}
    newBaseline.each do | eid, users|
      divergenceScores[eid] = {}
      users.each do | uid, tsProb|
        divergenceScores[eid][uid] = {}
        tsProb.each do |ts, prob|
          div = _klDivergence(prob, currBaseline[eid][uid][ts])
          DEBUG ? $stdout.puts("Divergence Score: #{ div }") : nil
          divergenceScores[eid][uid][ts] = div
        end
      end
    end

    DEBUG ? $stdout.puts("Divergence Scores: #{ divergenceScores }") : nil
    divergenceScores
  end

  # Updates the divergences into database
  # @param [Hash<Cassandra::Uuid => Hash<Fixnum => Hash<Fixnum => Float>>>] divergences
  #   The divergences to be updated
  def updateDivergences(divergences)
    dayOfWeek = parseDate(Date.today.prev_day.to_time)[0]
    divergences.each do |eid, users|
      users.each do |uid, tsDiv|
        tsDiv.each do |ts, div|
          args = [eid, uid, dayOfWeek, ts, div]
          @@session.execute(@updateDivergenceStmt, arguments: args)
        end
      end
    end
  end

  # Updates the global baseline to db
  # @param [Hash] baseline The global baseline to be updated
  def updateGlobalBaseline
    dayOfWeek = parseDate(Date.today.prev_day.to_time)[0]
    @globalBaseline.each do |eid, tsProb|
      tsProb.each do |ts, prob|
        args = [eid, dayOfWeek, ts, prob, Time.now]
        @@session.execute(@updateGlobalBaselineStmt, arguments: args)
      end
    end
  end

end

# A JobHandler class to handle all jobs
class JobHandler

  def self.perform
    Scorer.establish_connection
    bs = Scorer.new
    bs.update
  end
end

def main
  $stdout.sync = true
  JobHandler.perform
end

if __FILE__ == $0
  main
end
