require 'octocore'
require 'octorecommender'

module Octo
  class RecurringTasksScheduler
    extend Octo::Scheduler

    class << self

      # Define all the schedules to setup here
      def setup_schedules

        # Setup counters schedule
        schedule_counters

        # Setup baseline processing schedule
        schedule_baseline

        # Setup recommendations processing schedule
        schedule_recommender

        # Setup daily mailer schedule
        schedule_subscribermail
      end
    end

  end
end
