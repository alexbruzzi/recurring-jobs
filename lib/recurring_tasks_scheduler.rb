require 'octocore'

module Octo
  class RecurringTasksScheduler
    extend Octo::Scheduler

    class << self

      # Define all the schedules to setup here
      def setup_schedules

        # Setup counters schedule
        setup_schedule_counters
      end
    end

  end
end
