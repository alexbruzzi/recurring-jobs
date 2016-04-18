require 'resque/tasks'
require 'resque/scheduler/tasks'
require 'resque/scheduler/server'
require 'resque-scheduler'

require_relative 'recurring_tasks_scheduler'


Resque.after_fork do
  #Notifications::Sender.establish_connection
end

namespace :resque do

  task :setup do
    require 'resque'

    # Make sure Octo is plugged into the consciousness.
    dir = File.expand_path File.dirname(__FILE__)
    Octo.connect_with_config_file(File.join(dir, 'config.yml'))
    Resque.redis = Cequel::Record.redis
  end

  task :setup_schedule => :setup do
    Octo::RecurringTasksScheduler.setup_schedules
  end

  task :scheduler => :setup_schedule
end

