require 'octocore'

module Octo
  module Scheduler
    class ProductHitAggregator
      @queue = :producthit_minute_aggregator

      class << self
        def perform(*args)
          type = args[0].to_sym
          if Octo::Counter.constants.include?type
            connect
            if type == :TYPE_MINUTE
              Octo::ProductHit.aggregate!
            else
              method_name = Octo::ProductHit.send(:type_counters_method_names, type)
              Octo::ProductHit.send(method_name.to_sym, Time.now.floor)
            end
          end
        end

        def connect
          dir = File.expand_path File.dirname(__FILE__)
          Octo.connect_with_config_file(File.join(dir, 'config.yml'))
        end
      end
    end
  end
end
