# Recurring Jobs #

This repo contains all the resque recurring jobs. Follow the individual README.md inside project dirs to know more about them.

## Current Libs ##

- appopen\_baseline\_calculator: Calculates app open baseline per user
- find\_notifications\_to\_send: Finds which notification has to be sent to which user
- notification\_timeslots\_precalculate: Precalculate the time at which notifications would be sent to user


## Execution ##

The whole thing works a resque recurring jobs.

- `find_notifications_to_send` happens every 1 minute
- `appopen_baseline_calculator` happens everyday at 00:05 hrs
- `notification_timeslots_precalculate` happens everyday at 23:50 hrs

For development purposes:

#### Start the scheduler ####

```bash
rake resque:scheduler
```

#### Start the workers ####

Start the individual workers like this:

```
QUEUE=push_notification rake resque:work
QUEUE=find_push_notifications rake resque:work
QUEUE=producthit_minute_aggregator rake resque:work
```

Alternate way with multiple workers

```bash
COUNT=10 QUEUE=* rake resque:workers
```
