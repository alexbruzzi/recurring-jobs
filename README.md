# Recurring Jobs #

This repo contains all the resque recurring jobs. Follow the individual README.md inside project dirs to know more about them.

## Execution ##

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
