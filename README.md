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

Use `Ctrl+C` to kill these.

## Execution (Daemon Mode)

### Scheduler

#### Start

```
BACKGROUND=yes PIDFILE=shared/pids/resque_scheduler.pid rake resque:scheduler >> /dev/null
```

#### Kill

```
kill -QUIT `cat shared/pids/resque_scheduler.pid`
```

### Worker

#### Start

```
BACKGROUND=yes PIDFILE=shared/pids/resque_worker.pid COUNT=10 QUEUE=* rake resque:workers >> /dev/null
```

#### Kill

```
kill -QUIT `cat shared/pids/resque_worker.pid`
```
