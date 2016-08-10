# Recurring Jobs #

[![Build Status](https://travis-ci.org/octoai/recurring-jobs.svg?branch=master)](https://travis-ci.org/octoai/recurring-jobs)

This repo contains all the resque recurring jobs. Follow the individual README.md inside project dirs to know more about them.

Clone and perform `git submodule init`

## Config

By default it loads the config from `lib/config` directory. It also provides an environment variable `CONFIG_DIR` to explicitly specify the config dir. 

## Execution ##

For development purposes:

#### Start the scheduler ####

```bash
rake resque:scheduler CONFIG_DIR=/path/to/config
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
