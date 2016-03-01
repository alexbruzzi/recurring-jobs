# AppOpen baseLine #

This repo calculates the baseline per user per enterprise for `app.init` events. It finds the probability of a user _U_ opening the app at time _t_ (`HHMM` format). This probability is stored in the _user\_appopen\_baseline_ table.

This recurring job should run every 24 hours.

It takes into account last 8 weeks of data for a given day baseline calculation.

- Find all enterprises
- Find all (enterprise, userid) who opened app during yesterday
- Update this info into _user\_app\_open_
- For the day of week that was yesterday, find the app_open stats for last 8 weeks. Use this data to calculate the probability of `P(U, t)` and store it into the baseline table.

## Divergences ##

It also computes the divergences of each `(enterpriseid, userid, timestamp, dayofweek)` combination and stores it in database. Ideally, we would want to achieve a state where this divergence is stablised.