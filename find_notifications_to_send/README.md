# find\_notifications\_to\_send #

This script answers following questions:

- Which all users should be notified in next minute?
- What notifications should be sent to each user?

The way this works is as follows:

- The script runs every minute
- Finds out the users who have high probability of opening app in next time frame
- Find trending product right now
- Create a message containing `(userid, [productid1, productid2,...])` and send it to kafka for the notification sending clients to recieve