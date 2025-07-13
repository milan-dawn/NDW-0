
set -x

nats consumer info NEWS FootballConsumer | grep "Unprocessed Messages:"
nats consumer info NEWS FootballConsumer | grep "Last Delivered Message:"
echo
nats consumer info NEWS SportsConsumer | grep "Unprocessed Messages:"
nats consumer info NEWS SportsConsumer | grep "Last Delivered Message:"
echo

