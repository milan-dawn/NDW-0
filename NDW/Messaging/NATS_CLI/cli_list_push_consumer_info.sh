
if [ "$#" -gt 0 ]; then
    echo
    nats consumer info NEWS CollegeFootballConsumer
    echo
    nats consumer info NEWS FootballConsumer
    echo
    nats consumer info NEWS SportsConsumer
fi

echo
echo "------- Begin PUSH (Asynchronous): Statistics ---------"
echo "CollegeFootballConsumer"
nats consumer info NEWS CollegeFootballConsumer | grep "Last Delivered Message"
nats consumer info NEWS CollegeFootballConsumer | grep "Redelivered Messages"
nats consumer info NEWS CollegeFootballConsumer | grep "Unprocessed Messages"
#echo

echo "FootballConsumer"
nats consumer info NEWS FootballConsumer | grep "Last Delivered Message"
nats consumer info NEWS FootballConsumer | grep "Redelivered Messages"
nats consumer info NEWS FootballConsumer | grep "Unprocessed Messages"

#echo
echo "SportsConsumer"
nats consumer info NEWS SportsConsumer | grep "Last Delivered Message"
nats consumer info NEWS SportsConsumer | grep "Redelivered Messages"
nats consumer info NEWS SportsConsumer | grep "Unprocessed Messages"
echo "------- END: PUSH (Asynchronous): Statistics ---------"
echo



