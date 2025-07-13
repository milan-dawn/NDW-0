
echo
echo "Sending messages to PUSH (Async) based consumers..."
echo

nats pub News.Sports.Football.College "College Football News - Test"
nats pub News.Sports.Football "General Football News - Test"
nats pub News.Sports.Baseball "Baseball scores - Test"

./cli_list_push_consumer_info.sh

