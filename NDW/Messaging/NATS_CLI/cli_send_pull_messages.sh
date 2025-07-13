
echo
echo "Sending messages to PULL (Poll) based consumers..."
echo

nats pub News.Archive.Movies.Romance "Romance movie archived - Test"
nats pub News.Archive.Movies.Westerns "Western movie archived - Test"
nats pub News.Archive.Movies.Action "Action movie archived - Test"

./cli_list_pull_consumer_info.sh

