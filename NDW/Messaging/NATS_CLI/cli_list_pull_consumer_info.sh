
if [ "$#" -gt 0 ]; then
    echo
    nats consumer info NEWS WesternsMoviesArchiveConsumer
    echo
    nats consumer info NEWS RomanceMoviesArchiveConsumer
    echo
    nats consumer info NEWS MoviesArchiveConsumer
fi

echo
echo "------- Begin: PULL (Polling): Statistics ---------"
echo "WesternsMoviesArchiveConsumer ..."
nats consumer info NEWS WesternsMoviesArchiveConsumer | grep "Last Delivered Message"
nats consumer info NEWS WesternsMoviesArchiveConsumer | grep "Redelivered Messages"
nats consumer info NEWS WesternsMoviesArchiveConsumer | grep "Unprocessed Messages"
#echo

echo "RomanceMoviesArchiveConsumer ..."
nats consumer info NEWS RomanceMoviesArchiveConsumer | grep "Last Delivered Message"
nats consumer info NEWS RomanceMoviesArchiveConsumer | grep "Redelivered Messages"
nats consumer info NEWS RomanceMoviesArchiveConsumer | grep "Unprocessed Messages"

#echo
echo "MoviesArchiveConsumer ..."
nats consumer info NEWS MoviesArchiveConsumer | grep "Last Delivered Message"
nats consumer info NEWS MoviesArchiveConsumer | grep "Redelivered Messages"
nats consumer info NEWS MoviesArchiveConsumer | grep "Unprocessed Messages"
echo "------- END: PULL (Polling): Statistics ---------"
echo



