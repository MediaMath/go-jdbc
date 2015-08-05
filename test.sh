#! /bin/bash

if ! [ `which groovy` ]
	then
	echo 'Requires groovy'
	exit 1
fi

set -e

groovy server.groovy -c test_config.json -p 7777 &
GROOVY_SERVER_PID_THAT_CERTAINLY_WONT_CONFLICT_WITH_ANOTHER_PROCESS=$!

function stopGroovyServer {
	echo "Try killing" $GROOVY_SERVER_PID_THAT_CERTAINLY_WONT_CONFLICT_WITH_ANOTHER_PROCESS
	kill $GROOVY_SERVER_PID_THAT_CERTAINLY_WONT_CONFLICT_WITH_ANOTHER_PROCESS
}

trap stopGroovyServer EXIT
sleep 1 # Need a little lag till the server wakes up

go test $@