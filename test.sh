#! /bin/bash

if [ `which groovy` ]
	then 
	groovy server.groovy -c test_config.json -p 7777 &
	GROOVY_SERVER_PID_THAT_CERTAINLY_WONT_CONFLICT_WITH_ANOTHER_PROCESS=$!
	sleep 3 # Need a little lag till the server wakes up
	go test
	kill $GROOVY_SERVER_PID_THAT_CERTAINLY_WONT_CONFLICT_WITH_ANOTHER_PROCESS
else 
	echo 'Requires groovy'
	exit 1
fi

