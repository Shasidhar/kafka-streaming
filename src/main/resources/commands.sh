#!/usr/bin/env bash

cd $KAFKA_HOME
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic wordcount < /Users/mcbk01/interests/structuredstreaming/kafka-streaming/src/main/resources/wcinput.txt

bin/kafka-console-consumer.sh --topic wcOutput --bootstrap-server localhost:9092 --from-beginning
