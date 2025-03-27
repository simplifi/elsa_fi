#!/bin/sh

alias kt='kafka-topics --bootstrap-server kafka:29092'
topics="elsa-topic"

echo 'Waiting for Kafka to be ready...'
while ! kt --list 2>/dev/null; do
  echo 'Kafka is not ready yet...'
  sleep 1
done

echo 'Kafka is ready, creating topics...'
for topic in $topics; do
  echo "Creating topic: $topic"
  kt --create --if-not-exists --topic $topic --replication-factor 1 --partitions 2
  if [ $? -eq 0 ]; then
    echo "Successfully created topic: $topic"
  else
    echo "Failed to create topic: $topic"
    exit 1
  fi
done

echo 'Successfully created the following topics:'
kt --list
