#!/usr/bin/env bash

topic="megaphone-control"
$KAFKA/bin/kafka-configs.sh --zookeeper localhost --alter --entity-type topics --entity-name $topic --add-config retention.ms=1000
$KAFKA/bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic $topic
$KAFKA/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $topic

GREEN='\033[1;32m'
NC='\033[0m' # No Color
echo -e "${GREEN}Created topic \"$topic\"${NC}"
