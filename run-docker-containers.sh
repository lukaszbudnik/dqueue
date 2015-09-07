#!/bin/bash

docker ps

docker run -d \
  --name dqueue-graphite-statsd \
  --restart=always \
  -p 80:80 \
  -p 8125:8125/udp \
  hopsoft/graphite-statsd

docker run -d \
  --name dqueue-zookeeper \
  -p 2181:2181 \
  springxd/zookeeper

docker run -d \
  --name dqueue-cassandra \
  -p 9042:9042 \
  cassandra

docker ps
