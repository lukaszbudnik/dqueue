#!/bin/bash

#docker run -d -p 80:80 -p 8125:8125/udp -p 8126:8126 --name dqueue-grafana-dashboard kamon/grafana_graphite

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
