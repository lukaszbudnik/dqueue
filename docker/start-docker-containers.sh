#!/bin/bash

docker ps

docker start dqueue-graphite-statsd

docker start dqueue-zookeeper

docker start dqueue-cassandra

docker ps
