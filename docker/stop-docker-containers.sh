#!/bin/bash

docker ps

docker stop dqueue-graphite-statsd

docker stop dqueue-zookeeper

docker stop dqueue-cassandra

docker ps
