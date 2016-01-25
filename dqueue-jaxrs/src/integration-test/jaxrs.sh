#!/usr/bin/env bash
set -e

echo "Publish/Consume"

startTime=`uuid -v 1`
curl -s http://localhost:8080/dqueue/v1/publish -X POST -H 'Accept: */*' \
-F contents=@$0 -F startTime=$startTime

curl -s -I http://localhost:8080/dqueue/v1/consume -H 'Accept: */*' | grep $startTime

echo "Publish/Consume with filters"

startTime=`uuid -v 1`
curl -s http://localhost:8080/dqueue/v1/publish -X POST -H 'X-Dqueue-Filters: key1=value1,key2=value2' \
-H 'Accept: */*' -F contents=@$0 -F startTime=$startTime

curl -s -I http://localhost:8080/dqueue/v1/consume -H 'X-Dqueue-Filters: key1=value1,key2=value2' \
-H 'Accept: */*' | grep $startTime

echo "Publish/Consume ordered"

dependency1=`uuid -v 1`
startTime1=`uuid -v 1`
curl -s http://localhost:8080/dqueue/v1/ordered/publish -X POST -H 'Accept: */*' \
-F contents=@$0 -F startTime=$startTime1 -F dependency=$dependency1

dependency2=$startTime1
startTime2=`uuid -v 1`
curl -s http://localhost:8080/dqueue/v1/ordered/publish -X POST -H 'Accept: */*' \
-F contents=@$0 -F startTime=$startTime2 -F dependency=$dependency2

curl -I http://localhost:8080/dqueue/v1/ordered/consume -H 'Accept: */*' | grep $startTime1

curl -I http://localhost:8080/dqueue/v1/ordered/consume -H 'Accept: */*' | grep 'No Content'

curl -I http://localhost:8080/dqueue/v1/ordered/delete/$startTime1 -X DELETE -H 'Accept: */*'

curl -I 'http://localhost:8080/dqueue/v1/ordered/consume' -H 'Accept: */*' | grep $startTime2

curl -I http://localhost:8080/dqueue/v1/ordered/delete/$startTime2 -X DELETE -H 'Accept: */*'

curl -I http://localhost:8080/dqueue/v1/ordered/consume -H 'Accept: */*' | grep 'No Content'
