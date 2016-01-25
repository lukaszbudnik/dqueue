# dqueue [![Build Status](https://travis-ci.org/lukaszbudnik/dqueue.svg?branch=master)](https://travis-ci.org/lukaszbudnik/dqueue)
Distributed queue built on top of Apache Cassandra and Apache Zookeeper

## dqueue-jaxrs

Publish:

```
startTime=`uuid -v 1`
curl -v http://localhost:8080/dqueue/v1/publish -X POST -H 'Accept: */*' \
-F contents=@build.gradle -F startTime=$startTime
```

Consume:

```
curl -v http://localhost:8080/dqueue/v1/consume -H 'Accept: */*'
```

Publish with filters:

```
startTime=`uuid -v 1`
curl -v http://localhost:8080/dqueue/v1/publish -X POST -H 'X-Dqueue-Filters: key1=value1,key2=value2' \
-H 'Accept: */*' -F contents=@build.gradle -F startTime=$startTime
```

Consume with filters:

```
curl -v http://localhost:8080/dqueue/v1/consume -H 'X-Dqueue-Filters: key1=value1,key2=value2' \
-H 'Accept: */*'
```

Publish two ordered items:

```
dependency1=`uuid -v 1`
startTime1=`uuid -v 1`
curl -v http://localhost:8080/dqueue/v1/ordered/publish -X POST -H 'Accept: */*' \
-F contents=@build.gradle -F startTime=$startTime1 -F dependency=$dependency1
dependency2=$startTime1
startTime2=`uuid -v 1`
curl -v http://localhost:8080/dqueue/v1/ordered/publish -X POST -H 'Accept: */*' \
-F contents=@build.gradle -F startTime=$startTime2 -F dependency=$dependency2
```

Consume first item:

```
curl -v http://localhost:8080/dqueue/v1/ordered/consume -H 'Accept: */*'
```

Calling consume the second time returns 204 No Content. This is because first item is still in 'processing' state and blocking the second one: 

```
curl -v http://localhost:8080/dqueue/v1/ordered/consume -H 'Accept: */*'
```

Finish processing first item:

```
curl -v http://localhost:8080/dqueue/v1/ordered/delete/$startTime1 -X DELETE -H 'Accept: */*' 
```

Now consume second item:

```
curl -v 'http://localhost:8080/dqueue/v1/ordered/consume' -H 'Accept: */*'
```

And don't forget to delete it once you're done:

```
curl -v http://localhost:8080/dqueue/v1/ordered/delete/$startTime2 -X DELETE -H 'Accept: */*'
```

Ordered items also support `-H 'X-Dqueue-Filters: key1=value1,key2=value2'` header.
