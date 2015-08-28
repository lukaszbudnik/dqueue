package com.github.lukaszbudnik.dqueue;

import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QueueClientIntegrationTest {

    private int cassandraPort = 32775;
    private String[] cassandraAddress = new String[]{"192.168.99.100"};
    private String cassandraKeyspace = "test" + System.currentTimeMillis();
    private String cassandraTablePrefix = "queue";

    private QueueClient queueClient;
    private Session session;

    @Before
    public void before() throws Exception {
        queueClient = new QueueClientBuilder()
                .withCassandraPort(cassandraPort)
                .withCassandraAddress(cassandraAddress)
                .withCassandraKeyspace(cassandraKeyspace)
                .withCassandraTablePrefix(cassandraTablePrefix)
                .withCassandraCreateTables(true)
                .build();
        session = queueClient.getSession();
        session.execute("create keyspace " + cassandraKeyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}");
    }

    @After
    public void after() throws Exception {
        session.execute("drop keyspace " + cassandraKeyspace);
        queueClient.close();
    }

    @Test
    public void shouldReturnNothingIfQueueIsEmpty() throws ExecutionException, InterruptedException {
        Future<Map<String, Object>> itemFuture = queueClient.consume(ImmutableMap.of("type", 123));

        Map<String, Object> item = itemFuture.get();

        assertNull(item);
    }

    @Test
    public void shouldPublishAndConsumeWithoutFilters() throws ExecutionException, InterruptedException {
        String name = "name 1";
        ByteBuffer contents = ByteBuffer.wrap(name.getBytes());

        Future<UUID> publishFuture = queueClient.publish(contents);
        UUID startTime = publishFuture.get();

        Future<Map<String, Object>> itemFuture = queueClient.consume();

        Map<String, Object> item = itemFuture.get();

        UUID consumedStartTime = (UUID) item.get("start_time");
        ByteBuffer consumedContents = (ByteBuffer) item.get("contents");

        assertEquals(startTime, consumedStartTime);
        assertEquals(contents, consumedContents);
    }

    @Test
    public void shouldPublishAndConsumeWithManyFilter() throws ExecutionException, InterruptedException {
        // some filters
        Integer routingKey = 3;
        Integer version = 123;
        Integer type = 2;

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder
                .put("type", type)
                .put("version", version)
                .put("routing_key", routingKey);

        ByteBuffer contents = ByteBuffer.wrap("contents".getBytes());
        Future<UUID> publishFuture = queueClient.publish(contents, builder.build());
        UUID startTime = publishFuture.get();

        Future<Map<String, Object>> itemFuture = queueClient.consume(builder.build());

        Map<String, Object> item = itemFuture.get();

        UUID consumedStartTime = (UUID) item.get("start_time");
        ByteBuffer consumedContents = (ByteBuffer) item.get("contents");

        assertEquals(startTime, consumedStartTime);
        assertEquals(contents, consumedContents);
    }

    @Test
    public void shouldPublishAndConsumePreservingStartTimeOrder() throws ExecutionException, InterruptedException {
        Integer routingKey = 3;
        Integer version = 123;
        Integer type = 1;

        ByteBuffer contents1 = ByteBuffer.wrap("contents1".getBytes());
        ByteBuffer contents2 = ByteBuffer.wrap("contents2".getBytes());
        ByteBuffer contents3 = ByteBuffer.wrap("contents3".getBytes());

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder
                .put("type", type)
                .put("version", version)
                .put("routing_key", routingKey);

        Future<UUID> publishFuture1 = queueClient.publish(contents1, builder.build());
        UUID startTime1 = publishFuture1.get();


        Future<UUID> publishFuture2 = queueClient.publish(contents2, builder.build());
        UUID startTime2 = publishFuture2.get();

        Future<UUID> publishFuture3 = queueClient.publish(contents3, builder.build());
        UUID startTime3 = publishFuture3.get();

        Future<Map<String, Object>> itemFuture1 = queueClient.consume(builder.build());
        Map<String, Object> item1 = itemFuture1.get();
        UUID consumedStartTime1 = (UUID) item1.get("start_time");
        assertEquals(startTime1, consumedStartTime1);

        Future<Map<String, Object>> itemFuture2 = queueClient.consume(builder.build());
        Map<String, Object> item2 = itemFuture2.get();
        UUID consumedStartTime2 = (UUID) item2.get("start_time");
        assertEquals(startTime2, consumedStartTime2);

        Future<Map<String, Object>> itemFuture3 = queueClient.consume(builder.build());
        Map<String, Object> item3 = itemFuture3.get();
        UUID consumedStartTime3 = (UUID) item3.get("start_time");
        assertEquals(startTime3, consumedStartTime3);

    }
}
