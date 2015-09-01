package com.github.lukaszbudnik.dqueue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.github.lukaszbudnik.cloudtag.CloudTagEnsembleProvider;
import com.github.lukaszbudnik.cloudtag.CloudTagPropertiesModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QueueClientIntegrationTest {

    private String cassandraKeyspace = "test" + System.currentTimeMillis();

    private QueueClient queueClient;
    private Session session;
    private MetricRegistry metricRegistry;

    @Before
    public void before() throws Exception {
        Injector injector = Guice.createInjector(new CloudTagPropertiesModule(), new QueueClientBuilderGuicePropertiesModule());

        CloudTagEnsembleProvider cloudTagEnsembleProvider = injector.getInstance(CloudTagEnsembleProvider.class);
        QueueClientBuilder queueClientBuilder = injector.getInstance(QueueClientBuilder.class);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client = CuratorFrameworkFactory.builder().ensembleProvider(cloudTagEnsembleProvider).retryPolicy(retryPolicy).build();
        client.start();
        // warm up zookeeper client
        client.getChildren().forPath("/");

        metricRegistry = new MetricRegistry();

        queueClient = queueClientBuilder
                .withCassandraKeyspace(cassandraKeyspace)
                .withZookeeperClient(client)
                .withMetricRegistry(metricRegistry)
                .build();

        session = queueClient.getSession();
        session.execute("create keyspace " + cassandraKeyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor':3}");
    }

    @After
    public void after() throws Exception {
        session.execute("drop keyspace " + cassandraKeyspace);

        SortedMap<String, Timer> timers = metricRegistry.getTimers();

        timers.keySet().stream().forEach(k -> {
            Timer t = timers.get(k);
            System.out.println(k);
            System.out.println("\ttimes called ==> " + t.getCount());
            System.out.println("\tmedian ==> " + t.getSnapshot().getMedian() / 1000);
            System.out.println("\t75th percentile ==> " + t.getSnapshot().get75thPercentile() / 1000);
            System.out.println("\t99th percentile ==> " + t.getSnapshot().get99thPercentile() / 1000);
        });

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
        UUID startTime = UUIDs.timeBased();
        String name = "name 1";
        ByteBuffer contents = ByteBuffer.wrap(name.getBytes());

        Future<UUID> publishFuture = queueClient.publish(startTime, contents);
        publishFuture.get();

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

        UUID startTime = UUIDs.timeBased();
        ByteBuffer contents = ByteBuffer.wrap("contents".getBytes());
        Future<UUID> publishFuture = queueClient.publish(startTime, contents, builder.build());
        publishFuture.get();

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

        UUID startTime1 = UUIDs.timeBased();
        UUID startTime2 = UUIDs.timeBased();
        UUID startTime3 = UUIDs.timeBased();
        ByteBuffer contents1 = ByteBuffer.wrap("contents1".getBytes());
        ByteBuffer contents2 = ByteBuffer.wrap("contents2".getBytes());
        ByteBuffer contents3 = ByteBuffer.wrap("contents3".getBytes());

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder
                .put("type", type)
                .put("version", version)
                .put("routing_key", routingKey);

        Future<UUID> publishFuture1 = queueClient.publish(startTime1, contents1, builder.build());
        publishFuture1.get();


        Future<UUID> publishFuture2 = queueClient.publish(startTime2, contents2, builder.build());
        publishFuture2.get();

        Future<UUID> publishFuture3 = queueClient.publish(startTime3, contents3, builder.build());
        publishFuture3.get();

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
