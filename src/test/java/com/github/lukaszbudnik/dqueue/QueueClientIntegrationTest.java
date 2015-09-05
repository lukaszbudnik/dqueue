/**
 * Copyright (C) 2015 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
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
import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class QueueClientIntegrationTest {

    private static Injector injector = Guice.createInjector(new CloudTagPropertiesModule(), new QueueClientBuilderGuicePropertiesModule());
    private static CuratorFramework zookeeperClient;

    private String cassandraKeyspace = "test" + System.currentTimeMillis();
    private QueueClient queueClient;
    private Session session;
    private MetricRegistry metricRegistry;

    @BeforeClass
    public static void beforeClass() throws Exception {
        CloudTagEnsembleProvider cloudTagEnsembleProvider = injector.getInstance(CloudTagEnsembleProvider.class);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeperClient = CuratorFrameworkFactory.builder().ensembleProvider(cloudTagEnsembleProvider).retryPolicy(retryPolicy).build();
        zookeeperClient.start();
        // warm up zookeeper zookeeperClient - on my macbook pro takes even up to 10 seconds
        zookeeperClient.getChildren().forPath("/");
    }

    @Before
    public void before() throws Exception {
        QueueClientBuilder queueClientBuilder = injector.getInstance(QueueClientBuilder.class);

        metricRegistry = new MetricRegistry();

        queueClient = queueClientBuilder
                .withCassandraKeyspace(cassandraKeyspace)
                .withZookeeperClient(zookeeperClient)
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
            System.out.println("\tmedian ==> " + t.getSnapshot().getMedian() / 1000 / 1000);
            System.out.println("\t75th percentile ==> " + t.getSnapshot().get75thPercentile() / 1000 / 1000);
            System.out.println("\t99th percentile ==> " + t.getSnapshot().get99thPercentile() / 1000 / 1000);
        });

        queueClient.close();
    }

    @AfterClass
    public static void afterClass() {
        IOUtils.closeQuietly(zookeeperClient);
    }

    @Test
    public void shouldReturnNothingIfQueueIsEmpty() throws ExecutionException, InterruptedException {
        Future<Optional<Item>> itemFuture = queueClient.consume(ImmutableMap.of("type", 123));

        Optional<Item> item = itemFuture.get();

        assertFalse(item.isPresent());
    }

    @Test
    public void shouldPublishAndConsumeWithoutFilters() throws ExecutionException, InterruptedException {
        UUID startTime = UUIDs.timeBased();
        String name = "name 1";
        ByteBuffer contents = ByteBuffer.wrap(name.getBytes());

        Future<UUID> publishFuture = queueClient.publish(new Item(startTime, contents));
        publishFuture.get();

        Future<Optional<Item>> itemFuture = queueClient.consume();

        Optional<Item> item = itemFuture.get();

        UUID consumedStartTime = item.get().getStartTime();
        ByteBuffer consumedContents = item.get().getContents();

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
        Future<UUID> publishFuture = queueClient.publish(new Item(startTime, contents, builder.build()));
        publishFuture.get();

        Future<Optional<Item>> itemFuture = queueClient.consume(builder.build());

        Optional<Item> item = itemFuture.get();

        UUID consumedStartTime = item.get().getStartTime();
        ByteBuffer consumedContents = item.get().getContents();

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

        Future<UUID> publishFuture1 = queueClient.publish(new Item(startTime1, contents1, builder.build()));
        publishFuture1.get();


        Future<UUID> publishFuture2 = queueClient.publish(new Item(startTime2, contents2, builder.build()));
        publishFuture2.get();

        Future<UUID> publishFuture3 = queueClient.publish(new Item(startTime3, contents3, builder.build()));
        publishFuture3.get();

        Future<Optional<Item>> itemFuture1 = queueClient.consume(builder.build());
        Optional<Item> item1 = itemFuture1.get();
        UUID consumedStartTime1 = item1.get().getStartTime();
        assertEquals(startTime1, consumedStartTime1);

        Future<Optional<Item>> itemFuture2 = queueClient.consume(builder.build());
        Optional<Item> item2 = itemFuture2.get();
        UUID consumedStartTime2 = item2.get().getStartTime();
        assertEquals(startTime2, consumedStartTime2);

        Future<Optional<Item>> itemFuture3 = queueClient.consume(builder.build());
        Optional<Item> item3 = itemFuture3.get();
        UUID consumedStartTime3 = item3.get().getStartTime();
        assertEquals(startTime3, consumedStartTime3);
    }
}
