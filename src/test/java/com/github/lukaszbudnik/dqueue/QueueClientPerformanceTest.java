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
import com.readytalk.metrics.StatsDReporter;
import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueueClientPerformanceTest {

    private static final int NUMBER_OF_ITERATIONS = 10_000;
    private static Injector injector = Guice.createInjector(new CloudTagPropertiesModule(), new QueueClientBuilderGuicePropertiesModule());
    private static CuratorFramework zookeeperClient;

    private String cassandraKeyspace = "test" + System.currentTimeMillis();
    private QueueClient queueClient;
    private Session session;
    private MetricRegistry metricRegistry;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // don't run on travis!
        Assume.assumeFalse(System.getenv().containsKey("TRAVIS"));

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

        StatsDReporter.forRegistry(metricRegistry)
                .build("192.168.99.100", 8125)
                .start(1, TimeUnit.SECONDS);
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
    public void doItNoFilters() throws ExecutionException, InterruptedException {

        byte[] data = new byte[2045];
        Random r = new Random();
        r.nextBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        IntStream.range(0, NUMBER_OF_ITERATIONS).forEach((i) -> {
            UUID startTime = UUIDs.timeBased();
            Future<UUID> id = queueClient.publish(new Item(startTime, buffer));
            try {
                id.get();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });

        IntStream.range(0, NUMBER_OF_ITERATIONS).forEach((i) -> {
            Future<Optional<Item>> itemFuture = queueClient.consume();

            Optional<Item> item = null;
            try {
                item = itemFuture.get();
            } catch (Exception e) {
                fail(e.getMessage());
            }

            assertTrue(item.isPresent());
        });

    }

    @Test
    public void doIt5Filters() throws ExecutionException, InterruptedException {

        byte[] data = new byte[2045];
        Random r = new Random();
        r.nextBytes(data);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        Map<String, String> filters = ImmutableMap.of(
                // f1
                "f1",
                Long.toHexString(r.nextLong()),
                // f2
                "f2",
                Long.toHexString(r.nextLong()),
                // f3
                "f3",
                Long.toHexString(r.nextLong()),
                // f4
                "f4",
                Long.toHexString(r.nextLong()),
                // f5
                "f5",
                Long.toHexString(r.nextLong())
        );

        IntStream.range(0, NUMBER_OF_ITERATIONS).forEach((i) -> {
            UUID startTime = UUIDs.timeBased();
            Future<UUID> id = queueClient.publish(new Item(startTime, buffer, filters));
            try {
                id.get();
            } catch (Exception e) {
                fail(e.getMessage());
            }
        });

        IntStream.range(0, NUMBER_OF_ITERATIONS).forEach((i) -> {
            Future<Optional<Item>> itemFuture = queueClient.consume();

            Optional<Item> item = null;
            try {
                item = itemFuture.get();
            } catch (Exception e) {
                fail(e.getMessage());
            }

            assertTrue(item.isPresent());
        });

    }

}
