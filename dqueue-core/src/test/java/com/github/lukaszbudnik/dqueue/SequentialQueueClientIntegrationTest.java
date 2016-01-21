/**
 * Copyright (C) 2015-2016 Łukasz Budnik <lukasz.budnik@gmail.com>
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
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.github.lukaszbudnik.cloudtag.CloudTagEnsembleProvider;
import com.github.lukaszbudnik.gpe.PropertiesElResolverModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class SequentialQueueClientIntegrationTest {

    private static Injector injector;
    private static CuratorFramework zookeeperClient;

    private static String cassandraKeyspace = "test" + System.currentTimeMillis();
    private SequentialQueueClientImpl queueClient;
    private Session session;
    private MetricRegistry metricRegistry;
    private HealthCheckRegistry healthCheckRegistry;

    @BeforeClass
    public static void beforeClass() throws Exception {
        injector = Guice.createInjector(new PropertiesElResolverModule(Arrays.asList("/dqueue.properties", "/cloudtag.properties")));

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
        healthCheckRegistry = new HealthCheckRegistry();

        queueClient = (SequentialQueueClientImpl) queueClientBuilder
                .withCassandraKeyspace(cassandraKeyspace)
                .withZookeeperClient(zookeeperClient)
                .withMetricRegistry(metricRegistry)
                .withHealthMetricRegistry(healthCheckRegistry)
                .buildSequential();

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

        SortedMap<String, HealthCheck.Result> healthChecks = healthCheckRegistry.runHealthChecks();
        healthChecks.keySet().stream().forEach(k -> {
            HealthCheck.Result healthCheck = healthChecks.get(k);
            System.out.println(k + " healthy? ==> " + healthCheck.isHealthy());
        });

        queueClient.close();
    }

    @AfterClass
    public static void afterClass() {
        IOUtils.closeQuietly(zookeeperClient);
    }

    @Test
    public void shouldPublishSequential() throws ExecutionException, InterruptedException {
        Future<ImmutableList<UUID>> futures = queueClient.publishSequential(
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("A".getBytes())),
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("B".getBytes())),
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("C".getBytes()))
        );
        futures.get();

        Future<Optional<SequentialItem>> optionalFutureA = queueClient.consumeSequential();
        Optional<SequentialItem> itemOptionalA = optionalFutureA.get();
        SequentialItem itemA = itemOptionalA.get();

        assertNotNull(itemA);
        assertEquals("A", new String(itemA.getContents().array()));

        Future<Optional<SequentialItem>> optionalFutureB = queueClient.consumeSequential();
        Optional<SequentialItem> itemOptionalB = optionalFutureB.get();

        assertFalse(itemOptionalB.isPresent());

        queueClient.delete(itemA);

        optionalFutureB = queueClient.consumeSequential();
        itemOptionalB = optionalFutureB.get();
        SequentialItem itemB = itemOptionalB.get();

        assertNotNull(itemB);
        assertEquals("B", new String(itemB.getContents().array()));

        Future<Optional<SequentialItem>> optionalFutureC = queueClient.consumeSequential();
        Optional<SequentialItem> itemOptionalC = optionalFutureC.get();

        assertFalse(itemOptionalC.isPresent());

        queueClient.delete(itemB);

        optionalFutureC = queueClient.consumeSequential();
        itemOptionalC = optionalFutureC.get();
        SequentialItem itemC = itemOptionalC.get();

        assertNotNull(itemC);
        assertEquals("C", new String(itemC.getContents().array()));

        queueClient.delete(itemC);

        Future<Optional<SequentialItem>> empty = queueClient.consumeSequential();
        assertFalse(empty.get().isPresent());
    }

    @Test
    public void shouldPublishSequentialWithFilters() throws ExecutionException, InterruptedException {

        Map<String, String> filters = ImmutableMap.of("filter1", "1", "filter2", "two");

        Future<ImmutableList<UUID>> futures = queueClient.publishSequential(
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("A".getBytes()), filters),
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("B".getBytes()), filters),
                new Item(UUIDs.timeBased(), ByteBuffer.wrap("C".getBytes()), filters)
        );
        futures.get();

        Future<Optional<SequentialItem>> optionalFutureA = queueClient.consumeSequential(filters);
        Optional<SequentialItem> itemOptionalA = optionalFutureA.get();
        SequentialItem itemA = itemOptionalA.get();

        assertNotNull(itemA);
        assertEquals("A", new String(itemA.getContents().array()));

        Future<Optional<SequentialItem>> optionalFutureB = queueClient.consumeSequential(filters);
        Optional<SequentialItem> itemOptionalB = optionalFutureB.get();

        assertFalse(itemOptionalB.isPresent());

        queueClient.delete(itemA);

        optionalFutureB = queueClient.consumeSequential(filters);
        itemOptionalB = optionalFutureB.get();
        SequentialItem itemB = itemOptionalB.get();

        assertNotNull(itemB);
        assertEquals("B", new String(itemB.getContents().array()));

        Future<Optional<SequentialItem>> optionalFutureC = queueClient.consumeSequential(filters);
        Optional<SequentialItem> itemOptionalC = optionalFutureC.get();

        assertFalse(itemOptionalC.isPresent());

        queueClient.delete(itemB);

        optionalFutureC = queueClient.consumeSequential(filters);
        itemOptionalC = optionalFutureC.get();
        SequentialItem itemC = itemOptionalC.get();

        assertNotNull(itemC);
        assertEquals("C", new String(itemC.getContents().array()));

        queueClient.delete(itemC);

        Future<Optional<SequentialItem>> empty = queueClient.consumeSequential(filters);
        assertFalse(empty.get().isPresent());
    }

}
