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
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class QueueClientImpl implements QueueClient {

    public static final String NO_FILTERS = "no_filters";
    private final int cassandraPort;
    private final String[] cassandraAddress;
    private final String cassandraKeyspace;
    private final String cassandraTablePrefix;
    private final boolean cassandraCreateTables;
    private final Cache<String, Boolean> cacheCreatedTables;

    // cassandra
    private final Cluster cluster;
    private final Session session;

    // zookeeper
    private final CuratorFramework zookeeperClient;

    // codahale
    private final MetricRegistry metricRegistry;
    private final HealthCheckRegistry healthCheckRegistry;

    private final ThreadFactory threadFactory;
    private final ExecutorService executorService;

    QueueClientImpl(int cassandraPort, String[] cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables, CuratorFramework zookeeperClient, int threadPoolSize, MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry) throws Exception {
        this.cassandraPort = cassandraPort;
        this.cassandraAddress = cassandraAddress;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraTablePrefix = cassandraTablePrefix;
        this.cassandraCreateTables = cassandraCreateTables;
        this.zookeeperClient = zookeeperClient;
        this.metricRegistry = metricRegistry;
        this.healthCheckRegistry = healthCheckRegistry;

        Optional.ofNullable(healthCheckRegistry).ifPresent(hcr -> {
            hcr.register("zookeeper", new HealthCheck() {
                @Override
                protected Result check() throws Exception {
                    if (zookeeperClient.getZookeeperClient().isConnected()) {
                        return Result.healthy();
                    } else {
                        return Result.unhealthy("Zookeeper client not connected");
                    }
                }
            });
            hcr.register("cassandra", new HealthCheck() {
                @Override
                protected Result check() throws Exception {
                    int counter = 0;
                    Session.State state = session.getState();
                    for (Host host : state.getConnectedHosts()) {
                        counter += state.getOpenConnections(host);
                    }
                    if (counter > 0) {
                        return Result.healthy();
                    } else {
                        return Result.unhealthy("Cassandra client not connected");
                    }
                }
            });
        });

        cluster = Cluster.builder().withPort(cassandraPort).addContactPoints(cassandraAddress).build();
        session = cluster.connect();

        cacheCreatedTables = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build();

        threadFactory = new ThreadFactoryBuilder().setNameFormat("dqueue-thread-%d").build();

        executorService = Executors.newFixedThreadPool(threadPoolSize, threadFactory);
    }

    Session getSession() {
        return session;
    }

    private <R> R executeAndMeasureTime(NoArgFunction<R> function, String metricName) {
        Optional<Timer.Context> timer = Optional.ofNullable(metricRegistry).map(m -> m.timer(metricName).time());
        try {
            return function.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            timer.ifPresent(Timer.Context::stop);
        }
    }

    private void executeAndMeasureTime(NoArgVoidFunction function, String metricName) {
        Optional<Timer.Context> timer = Optional.ofNullable(metricRegistry).map(m -> m.timer(metricName).time());
        try {
            function.apply();
        } catch (Exception e) {
            throw new RuntimeException();
        } finally {
            timer.ifPresent(Timer.Context::stop);
        }
    }

    @Override
    public Future<UUID> publish(Item item) {
        if (item.getStartTime() == null || item.getStartTime().version() != 1) {
            throw new IllegalArgumentException("Start time must be a valid UUID version 1 identifier");
        }
        if (item.getContents() == null) {
            throw new IllegalArgumentException("Contents must not be null");
        }
        if (item.getFilters() == null) {
            throw new IllegalArgumentException("Filters cannot be null, if no filters are to be used pass an empty map");
        }

        Future<UUID> publishResult = executorService.submit(() -> {
            String filterNames;
            if (item.getFilters().isEmpty()) {
                filterNames = NO_FILTERS;
            } else {
                filterNames = String.join("_", item.getFilters().keySet());
            }
            String wholeOperationMetricName = "dqueue.publish." + filterNames + ".whole.timer";
            Optional<Timer.Context> publishTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer(wholeOperationMetricName).time());

            try {
                String tableName = createTableIfNotExists("publish", filterNames, item.getFilters());

                QueryBuilder queryBuilder = new QueryBuilder(cluster);
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
                String primaryKey = df.format(new Date(UUIDs.unixTimestamp(item.getStartTime())));
                final Insert insert = queryBuilder.insertInto(cassandraKeyspace, tableName)
                        .value("key", primaryKey)
                        .value("start_time", item.getStartTime())
                        .value("contents", item.getContents());

                if (item.getFilters() != null && !item.getFilters().isEmpty()) {
                    for (String key : item.getFilters().keySet()) {
                        insert.value(key, item.getFilters().get(key));
                    }
                }

                String cassandraInsertMetricName = "dqueue.publish." + filterNames + ".cassandraInsert.timer";
                executeAndMeasureTime(() -> session.executeAsync(insert).getUninterruptibly(), cassandraInsertMetricName);

                return item.getStartTime();
            } finally {
                publishTimer.ifPresent(Timer.Context::stop);
            }
        });

        return publishResult;
    }

    @Override
    public Future<Optional<Item>> consume() {
        return consume(Collections.emptyMap());
    }

    @Override
    public Future<Optional<Item>> consume(Map<String, ?> filters) {
        if (filters == null) {
            throw new IllegalArgumentException("Filters cannot be null, if no filters are to be used pass an empty map");
        }

        Future<Optional<Item>> itemFuture = executorService.submit(() -> {

            String filterNames;
            if (filters.isEmpty()) {
                filterNames = NO_FILTERS;
            } else {
                filterNames = String.join("_", filters.keySet());
            }

            String wholeOperationMetricName = "dqueue.consume." + filterNames + ".whole.timer";

            Optional<Timer.Context> consumeTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer(wholeOperationMetricName).time());

            InterProcessMutex interProcessMutex = new InterProcessMutex(zookeeperClient, "/dqueue/" + filterNames);

            try {
                String mutexAcquireOperationMetricName = "dqueue.consume." + filterNames + ".mutextAcquire.timer";
                executeAndMeasureTime(() -> interProcessMutex.acquire(), mutexAcquireOperationMetricName);

                String tableName = createTableIfNotExists("consume", filterNames, filters);

                Select.Where select = buildSelect(tableName, filters);
                String cassandraSelectOperationMetricName = "dqueue.consume." + filterNames + ".cassandraSelect.timer";
                ResultSet resultSet = executeAndMeasureTime(() -> session.executeAsync(select).getUninterruptibly(), cassandraSelectOperationMetricName);
                Row row = resultSet.one();

                if (row == null) {
                    return Optional.empty();
                }

                UUID startTime = row.getUUID("start_time");
                ByteBuffer contents = row.getBytes("contents");

                Delete.Where delete = buildDelete(tableName, startTime, filters);

                String cassandraDeleteOperationMetricName = "dqueue.consume." + filterNames + ".cassandraDelete.timer";
                executeAndMeasureTime(() -> session.executeAsync(delete).getUninterruptibly(), cassandraDeleteOperationMetricName);

                return Optional.of(new Item(startTime, contents, filters));
            } finally {
                interProcessMutex.release();
                consumeTimer.ifPresent(Timer.Context::stop);
            }
        });

        return itemFuture;
    }

    private Delete.Where buildDelete(String tableName, UUID startTime, Map<String, ?> filters) {
        long timestamp = UUIDs.unixTimestamp(startTime);
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String key = df.format(new Date(timestamp));

        QueryBuilder queryBuilder = new QueryBuilder(session.getCluster());
        Delete.Where delete = queryBuilder.delete().from(cassandraKeyspace, tableName)
                .where(QueryBuilder.eq("key", key))
                .and(QueryBuilder.eq("start_time", startTime));

        if (filters != null && !filters.isEmpty()) {
            for (String filter : filters.keySet()) {
                delete.and(QueryBuilder.eq(filter, filters.get(filter)));
            }
        }

        return delete;
    }

    private Select.Where buildSelect(String tableName, Map<String, ?> filters) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String key = df.format(new Date());

        QueryBuilder queryBuilder = new QueryBuilder(session.getCluster());
        Select.Where select = queryBuilder.select().column("start_time").column("contents")
                .from(cassandraKeyspace, tableName)
                .where(QueryBuilder.eq("key", key));

        if (filters != null && filters.size() > 0) {
            for (String filter : filters.keySet()) {
                select.and(QueryBuilder.eq(filter, filters.get(filter)));
            }
        }

        select.and(QueryBuilder.lt("start_time", UUIDs.endOf(System.currentTimeMillis())));

        select.limit(1);

        return select;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
        IOUtils.closeQuietly(session);
        IOUtils.closeQuietly(cluster);
    }

    private String createTableIfNotExists(String operation, String filterNames, Map<String, ?> filters) {
        String tableName = cassandraTablePrefix + "_" + filterNames;
        if (cassandraCreateTables && cacheCreatedTables.getIfPresent(filterNames) == null) {
            String columns = "";
            String columnsAndTypes = "";
            if (!filters.isEmpty()) {
                columns += String.join(", ", filters.keySet()) + ", ";
                for (String filter : filters.keySet()) {
                    TypeCodec typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(filters.get(filter));
                    DataType.Name cqlType = typeCodec.getCqlType().getName();
                    columnsAndTypes += filter + " " + cqlType.name() + ", ";
                }

            }
            String createTable = "create table if not exists " + cassandraKeyspace + "." + tableName + " ( key varchar, " + columnsAndTypes + " contents blob, start_time timeuuid, primary key (key, " + columns + " start_time) ) ";

            String createTableMetricName = "dqueue." + operation + "." + filterNames + ".cassandraCreateTable.timer";
            executeAndMeasureTime(() -> session.execute(createTable), createTableMetricName);
            cacheCreatedTables.put(filterNames, Boolean.TRUE);
        }
        return tableName;
    }

}
