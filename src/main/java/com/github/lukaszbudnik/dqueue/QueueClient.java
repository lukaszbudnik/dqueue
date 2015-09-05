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
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
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

public class QueueClient implements AutoCloseable {

    private int cassandraPort;
    private String[] cassandraAddress;
    private String cassandraKeyspace;
    private String cassandraTablePrefix;
    private boolean cassandraCreateTables;

    // cassandra
    private Cluster cluster;
    private Session session;

    // zookeeper
    private CuratorFramework zookeeperClient;

    // codahale
    private MetricRegistry metricRegistry;

    private ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("dqueue-thread-%d").build();
    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);

    public QueueClient(int cassandraPort, String[] cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables, CuratorFramework zookeeperClient, MetricRegistry metricRegistry) throws Exception {
        this.cassandraPort = cassandraPort;
        this.cassandraAddress = cassandraAddress;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraTablePrefix = cassandraTablePrefix;
        this.cassandraCreateTables = cassandraCreateTables;
        this.zookeeperClient = zookeeperClient;
        this.metricRegistry = metricRegistry;

        cluster = Cluster.builder().withPort(cassandraPort).addContactPoints(cassandraAddress).build();
        session = cluster.connect();
    }

    Session getSession() {
        return session;
    }

    public Future<UUID> publish(Item item) {
        if (item.getStartTime() == null || item.getStartTime().version() != 1) {
            throw new IllegalArgumentException("Start time must be a valid UUID version 1 identifier");
        }
        if (item.getContents() == null) {
            throw new IllegalArgumentException("Contents must not be null");
        }

        Future<UUID> publishResult = executorService.submit(() -> {
            String filterNames = (item.getFilters() != null ? "." + String.join(".", item.getFilters().keySet()) : "");
            String wholeOperationMetricName = filterNames + ".whole.";

            Optional<Timer.Context> publishTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.publish" + wholeOperationMetricName + "timer").time());

            String tableName = createTableIfNotExists(filterNames, item.getFilters());

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

            session.executeAsync(insert).getUninterruptibly();

            publishTimer.ifPresent(Timer.Context::stop);

            return item.getStartTime();
        });

        return publishResult;
    }

    public Future<Optional<Item>> consume() {
        return consume(Collections.emptyMap());
    }

    public Future<Optional<Item>> consume(Map<String, ?> filters) {

        Future<Optional<Item>> itemFuture = executorService.submit(() -> {

            String filterNames = (filters != null ? "." + String.join(".", filters.keySet()) : "");
            String wholeOperationMetricName = filterNames + ".whole.";

            Optional<Timer.Context> consumeTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + wholeOperationMetricName + "timer").time());

            String tableName = createTableIfNotExists(filterNames, filters);

            Select.Where select = buildSelect(tableName, filters);

            InterProcessMutex interProcessMutex = new InterProcessMutex(zookeeperClient, "/dqueue/" + tableName);

            try {
                String mutexAcquireOperationMetricName = filterNames + ".mutextAcquire.";
                Optional<Timer.Context> mutexAcquireTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + mutexAcquireOperationMetricName + "timer").time());
                interProcessMutex.acquire();
                mutexAcquireTimer.ifPresent(Timer.Context::stop);

                String cassandraSelectOperationMetricName = filterNames + ".cassandraSelect.";
                Optional<Timer.Context> cassandraSelectTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + cassandraSelectOperationMetricName + "timer").time());
                ResultSet resultSet = session.executeAsync(select).getUninterruptibly();
                Row row = resultSet.one();
                cassandraSelectTimer.ifPresent(Timer.Context::stop);

                if (row == null) {
                    return Optional.empty();
                }

                UUID startTime = row.getUUID("start_time");
                ByteBuffer contents = row.getBytes("contents");

                Delete.Where delete = buildDelete(tableName, startTime, filters);

                String cassandraDeleteOperationMetricName = filterNames + ".cassandraDelete.";
                Optional<Timer.Context> cassandraDeleteTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + cassandraDeleteOperationMetricName + "timer").time());
                session.executeAsync(delete).getUninterruptibly();
                cassandraDeleteTimer.ifPresent(Timer.Context::stop);

                return Optional.of(new Item(startTime, contents, filters));
            } catch (Exception e) {
                throw e;
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
                delete.and((QueryBuilder.eq(filter, filters.get(filter))));
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

    private String createTableIfNotExists(String filterNames, Map<String, ?> filters) {
        String tableName = cassandraTablePrefix;
        if (cassandraCreateTables) {
            String columns = "";
            String columnsAndTypes = "";
            if (!filters.isEmpty()) {
                tableName += "_" + String.join("_", filters.keySet());
                columns += String.join(", ", filters.keySet()) + ", ";
                for (String filter : filters.keySet()) {
                    TypeCodec typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(filters.get(filter));
                    DataType.Name cqlType = typeCodec.getCqlType().getName();
                    columnsAndTypes += filter + " " + cqlType.name() + ", ";
                }

            }
            String createTable = "create table if not exists " + cassandraKeyspace + "." + tableName + " ( key varchar, " + columnsAndTypes + " contents blob, start_time timeuuid, primary key (key, " + columns + " start_time) ) ";

            String createTableMetricName = filterNames + ".cassandraCreateTable.";
            Optional<Timer.Context> createTableTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + createTableMetricName + "timer").time());
            session.execute(createTable);
            createTableTimer.ifPresent(Timer.Context::stop);
        }
        return tableName;
    }

}
