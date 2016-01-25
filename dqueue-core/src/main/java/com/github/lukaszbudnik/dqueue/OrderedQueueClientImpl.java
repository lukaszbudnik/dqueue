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
import com.codahale.metrics.health.HealthCheckRegistry;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;

public class OrderedQueueClientImpl extends QueueClientImpl implements OrderedQueueClient {

    private static final int QUEUED = 0;
    private static final int FETCHED = 1;

    OrderedQueueClientImpl(int cassandraPort, String[] cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables, CuratorFramework zookeeperClient, int threadPoolSize, MetricRegistry metricRegistry, HealthCheckRegistry healthCheckRegistry) throws Exception {
        super(cassandraPort, cassandraAddress, cassandraKeyspace, cassandraTablePrefix, cassandraCreateTables, zookeeperClient, threadPoolSize, metricRegistry, healthCheckRegistry);
    }

    @Override
    public Future<ImmutableList<UUID>> publishOrdered(List<Item> items) {

        Future<ImmutableList<UUID>> publishResult = executorService.submit(() -> {
            UUID dependency = zeroUUID;
            ImmutableList.Builder<UUID> uuids = ImmutableList.builder();

            for (Item item: items) {
                dependency = publishOrdered(dependency, item);
                uuids.add(dependency);
            }

            return uuids.build();
        });

        return publishResult;
    }

    @Override
    public Future<UUID> publishOrdered(OrderedItem orderedItem) {
        return executorService.submit(() -> publishOrdered(orderedItem.getDependency(), orderedItem));
    }

    private UUID publishOrdered(UUID dependency, Item item) {
        String filterNames;
        if (item.getFilters().isEmpty()) {
            filterNames = NO_FILTERS;
        } else {
            filterNames = String.join("_", item.getFilters().keySet());
        }
        String wholeOperationMetricName = "dqueue.publish." + filterNames + ".whole.timer";
        Optional<Timer.Context> publishTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer(wholeOperationMetricName).time());

        try {
            String tableName = createSequentialTableIfNotExists("publish", filterNames, item.getFilters());

            Insert insert = buildInsert(dependency, item, QUEUED, tableName + "_queued");

            String cassandraInsertMetricName = "dqueue.publish." + filterNames + ".cassandraInsertQueued.timer";
            executeAndMeasureTime(() -> session.executeAsync(insert).getUninterruptibly(), cassandraInsertMetricName);

            dependency = item.getStartTime();

        } finally {
            publishTimer.ifPresent(Timer.Context::stop);
        }
        return dependency;
    }

    @Override
    public void deleteOrdered(UUID startTime, Map<String, ?> filters) {
        String filterNames;
        if (filters.isEmpty()) {
            filterNames = NO_FILTERS;
        } else {
            filterNames = String.join("_", filters.keySet());
        }
        String tableName = createSequentialTableIfNotExists("delete", filterNames, filters);
        Delete.Where delete = buildSequentialDelete(tableName + "_processing", startTime, filters);
        String cassandraDeleteOperationMetricName = "dqueue.consume." + filterNames + ".cassandraDelete.timer";
        executeAndMeasureTime(() -> session.executeAsync(delete).getUninterruptibly(), cassandraDeleteOperationMetricName);
    }

    private Insert buildInsert(UUID dependency, Item item, int status, String tableName) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String primaryKey = df.format(new Date(UUIDs.unixTimestamp(item.getStartTime())));

        QueryBuilder queryBuilder = new QueryBuilder(cluster);

        Insert insert = queryBuilder.insertInto(cassandraKeyspace, tableName)
                .value("key", primaryKey)
                .value("start_time", item.getStartTime())
                .value("contents", item.getContents());

        if (status == QUEUED) {
            insert.value("dependency", dependency);
        }

        if (item.getFilters() != null && !item.getFilters().isEmpty()) {
            for (String key : item.getFilters().keySet()) {
                insert.value(key, item.getFilters().get(key));
            }
        }

        return insert;
    }

    @Override
    public Future<Optional<OrderedItem>> consumeOrdered(Map<String, ?> filters) {
        Future<Optional<OrderedItem>> itemFuture = executorService.submit(() -> {

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

                String tableName = createSequentialTableIfNotExists("consume", filterNames, filters);

                Select.Where select = buildSequentialSelect(tableName + "_queued", filters);
                String cassandraSelectOperationMetricName = "dqueue.consume." + filterNames + ".cassandraSelect.timer";
                ResultSet resultSet = executeAndMeasureTime(() -> session.executeAsync(select).getUninterruptibly(), cassandraSelectOperationMetricName);
                List<Row> rows = resultSet.all();

                for (int i = 0; i < rows.size(); i++) {
                    Row row = rows.get(i);

                    UUID dependency = row.getUUID("dependency");

                    if (!dependency.equals(zeroUUID)) {
                        // check if dependency exists in processing
                        Select dependencySelectInProcessing = buildDependencySelectInProcessing(tableName, dependency, filters);
                        String cassandraDependencySelectProcessingOperationMetricName = "dqueue.consume." + filterNames + ".cassandraDependencySelectProcessing.timer";
                        ResultSet dependencyResultSet = executeAndMeasureTime(() -> session.executeAsync(dependencySelectInProcessing).getUninterruptibly(), cassandraDependencySelectProcessingOperationMetricName);
                        if (dependencyResultSet.one() != null) {
                            break;
                        }
                        // check if dependency exists in queued
                        Select dependencySelectInQueued = buildDependencySelectInQueued(tableName, dependency, filters);
                        String cassandraDependencySelectQueuedOperationMetricName = "dqueue.consume." + filterNames + ".cassandraDependencySelectQueued.timer";
                        dependencyResultSet = executeAndMeasureTime(() -> session.executeAsync(dependencySelectInQueued).getUninterruptibly(), cassandraDependencySelectQueuedOperationMetricName);
                        if (dependencyResultSet.one() != null) {
                            break;
                        }
                    }

                    UUID startTime = row.getUUID("start_time");
                    ByteBuffer contents = row.getBytes("contents");

                    Delete.Where delete = buildSequentialDelete(tableName + "_queued", startTime, filters);
                    String cassandraDeleteOperationMetricName = "dqueue.consume." + filterNames + ".cassandraDeleteQueued.timer";
                    executeAndMeasureTime(() -> session.executeAsync(delete).getUninterruptibly(), cassandraDeleteOperationMetricName);

                    Insert insertFetched = buildInsert(dependency, new Item(startTime, contents, filters), FETCHED, tableName + "_processing");

                    String cassandraInsertMetricName = "dqueue.publish." + filterNames + ".cassandraInsertFetched.timer";
                    executeAndMeasureTime(() -> session.executeAsync(insertFetched).getUninterruptibly(), cassandraInsertMetricName);

                    return Optional.of(new OrderedItem(startTime, dependency, contents, filters));
                }

                return Optional.empty();

            } finally {
                interProcessMutex.release();
                consumeTimer.ifPresent(Timer.Context::stop);
            }
        });

        return itemFuture;
    }

    private Delete.Where buildSequentialDelete(String tableName, UUID startTime, Map<String, ?> filters) {
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

    private Select.Where buildSequentialSelectWithoutStatus(String tableName, Map<String, ?> filters) {
        long nowTimestamp = System.currentTimeMillis();
        long yesterdayTimestamp = nowTimestamp - 24 * 60 * 60 * 1_000;
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String nowKey = df.format(new Date(nowTimestamp));
        String yesterdayKey = df.format(new Date(yesterdayTimestamp));

        QueryBuilder queryBuilder = new QueryBuilder(session.getCluster());
        Select.Where select = queryBuilder.select().all()
                .from(cassandraKeyspace, tableName)
                .where(QueryBuilder.in("key", nowKey, yesterdayKey));

        if (filters != null && filters.size() > 0) {
            for (String filter : filters.keySet()) {
                select.and(QueryBuilder.eq(filter, filters.get(filter)));
            }
        }

        return select;
    }

    private Select.Where buildSequentialSelect(String tableName, Map<String, ?> filters) {
        Select.Where select = buildSequentialSelectWithoutStatus(tableName, filters);
        select.and(QueryBuilder.lt("start_time", UUIDs.endOf(System.currentTimeMillis())));

        return select;
    }

    private Select buildDependencySelectInProcessing(String tableName, UUID dependency, Map<String, ?> filters) {
        Select.Where select = buildSequentialSelectWithoutStatus(tableName + "_processing", filters);

        select.and(QueryBuilder.eq("start_time", dependency));

        return select.limit(1);
    }

    private Select buildDependencySelectInQueued(String tableName, UUID dependency, Map<String, ?> filters) {
        Select.Where select = buildSequentialSelectWithoutStatus(tableName + "_queued", filters);

        select.and(QueryBuilder.eq("start_time", dependency));

        return select.limit(1);
    }

    private String createSequentialTableIfNotExists(String operation, String filterNames, Map<String, ?> filters) {
        String tableName = cassandraTablePrefix + "_" + filterNames;
        if (cassandraCreateTables && cacheCreatedTables.getIfPresent(filterNames) == null) {
            String columns = ", ";
            String columnsAndTypes = "";
            if (!filters.isEmpty()) {
                columns += String.join(", ", filters.keySet()) + ", ";
                for (String filter : filters.keySet()) {
                    TypeCodec typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(filters.get(filter));
                    DataType.Name cqlType = typeCodec.getCqlType().getName();
                    columnsAndTypes += filter + " " + cqlType.name() + ", ";
                }

            }
            String createTableQueued = "create table if not exists " + cassandraKeyspace + "." + tableName + "_queued ( key varchar, " + columnsAndTypes + " contents blob, start_time timeuuid, dependency timeuuid, primary key (key " + columns + " start_time, dependency) ) ";
            String createTableProcessing = "create table if not exists " + cassandraKeyspace + "." + tableName + "_processing ( key varchar, " + columnsAndTypes + " contents blob, start_time timeuuid, primary key (key " + columns + " start_time) ) ";

            String createTableMetricName = "dqueue." + operation + "." + filterNames + ".cassandraCreateTable.timer";
            executeAndMeasureTime(() -> {
                session.execute(createTableQueued).wasApplied();
                session.execute(createTableProcessing).wasApplied();
            }, createTableMetricName);
            cacheCreatedTables.put(filterNames, Boolean.TRUE);
        }
        return tableName;
    }

}
