package com.github.lukaszbudnik.dqueue;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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

    public Future<UUID> publish(UUID startTime, ByteBuffer contents) {
        if (startTime == null || startTime.version() != 1) {
            throw new IllegalArgumentException("Start time id must be a valid UUID version 1 identifier");
        }
        return publish(startTime, contents, null);
    }

    public Future<UUID> publish(UUID startTime, ByteBuffer contents, Map<String, ?> filters) {
        Future<UUID> publishResult = executorService.submit(() -> {
            String filterNames = (filters != null ? "." + String.join(".", filters.keySet()) : "");
            String wholeOperationMetricName = filterNames + ".whole.";

            Optional<Timer.Context> publishTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.publish" + wholeOperationMetricName + "timer").time());

            String tableName = makeSureTableExists(filters);

            QueryBuilder queryBuilder = new QueryBuilder(cluster);
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String primaryKey = df.format(new Date(UUIDs.unixTimestamp(startTime)));
            final Insert insert = queryBuilder.insertInto(cassandraKeyspace, tableName)
                    .value("key", primaryKey)
                    .value("start_time", startTime)
                    .value("contents", contents);

            if (filters != null && !filters.isEmpty()) {
                for (String key : filters.keySet()) {
                    insert.value(key, filters.get(key));
                }
            }

            session.executeAsync(insert).getUninterruptibly();

            publishTimer.ifPresent(Timer.Context::stop);

            return startTime;
        });

        return publishResult;
    }

    public Future<Map<String, Object>> consume() {
        return consume(null);
    }

    public Future<Map<String, Object>> consume(Map<String, ?> filters) {

        Future<Map<String, Object>> itemFuture = executorService.submit(() -> {

            String filterNames = (filters != null ? "." + String.join(".", filters.keySet()) : "");
            String wholeOperationMetricName = filterNames + ".whole.";

            Optional<Timer.Context> consumeTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + wholeOperationMetricName + "timer").time());

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            String key = df.format(new Date());
            String tableName = makeSureTableExists(filters);
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

            InterProcessMutex interProcessMutex = new InterProcessMutex(zookeeperClient, "/dqueue/" + tableName);

            try {
                String mutexAcquireOperationMetricName = filterNames + ".mutextAcquire.";
                Optional<Timer.Context> mutexAcquireTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + mutexAcquireOperationMetricName + "timer").time());
                interProcessMutex.acquire();
                mutexAcquireTimer.ifPresent(Timer.Context::stop);

                ImmutableMap.Builder<String, Object> itemBuilder = ImmutableMap.builder();

                String cassandraSelectOperationMetricName = filterNames + ".cassandraSelect.";
                Optional<Timer.Context> cassandraSelectTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + cassandraSelectOperationMetricName + "timer").time());
                ResultSet resultSet = session.executeAsync(select).getUninterruptibly();
                Row row = resultSet.one();
                cassandraSelectTimer.ifPresent(Timer.Context::stop);

                if (row == null) {
                    Optional.empty();
                    return null;
                }

                for (ColumnDefinitions.Definition def : resultSet.getColumnDefinitions().asList()) {
                    Object value = row.get(def.getName(), CodecRegistry.DEFAULT_INSTANCE.codecFor(def.getType()));
                    itemBuilder.put(def.getName(), value);
                }

                Map<String, Object> map = itemBuilder.build();

                Delete.Where delete = queryBuilder.delete().from(cassandraKeyspace, tableName)
                        .where(QueryBuilder.eq("key", key))
                        .and(QueryBuilder.eq("start_time", map.get("start_time")));

                if (filters != null && !filters.isEmpty()) {
                    for (String filter : filters.keySet()) {
                        delete.and((QueryBuilder.eq(filter, filters.get(filter))));
                    }
                }

                String cassandraDeleteOperationMetricName = filterNames + ".cassandraDelete.";
                Optional<Timer.Context> cassandraDeleteTimer = Optional.ofNullable(metricRegistry).map(m -> m.timer("dqueue.consume" + cassandraDeleteOperationMetricName + "timer").time());
                session.executeAsync(delete).getUninterruptibly();
                cassandraDeleteTimer.ifPresent(Timer.Context::stop);

                return map;
            } catch (Exception e) {
                throw e;
            } finally {
                interProcessMutex.release();
                consumeTimer.ifPresent(Timer.Context::stop);
            }
        });

        return itemFuture;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
        IOUtils.closeQuietly(zookeeperClient);
        IOUtils.closeQuietly(session);
        IOUtils.closeQuietly(cluster);
    }

    private String makeSureTableExists(Map<String, ?> filters) {
        String tableName = cassandraTablePrefix;
        if (cassandraCreateTables) {
            tableName += "_";
            String columns = "";
            String columnsAndTypes = "";
            if (filters != null) {
                tableName += String.join("_", filters.keySet());
                columns += String.join(", ", filters.keySet()) + ", ";
                for (String filter : filters.keySet()) {
                    TypeCodec typeCodec = CodecRegistry.DEFAULT_INSTANCE.codecFor(filters.get(filter));
                    DataType.Name cqlType = typeCodec.getCqlType().getName();
                    columnsAndTypes += filter + " " + cqlType.name() + ", ";
                }

            }
            String createTable = "create table if not exists " + cassandraKeyspace + "." + tableName + " ( key varchar, " + columnsAndTypes + " contents blob, start_time timeuuid, primary key (key, " + columns + " start_time) ) ";

            session.execute(createTable);
        }
        return tableName;
    }

}
