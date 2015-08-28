package com.github.lukaszbudnik.dqueue;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;
import com.github.lukaszbudnik.cloudtag.CloudTagEnsembleProvider;
import com.github.lukaszbudnik.cloudtag.CloudTagPropertiesModule;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.IOUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

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
    private CuratorFramework client;

    private ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("dqueue-thread-%d").build();
    private ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), threadFactory);

    public QueueClient(int cassandraPort, String[] cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables) throws Exception {
        this.cassandraPort = cassandraPort;
        this.cassandraAddress = cassandraAddress;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraTablePrefix = cassandraTablePrefix;
        this.cassandraCreateTables = cassandraCreateTables;

        cluster = Cluster.builder().withPort(cassandraPort).addContactPoints(cassandraAddress).build();
        session = cluster.connect();

        Injector injector = Guice.createInjector(new CloudTagPropertiesModule());
        CloudTagEnsembleProvider cloudTagEnsembleProvider = injector.getInstance(CloudTagEnsembleProvider.class);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.builder().ensembleProvider(cloudTagEnsembleProvider).retryPolicy(retryPolicy).build();
        client.start();
        // warm up zookeeper client
        client.getChildren().forPath("/");
    }

    Session getSession() {
        return session;
    }

    public Future<UUID> publish(ByteBuffer contents) {
        return publish(contents, null);
    }

    public Future<UUID> publish(ByteBuffer contents, Map<String, ?> filters) {
        String tableName = makeSureTableExists(filters);

        QueryBuilder queryBuilder = new QueryBuilder(cluster);
        final UUID startTime = UUIDs.timeBased();
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

        Future<UUID> publishResult = executorService.submit(new Callable<UUID>() {
            @Override
            public UUID call() throws Exception {
                session.executeAsync(insert).getUninterruptibly();
                return startTime;
            }
        });

        return publishResult;
    }

    public Future<Map<String, Object>> consume() {
        return consume(null);
    }

    public Future<Map<String, Object>> consume(final Map<String, ?> filters) {

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        final String key = df.format(new Date());
        final String tableName = makeSureTableExists(filters);
        final QueryBuilder queryBuilder = new QueryBuilder(session.getCluster());
        final Select.Where select = queryBuilder.select().column("start_time").column("contents")
                .from(cassandraKeyspace, tableName)
                .where(QueryBuilder.eq("key", key));

        if (filters != null && filters.size() > 0) {
            for (String filter : filters.keySet()) {
                select.and(QueryBuilder.eq(filter, filters.get(filter)));
            }
        }

        select.and(QueryBuilder.lt("start_time", UUIDs.endOf(System.currentTimeMillis())));

        select.limit(1);

        Future<Map<String, Object>> itemFuture = executorService.submit(new Callable<Map<String, Object>>() {
            @Override
            public Map<String, Object> call() throws Exception {

                InterProcessMutex interProcessMutex = new InterProcessMutex(client, "/dqueue/" + tableName);

                try {
                    interProcessMutex.acquire();

                    ImmutableMap.Builder<String, Object> itemBuilder = ImmutableMap.builder();

                    ResultSet resultSet = session.executeAsync(select).getUninterruptibly();
                    Row row = resultSet.one();

                    if (row == null) {
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

                    session.executeAsync(delete).getUninterruptibly();

                    return map;
                } catch (Exception e) {
                    throw e;
                } finally {
                    interProcessMutex.release();
                }
            }
        });

        return itemFuture;
    }

    @Override
    public void close() throws Exception {
        executorService.shutdown();
        IOUtils.closeQuietly(client);
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
