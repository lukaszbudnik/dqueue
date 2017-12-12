/**
 * Copyright (C) 2015-2017 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue;


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.curator.framework.CuratorFramework;


public class QueueClientBuilder {

    private static final int DEFAULT_CASSANDRA_PORT = 9042;
    private static final int DEFAULT_THREAD_POOL_SIZE = 300;

    @Inject(optional = true)
    @Named("dqueue.cassandraPort")
    private int cassandraPort = DEFAULT_CASSANDRA_PORT;

    @Inject
    @Named("dqueue.cassandraAddress")
    private String cassandraAddress;

    @Inject
    @Named("dqueue.cassandraKeyspace")
    private String cassandraKeyspace;

    @Inject
    @Named("dqueue.cassandraTablePrefix")
    private String cassandraTablePrefix;

    @Inject(optional = true)
    @Named("dqueue.cassandraCreateTables")
    private boolean cassandraCreateTables = true;

    @Inject(optional = true)
    @Named("dqueue.threadPoolSize")
    private int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

    private CuratorFramework zookeeperClient;

    private MetricRegistry metricRegistry;
    private HealthCheckRegistry healthCheckRegistry;

    public QueueClientBuilder() {
    }

    public QueueClientBuilder withCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
        return this;
    }

    public QueueClientBuilder withCassandraAddress(String cassandraAddress) {
        this.cassandraAddress = cassandraAddress;
        return this;
    }

    public QueueClientBuilder withCassandraKeyspace(String cassandraKeyspace) {
        this.cassandraKeyspace = cassandraKeyspace;
        return this;
    }

    public QueueClientBuilder withCassandraTablePrefix(String cassandraTablePrefix) {
        this.cassandraTablePrefix = cassandraTablePrefix;
        return this;
    }

    public QueueClientBuilder withCassandraCreateTables(boolean cassandraCreateTables) {
        this.cassandraCreateTables = cassandraCreateTables;
        return this;
    }

    public QueueClientBuilder withThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        return this;
    }

    public QueueClientBuilder withZookeeperClient(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
        return this;
    }

    public QueueClientBuilder withMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
        return this;
    }

    public QueueClientBuilder withHealthMetricRegistry(HealthCheckRegistry healthCheckRegistry) {
        this.healthCheckRegistry = healthCheckRegistry;
        return this;
    }

    public QueueClient build() throws Exception {
        QueueClientImpl queueClient = new QueueClientImpl(cassandraPort, cassandraAddress.split(","), cassandraKeyspace, cassandraTablePrefix, cassandraCreateTables, zookeeperClient, threadPoolSize, metricRegistry, healthCheckRegistry);
        return queueClient;
    }

    public OrderedQueueClient buildOrdered() throws Exception {
        OrderedQueueClientImpl queueClient = new OrderedQueueClientImpl(cassandraPort, cassandraAddress.split(","), cassandraKeyspace, cassandraTablePrefix, cassandraCreateTables, zookeeperClient, threadPoolSize, metricRegistry, healthCheckRegistry);
        return queueClient;
    }

    public int getCassandraPort() {
        return cassandraPort;
    }

    public void setCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
    }

    public String getCassandraAddress() {
        return cassandraAddress;
    }

    public void setCassandraAddress(String cassandraAddress) {
        this.cassandraAddress = cassandraAddress;
    }

    public String getCassandraKeyspace() {
        return cassandraKeyspace;
    }

    public void setCassandraKeyspace(String cassandraKeyspace) {
        this.cassandraKeyspace = cassandraKeyspace;
    }

    public String getCassandraTablePrefix() {
        return cassandraTablePrefix;
    }

    public void setCassandraTablePrefix(String cassandraTablePrefix) {
        this.cassandraTablePrefix = cassandraTablePrefix;
    }

    public boolean isCassandraCreateTables() {
        return cassandraCreateTables;
    }

    public void setCassandraCreateTables(boolean cassandraCreateTables) {
        this.cassandraCreateTables = cassandraCreateTables;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    public void setMetricRegistry(MetricRegistry metricRegistry) {
        this.metricRegistry = metricRegistry;
    }

    public HealthCheckRegistry getHealthCheckRegistry() {
        return healthCheckRegistry;
    }

    public void setHealthCheckRegistry(HealthCheckRegistry healthCheckRegistry) {
        this.healthCheckRegistry = healthCheckRegistry;
    }

    public CuratorFramework getZookeeperClient() {
        return zookeeperClient;
    }

    public void setZookeeperClient(CuratorFramework zookeeperClient) {
        this.zookeeperClient = zookeeperClient;
    }
}
