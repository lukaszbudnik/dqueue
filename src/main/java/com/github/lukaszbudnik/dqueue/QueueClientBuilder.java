package com.github.lukaszbudnik.dqueue;

public class QueueClientBuilder {

    private int cassandraPort;
    private String[] cassandraAddress;
    private String cassandraKeyspace;
    private String cassandraTablePrefix;
    private boolean cassandraCreateTables;

    public QueueClientBuilder(int cassandraPort, String[] cassandraAddress, String cassandraKeyspace, String cassandraTablePrefix, boolean cassandraCreateTables) {
        this.cassandraPort = cassandraPort;
        this.cassandraAddress = cassandraAddress;
        this.cassandraKeyspace = cassandraKeyspace;
        this.cassandraTablePrefix = cassandraTablePrefix;
        this.cassandraCreateTables = cassandraCreateTables;
    }

    public QueueClientBuilder() {
    }

    public QueueClientBuilder withCassandraPort(int cassandraPort) {
        this.cassandraPort = cassandraPort;
        return this;
    }

    public QueueClientBuilder withCassandraAddress(String[] cassandraAddress) {
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

    public QueueClient build() throws Exception {
        QueueClient queueClient = new QueueClient(cassandraPort, cassandraAddress, cassandraKeyspace, cassandraTablePrefix, cassandraCreateTables);
        return queueClient;
    }

}
