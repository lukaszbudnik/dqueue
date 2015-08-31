package com.github.lukaszbudnik.dqueue;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class QueueClientBuilderGuicePropertiesModuleTest {

    @Test
    public void shouldBindConfigurationSettings() {
        Injector injector = Guice.createInjector(new QueueClientBuilderGuicePropertiesModule("/dqueue_guice_module_test.properties"));

        QueueClientBuilder queueClientBuilder = injector.getInstance(QueueClientBuilder.class);

        assertEquals("dqueue_keyspace", queueClientBuilder.getCassandraKeyspace());
        assertEquals(11111, queueClientBuilder.getCassandraPort());
        assertEquals("cassandra1.docker,cassandra2.docker", queueClientBuilder.getCassandraAddress());
        assertEquals("dqueue", queueClientBuilder.getCassandraTablePrefix());
        assertFalse(queueClientBuilder.isCassandraCreateTables());
    }

}
