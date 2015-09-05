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

        // in test dqueue properties file cassandra keyspace is set to dynamic value:
        // ${properties['java.version']}
        String javaVersion = System.getProperty("java.version");
        assertEquals(javaVersion, queueClientBuilder.getCassandraKeyspace());

        // in test dqueue properties file cassandra table prefix is set to dynamic value:
        // ${env['HOME']}
        String home = System.getenv("HOME");
        assertEquals(home, queueClientBuilder.getCassandraTablePrefix());

        // other settings are literal
        assertEquals(11111, queueClientBuilder.getCassandraPort());
        assertEquals("cassandra1.docker,cassandra2.docker", queueClientBuilder.getCassandraAddress());
        assertFalse(queueClientBuilder.isCassandraCreateTables());
    }

}
