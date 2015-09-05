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
