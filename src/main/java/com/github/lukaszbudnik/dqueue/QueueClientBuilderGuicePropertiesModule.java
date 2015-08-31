package com.github.lukaszbudnik.dqueue;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.name.Names;

import java.util.Properties;

public class QueueClientBuilderGuicePropertiesModule implements Module {
    public static final String DEFAULT_DQUEUE_PROPERTIES = "/dqueue.properties";

    private final String dqueueProperties;

    public QueueClientBuilderGuicePropertiesModule() {
        this(DEFAULT_DQUEUE_PROPERTIES);
    }

    public QueueClientBuilderGuicePropertiesModule(String dqueueProperties) {
        this.dqueueProperties = dqueueProperties;
    }

    @Override
    public void configure(Binder binder) {
        try {
            Properties properties = new Properties();
            properties.load(getClass().getResourceAsStream(dqueueProperties));
            Names.bindProperties(binder, properties);
        } catch (Exception e) {
            // if a properties file is not found this module will do nothing
            // later on injector will fail if named dependencies will not be resolved
            // so no panic :)
        }
    }
}
