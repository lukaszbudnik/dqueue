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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import de.odysseus.el.ExpressionFactoryImpl;
import de.odysseus.el.util.SimpleContext;

import javax.el.ExpressionFactory;
import javax.el.ValueExpression;
import java.util.Enumeration;
import java.util.Map;
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

            binder = binder.skipSources(Names.class);

            ExpressionFactory factory = new ExpressionFactoryImpl();
            SimpleContext context = new SimpleContext();
            context.setVariable("env", factory.createValueExpression(System.getenv(), Map.class));
            context.setVariable("properties", factory.createValueExpression(System.getProperties(), Map.class));

            for (Enumeration<?> e = properties.propertyNames(); e.hasMoreElements(); ) {
                String propertyName = (String) e.nextElement();
                String value = properties.getProperty(propertyName);
                ValueExpression expression = factory.createValueExpression(context, value, String.class);
                String evaluated = (String) expression.getValue(context);
                binder.bind(Key.get(String.class, Names.named(propertyName))).toInstance(evaluated);
            }
        } catch (Exception e) {
            // if a properties file is not found this module will do nothing
            // later on injector will fail if named dependencies will not be resolved
            // so no panic :)
        }
    }
}
