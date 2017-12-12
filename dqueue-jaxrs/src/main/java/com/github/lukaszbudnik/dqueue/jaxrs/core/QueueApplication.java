/**
 * Copyright (C) 2015-2017 Łukasz Budnik <lukasz.budnik@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 */
package com.github.lukaszbudnik.dqueue.jaxrs.core;

import com.github.lukaszbudnik.cloudtag.CloudTagEnsembleProvider;
import com.github.lukaszbudnik.dqueue.QueueClientBuilder;
import com.github.lukaszbudnik.dqueue.OrderedQueueClient;
import com.github.lukaszbudnik.dqueue.jaxrs.service.QueueService;
import com.github.lukaszbudnik.gpe.PropertiesElResolverModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

public class QueueApplication extends Application<QueueConfiguration> {

    public static void main(String[] args) throws Exception {
        new QueueApplication().run(args);
    }

    @Override
    public void run(QueueConfiguration configuration, Environment environment) throws Exception {
        Injector injector = Guice.createInjector(new PropertiesElResolverModule(configuration.getProperties()));

        QueueClientBuilder queueClientBuilder = injector.getInstance(QueueClientBuilder.class);

        CloudTagEnsembleProvider cloudTagEnsembleProvider = injector.getInstance(CloudTagEnsembleProvider.class);
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework zookeeperClient = CuratorFrameworkFactory.builder().ensembleProvider(cloudTagEnsembleProvider).retryPolicy(retryPolicy).build();
        zookeeperClient.start();
        zookeeperClient.getData().forPath("/");

        queueClientBuilder.withZookeeperClient(zookeeperClient);
        queueClientBuilder.withMetricRegistry(environment.metrics());
        queueClientBuilder.withHealthMetricRegistry(environment.healthChecks());

        OrderedQueueClient queueClient = queueClientBuilder.buildOrdered();

        environment.jersey().register(new QueueService(queueClient));
        environment.jersey().register(MultiPartFeature.class);
    }

    @Override
    public void initialize(Bootstrap<QueueConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/html", "/html", null, "html"));
        bootstrap.addBundle(new AssetsBundle("/js", "/js", null, "js"));
    }
}
