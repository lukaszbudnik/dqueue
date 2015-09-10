package com.github.lukaszbudnik.dqueue.jaxrs.core;

import com.github.lukaszbudnik.cloudtag.CloudTagEnsembleProvider;
import com.github.lukaszbudnik.dqueue.QueueClient;
import com.github.lukaszbudnik.dqueue.QueueClientBuilder;
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

        QueueClient queueClient = queueClientBuilder.build();

        environment.jersey().register(new QueueService(queueClient));
        environment.jersey().register(MultiPartFeature.class);
    }

    @Override
    public void initialize(Bootstrap<QueueConfiguration> bootstrap) {
        bootstrap.addBundle(new AssetsBundle("/html", "/html", null, "html"));
        bootstrap.addBundle(new AssetsBundle("/js", "/js", null, "js"));
    }
}
