package org.kiwiproject.dropwizard.activemq.internal;

import com.codahale.metrics.health.HealthCheck;

import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.dropwizard.activemq.ActiveMqConsumer;
import org.kiwiproject.elucidation.client.ElucidationClient;

import javax.jms.ConnectionFactory;

import io.dropwizard.lifecycle.Managed;

@Slf4j
public class Consumer implements Managed, Runnable {

    public Consumer(ConnectionFactory factory,
                    String destination,
                    ActiveMqConsumer delegateConsumer,
                    ElucidationClient<String> elucidation,
                    String serviceName) {
        // TODO
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'run'");
    }

    public HealthCheck getHealtCheck() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getHealtCheck'");
    }

}
