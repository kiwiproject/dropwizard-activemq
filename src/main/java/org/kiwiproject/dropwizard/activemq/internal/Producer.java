package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

import javax.jms.ConnectionFactory;

@Slf4j
public class Producer {

    private final ConnectionFactory factory;
    private final String destination;

    @Getter
    private final boolean isDefaultProducer;

    private final String serviceName;

    // TODO Why is this NOT part of the constructor?
    @Setter
    private Duration timeToLive;

    public Producer(ConnectionFactory factory,
                    String destination,
                    boolean isDefaultProducer,
                    String serviceName) {

        this.factory = requireNotNull(factory);
        this.destination = requireNotBlank(destination);
        this.isDefaultProducer = isDefaultProducer;
        this.serviceName = requireNotBlank(serviceName);
    }

    // TODO
}
