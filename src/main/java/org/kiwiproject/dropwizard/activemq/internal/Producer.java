package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;

import javax.jms.ConnectionFactory;

/**
 * This internal class uses JMS {@link javax.jms.MessageProducer} instances to send messages.
 */
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

    public void produce(String payload) {
        produce(payload, destination);
    }

    public void produce(String payload, String destination) {
        produce(payload, destination, Map.of());
    }

    public void produce(String payload, Map<String, Object> headers) {
        produce(payload, destination, headers);
    }

    public void produce(String payload, String destination, Map<String, Object> headers) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produce'");
    }

    public void produceBytesMessage(byte[] payload) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceBytesMessage'");
    }

    public void produceBytesMessage(byte[] payload, String destination) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceBytesMessage'");
    }

    // TODO
}
