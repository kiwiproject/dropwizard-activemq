package org.kiwiproject.dropwizard.activemq.internal;

import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.dropwizard.activemq.ActiveMqProducer;
import org.kiwiproject.elucidation.client.ElucidationClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import javax.jms.ConnectionFactory;

@Slf4j
public class ProducerDelegate implements ActiveMqProducer {

    public ProducerDelegate(ConnectionFactory factory,
                            List<String> destinations,
                            List<String> defaultDestinations,
                            boolean allowDynamicDestinations,
                            Duration timeToLive,
                            ElucidationClient<String> elucidation,
                            String serviceName) {
        // TODO
    }

    @Override
    public void produce(String destination, String payload, PayloadDestination payloadDestination,
            Map<String, Object> headers) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produce'");
    }

    @Override
    public void produceBytesMessage(String destination, byte[] payload) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceBytesMessage'");
    }

    // TODO
}
