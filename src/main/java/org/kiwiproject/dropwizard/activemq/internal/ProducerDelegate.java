package org.kiwiproject.dropwizard.activemq.internal;

import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.dropwizard.activemq.ActiveMqConstants.ALL_EVENTS_QUEUE;

import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.dropwizard.activemq.ActiveMqProducer;
import org.kiwiproject.dropwizard.activemq.util.DynamicDestinations;
import org.kiwiproject.elucidation.client.ElucidationClient;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.ConnectionFactory;

@Slf4j
public class ProducerDelegate implements ActiveMqProducer {

    private static final String PRODUCER_NOT_FOUND = "Producer '{}' not found";

    @VisibleForTesting
    final Map<String, Producer> producers = new HashMap<>();

    private final boolean allowDynamicDestinations;
    private final Duration timeToLive;
    private final ElucidationClient<String> elucidation;
    private final String serviceName;

    // TODO...

    public ProducerDelegate(ConnectionFactory factory,
                            List<String> destinations,
                            List<String> defaultDestinations,
                            boolean allowDynamicDestinations,
                            Duration timeToLive,
                            ElucidationClient<String> elucidation,
                            String serviceName) {

        // Set this first so that other internal methods see it (putNewProducer)
        this.serviceName = requireNotBlank(serviceName);

        this.allowDynamicDestinations = allowDynamicDestinations;
        this.timeToLive = requireNotNull(timeToLive);

        checkArgumentNotNull(factory);
        requireNotNull(destinations).forEach(destination -> putNewProducer(destination, factory));

        checkArgumentNotNull(defaultDestinations);
        if (defaultDestinations.isEmpty()) {
            if (doesNotContainAllEventsQueue()) {
                putNewDefaultProducer(ALL_EVENTS_QUEUE, factory);
            }
        } else {
            defaultDestinations.forEach(destination -> putNewDefaultProducer(destination, factory));
        }

        if (allowDynamicDestinations) {
            putNewProducer(DynamicDestinations.DYNAMIC_DESTINATION_ID, factory);
        }

        this.elucidation = requireNotNull(elucidation);
    }

    private void putNewProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, false);
    }

    private boolean doesNotContainAllEventsQueue() {
        return !producers.containsKey(ALL_EVENTS_QUEUE);
    }

    private void putNewDefaultProducer(String destination, ConnectionFactory factory) {
        putNewProducer(destination, factory, true);
    }

    private void putNewProducer(String destination, ConnectionFactory factory, boolean isDefaultProducer) {
        var producer = new Producer(factory, destination, isDefaultProducer, serviceName);
        producer.setTimeToLive(timeToLive);

        producers.put(destination, producer);
    }

    /**
     * Does this producer produce to the specified destination and is not a default producer?
     */
    public boolean containsDestination(String destination) {
        return producers.containsKey(destination) && !producers.get(destination).isDefaultProducer();
    }

    @Override
    public void produce(String destination,
                        String payload,
                        PayloadDestination payloadDestination,
                        Map<String, Object> headers) {

        if (isDynamicDestination(destination)) {
            produceToDynamicDestination(destination, payload, payloadDestination, headers);
        } else if (producers.containsKey(destination)) {
            produceToKnownDestination(destination, payload, payloadDestination, headers);
        } else {
            throwIllegalArgumentExceptionForUnknownDestination(destination);
        }
    }

    private boolean isDynamicDestination(String destination) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isDynamicDestination'");
    }

    private void produceToDynamicDestination(String destination, String payload, PayloadDestination payloadDestination,
            Map<String, Object> headers) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceToDynamicDestination'");
    }

    private void produceToKnownDestination(String destination, String payload, PayloadDestination payloadDestination,
            Map<String, Object> headers) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceToKnownDestination'");
    }

    @Override
    public void produceBytesMessage(String destination, byte[] payload) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'produceBytesMessage'");
    }

    private static void throwIllegalArgumentExceptionForUnknownDestination(String destination) {
        var message = f(PRODUCER_NOT_FOUND, destination);
        LOG.error(message);

        throw new IllegalArgumentException(message);
    }
}
