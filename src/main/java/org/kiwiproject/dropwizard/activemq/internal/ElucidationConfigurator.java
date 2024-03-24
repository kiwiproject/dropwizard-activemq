package org.kiwiproject.dropwizard.activemq.internal;

import static org.apache.commons.lang3.StringUtils.abbreviate;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.BYTES_MESSAGE_TYPE;
import static org.kiwiproject.dropwizard.activemq.internal.DestinationExtractor.createElucidationDestination;
import static org.kiwiproject.dropwizard.activemq.internal.TypesDetector.determineMessageTypeFrom;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.ElucidationContext;
import org.kiwiproject.dropwizard.activemq.config.ElucidationConfigured;
import org.kiwiproject.elucidation.client.ElucidationRecorder;
import org.kiwiproject.elucidation.common.model.ConnectionEvent;
import org.kiwiproject.elucidation.common.model.Direction;
import org.kiwiproject.jersey.client.RegistryAwareClient;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
@UtilityClass
public class ElucidationConfigurator {

    public static ElucidationContext configure(ElucidationConfigured elucidationConfigured,
                                               RegistryAwareClient client) {

        var elucidationService = elucidationConfigured.getElucidationService();
        var serviceName = elucidationConfigured.getServiceName();

        Supplier<String> elucidationServerBaseUriSupplier = () ->
                client.targetForService(elucidationService).getUri().toString();

        var recorder = new ElucidationRecorder(client, elucidationServerBaseUriSupplier);
        var consumingEventFactory = newConsumingJmsEventFactory(serviceName);
        var producingEventFactory = newProducingJmsEventFactory(serviceName);

        return ElucidationContext.builder()
                .eventRecorder(recorder)
                .consumingTextMessageEventFactory(consumingEventFactory)
                .producingTextMessageEventFactory(producingEventFactory)
                .build();
    }

    /**
     * On the consuming side, we will already have extracted the message type, so no need to extract a second time.
     */
    private static Function<String, Optional<ConnectionEvent>> newConsumingJmsEventFactory(String serviceName) {
        return messageType -> Optional.of(newConnectionEvent(serviceName, Direction.INBOUND, messageType));
    }

    /**
     * On the producing side, we will need to extract the message type from the "composite" which is expected
     * to be in the format {@code <destination>::<payload>}. The message type is determined from the payload,
     * e.g., if the payload is JSON we use {@link TypesDetector#determineMessageTypeFrom(String)}.
     */
    private static Function<String, Optional<ConnectionEvent>> newProducingJmsEventFactory(String serviceName) {
        return composite -> {
            String[] parts = DestinationExtractor.DELIMITER_SPLITTER.split(composite);

            String destination = parts[0];
            String payload;

            if (parts.length == 2) {
                payload = parts[1];
            } else {
                LOG.error("Unable to extract destination/payload parts for elucidation reporting from string: {}",
                        abbreviate(composite, 100));
                payload = composite;
            }

            if (BYTES_MESSAGE_TYPE.equals(payload)) {
                var elucidationDestination = createElucidationDestination(destination, BYTES_MESSAGE_TYPE);
                var event = newConnectionEvent(serviceName, Direction.OUTBOUND, elucidationDestination);
                return Optional.of(event);
            }

            Optional<String> messageType = Optional.ofNullable(determineMessageTypeFrom(payload));

            return messageType
                    .map(type -> {
                        var elucidationDestination = createElucidationDestination(destination, type);
                        return newConnectionEvent(serviceName, Direction.OUTBOUND, elucidationDestination);
                    });
        };
    }

    private static ConnectionEvent newConnectionEvent(String serviceName,
                                                      Direction eventDirection,
                                                      String messageType) {
        return ConnectionEvent.builder()
                .serviceName(serviceName)
                .eventDirection(eventDirection)
                .communicationType("JMS")
                .connectionIdentifier(messageType)
                .observedAt(System.currentTimeMillis())
                .build();
    }
}
