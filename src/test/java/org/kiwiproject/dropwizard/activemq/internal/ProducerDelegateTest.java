package org.kiwiproject.dropwizard.activemq.internal;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.collect.KiwiMaps.newHashMap;
import static org.kiwiproject.dropwizard.activemq.internal.ProducerDelegateTest.DestinationType.DYNAMIC;
import static org.kiwiproject.dropwizard.activemq.internal.ProducerDelegateTest.DestinationType.NAMED;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.kiwiproject.elucidation.client.ElucidationClient;
import org.kiwiproject.elucidation.client.ElucidationResult;

import javax.jms.ConnectionFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@DisplayName("ProducerDelegate")
class ProducerDelegateTest {

    enum DestinationType {
        NAMED, DYNAMIC
    }

    private static final String NAMED_DESTINATION = "queue:test";
    private static final String DYNAMIC_IDENTIFIER = "*";
    private static final String DYNAMIC_DESTINATION = "*:test";
    private static final String ALL_EVENTS_DESTINATION = "queue:all_events";

    private Producer testProducer;
    private Producer allEventsProducer;
    private ConnectionFactory factory;
    private ElucidationClient<String> elucidationClient;
    private String serviceName;

    @BeforeEach
    void setUp() {
        testProducer = mock(Producer.class);
        allEventsProducer = mock(Producer.class);

        factory = mock(ConnectionFactory.class);

        elucidationClient = mockElucidationClient();
        var result = ElucidationResult.fromSkipMessage("Recorder not enabled");
        when(elucidationClient.recordNewEvent(anyString())).thenReturn(CompletableFuture.completedFuture(result));

        serviceName = uniqueServiceName();
    }

    @SuppressWarnings("unchecked")
    private ElucidationClient<String> mockElucidationClient() {
        return mock(ElucidationClient.class);
    }

    @Test
    void shouldCreateNewInstance_WithNoDestinations() {
        var delegate = newDefaultProducerDelegate(List.of(), List.of());

        assertAll(
                () -> assertThat(delegate.producers).containsOnlyKeys(ALL_EVENTS_DESTINATION),
                () -> assertThat(delegate.containsDefaultDestination(ALL_EVENTS_DESTINATION)).isTrue()
        );
    }

    @Test
    void shouldCreateNewInstance_WithOneDestination_AndContainingAllEventsQueue() {
        var delegate = newDefaultProducerDelegate(List.of(NAMED_DESTINATION), List.of());

        assertAll(
                () -> assertThat(delegate.producers).containsOnlyKeys(NAMED_DESTINATION, ALL_EVENTS_DESTINATION),
                () -> assertThat(delegate.containsDestination(NAMED_DESTINATION)).isTrue(),
                () -> assertThat(delegate.containsDefaultDestination(ALL_EVENTS_DESTINATION)).isTrue()
        );
    }

    @Test
    void shouldCreateNewInstance_WithAllEventsQueue_AsNormalDestination() {
        var delegate = newDefaultProducerDelegate(List.of(ALL_EVENTS_DESTINATION), List.of());

        assertAll(
                () -> assertThat(delegate.producers).containsOnlyKeys(ALL_EVENTS_DESTINATION),
                () -> assertThat(delegate.containsDestination(ALL_EVENTS_DESTINATION)).isTrue(),
                () -> assertThat(delegate.containsDefaultDestination(ALL_EVENTS_DESTINATION)).isFalse()
        );
    }

    @Test
    void shouldCreateNewInstance_WithSingleDestination_AndMultipleDefaultDestinations() {
        var defaultDestinations = List.of("queue1", "queue2");
        var delegate = newDefaultProducerDelegate(List.of("test"), defaultDestinations);

        assertAll(
                () -> assertThat(delegate.producers).containsOnlyKeys("test", "queue1", "queue2"),
                () -> assertThat(delegate.containsDestination("test")).isTrue(),
                () -> assertThat(delegate.containsDefaultDestination("queue1")).isTrue(),
                () -> assertThat(delegate.containsDefaultDestination("queue2")).isTrue(),
                () -> assertThat(delegate.containsDefaultDestination(ALL_EVENTS_DESTINATION)).isFalse()
        );
    }

    private ProducerDelegate newDefaultProducerDelegate(List<String> destinations, List<String> defaultDestinations) {
        return new ProducerDelegate(factory,
                destinations,
                defaultDestinations,
                false,
                Duration.ZERO,
                ElucidationClient.noop(),
                serviceName);
    }

    @Test
    void shouldProduceToAllEvents_whenUsingDynamicDestinations_AndNoExplicitDefaultDestinations() {
        assertProducesToAllEventsAsDefaultDestination(DYNAMIC);
    }

    @Test
    void shouldProduceToAllEvents_whenUsingNamedDestinations_AndNoExplicitDefaultDestinations() {
        assertProducesToAllEventsAsDefaultDestination(NAMED);
    }

    private void assertProducesToAllEventsAsDefaultDestination(DestinationType destinationType) {
        var payload = "Test!";

        String destination;
        ProducerDelegate delegate;

        if (destinationType == DYNAMIC) {
            destination = DYNAMIC_DESTINATION;
            delegate = newDefaultProducerDelegateAllowingDynamicDestinations();
            doNothing().when(testProducer).produce(payload, destination);
        } else {
            destination = NAMED_DESTINATION;
            delegate = newDefaultProducerDelegateNoDynamicDestinations();
            doNothing().when(testProducer).produce(payload);
        }

        when(allEventsProducer.isDefaultProducer()).thenReturn(true);
        doNothing().when(allEventsProducer).produce(payload);

        delegate.produceToDestinationAndAllEventsQueue(destination, payload);

        if (destinationType == DYNAMIC) {
            verify(testProducer).produce(payload, destination, Map.of());
        } else {
            verify(testProducer).produce(payload, Map.of());
        }

        verify(allEventsProducer).isDefaultProducer();
        verify(allEventsProducer).produce(payload, Map.of());

        verifyNoMoreInteractions(testProducer, allEventsProducer);

        verifyElucidationWasInvokedProperly(destination, payload, ALL_EVENTS_DESTINATION);
    }

    private ProducerDelegate newDefaultProducerDelegateAllowingDynamicDestinations() {
        return newDefaultProducerDelegateAllowingDynamicDestinations(
                true,
                DYNAMIC_IDENTIFIER, testProducer,
                ALL_EVENTS_DESTINATION, allEventsProducer
        );
    }

    private ProducerDelegate newDefaultProducerDelegateAllowingDynamicDestinations(boolean allowDynamicDestinations,
                                                                                   Object... destinationToProducers) {
        var delegate = new ProducerDelegate(factory,
                List.of(),
                List.of(),
                allowDynamicDestinations,
                Duration.ZERO,
                elucidationClient,
                serviceName);

        delegate.producers.putAll(newHashMap(destinationToProducers));

        return delegate;
    }

    @Test
    void shouldProduceToExplicitDefaultDestinations_whenUsingDynamicDestinations_AndExplicitDefaultDestinations() {
        assertProducesToExplicitDefaultDestinations(DYNAMIC);
    }

    @Test
    void shouldProduceToExplicitDefaultDestinations_whenUsingNamedDestinations_AndExplicitDefaultDestinations() {
        assertProducesToExplicitDefaultDestinations(NAMED);
    }

    private void assertProducesToExplicitDefaultDestinations(DestinationType destinationType) {
        var payload = "Test!";

        var queue1 = "queue:Q1";
        var queue1Producer = newMockDefaultProducer(payload, queue1);

        var queue2 = "queue:Q2";
        var queue2Producer = newMockDefaultProducer(payload, queue2);

        String identifier;
        String destination;
        ProducerDelegate delegate;
        boolean allowDynamicDestinations;

        if (destinationType == DYNAMIC) {
            identifier = DYNAMIC_IDENTIFIER;
            destination = DYNAMIC_DESTINATION;
            allowDynamicDestinations = true;
            doNothing().when(testProducer).produce(payload, destination, Map.of());
        } else {
            identifier = NAMED_DESTINATION;
            destination = NAMED_DESTINATION;
            allowDynamicDestinations = false;
            doNothing().when(testProducer).produce(payload, Map.of());
        }

        delegate = new ProducerDelegate(factory,
                List.of(destination),
                List.of(queue1, queue2),
                allowDynamicDestinations,
                Duration.ZERO,
                elucidationClient,
                serviceName);

        delegate.producers.putAll(newHashMap(
                identifier, testProducer,
                queue1, queue1Producer,
                queue2, queue2Producer
        ));

        delegate.produceToDestinationAndAllEventsQueue(destination, payload);

        verify(queue1Producer).produce(payload, Map.of());
        verify(queue1Producer).isDefaultProducer();

        verify(queue2Producer).produce(payload, Map.of());
        verify(queue2Producer).isDefaultProducer();

        if (destinationType == DYNAMIC) {
            verify(testProducer).produce(payload, destination, Map.of());
        } else {
            verify(testProducer).produce(payload, Map.of());
        }

        verifyNoMoreInteractions(queue1Producer, queue2Producer, testProducer);
        verifyNoInteractions(allEventsProducer);

        verifyElucidationWasInvokedProperly(destination, payload, queue1, queue2);
    }

    private Producer newMockDefaultProducer(String payload, String destination) {
        var producer = mock(Producer.class);
        when(producer.isDefaultProducer()).thenReturn(true);
        doNothing().when(producer).produce(payload, destination);
        return producer;
    }

    @Test
    void shouldThrowIllegalArgumentException_WhenDestinationInInvalid() {
        var delegate = newDefaultProducerDelegateNoDynamicDestinations();

        var payload = "Test!";

        assertThatIllegalArgumentException()
                .isThrownBy(() -> delegate.produceToDestination("queue:invalid", payload))
                .withMessage("Producer 'queue:invalid' not found!");

        verifyNoInteractions(testProducer, allEventsProducer, elucidationClient);
    }

    @Test
    void shouldProduceToAllEvents() {
        var delegate = newDefaultProducerDelegateNoDynamicDestinations();

        var payload = "Test!";

        delegate.produceToAllEventsQueue(payload);

        verify(allEventsProducer).produce(payload, Map.of());

        verifyNoMoreInteractions(allEventsProducer);
        verifyNoInteractions(testProducer);

        verifyElucidationWasInvokedProperly("queue:all_events", payload);
    }

    @Test
    void shouldProduceBytesMessage_WithInvalidDestination() {
        var delegate = newDefaultProducerDelegateNoDynamicDestinations();

        var payload = "Test!".getBytes(UTF_8);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> delegate.produceBytesMessage("queue:invalid", payload))
                .withMessage("Producer 'queue:invalid' not found!");

        verifyNoInteractions(testProducer, allEventsProducer, elucidationClient);
    }

    @Test
    void shouldProduceBytesMessage_WithKnownDestination() {
        var delegate = newDefaultProducerDelegateNoDynamicDestinations();

        var destination = "queue:test";
        var payload = "Test!".getBytes(UTF_8);

        delegate.produceBytesMessage(destination, payload);

        verify(testProducer).produceBytesMessage(payload);
        verifyNoMoreInteractions(testProducer);
        verifyNoInteractions(allEventsProducer);

        verifyElucidationWasInvokedProperly(destination, "BYTES_MESSAGE");
    }

    private ProducerDelegate newDefaultProducerDelegateNoDynamicDestinations() {
        return newDefaultProducerDelegateAllowingDynamicDestinations(
                false,
                NAMED_DESTINATION, testProducer,
                ALL_EVENTS_DESTINATION, allEventsProducer
        );
    }

    private void verifyElucidationWasInvokedProperly(String destination, String payload, String... defaultDestinations) {
        var shortName = getShortName(destination);

        verify(elucidationClient).recordNewEvent(createEventId(shortName, payload));

        Arrays.stream(defaultDestinations)
                .forEach(defaultDestination -> {
                    var defaultShortName = getShortName(defaultDestination);
                    verify(elucidationClient).recordNewEvent(createEventId(defaultShortName, payload));
                });

        verifyNoMoreInteractions(elucidationClient);
    }

    private static String getShortName(String destination) {
        return destination.split(":")[1];
    }

    private static String createEventId(String serviceName, String payload) {
        return serviceName + "::" + payload;
    }
}
