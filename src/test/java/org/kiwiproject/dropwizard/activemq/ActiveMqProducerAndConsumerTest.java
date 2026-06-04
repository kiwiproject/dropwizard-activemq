package org.kiwiproject.dropwizard.activemq;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.second;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createNonTransactedSession;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createQueueConsumerMessageListener;
import static org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils.createTopicConsumerMessageListener;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.kiwiproject.test.assertj.KiwiAssertJ.assertIsExactType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import org.awaitility.Durations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.internal.Consumer;
import org.kiwiproject.dropwizard.activemq.internal.ProducerDelegate;
import org.kiwiproject.dropwizard.activemq.test.junit.jupiter.EmbeddedActiveMqExtension;
import org.kiwiproject.dropwizard.activemq.test.mock.MockActiveMqConsumer;
import org.kiwiproject.jersey.client.RegistryAwareClient;
import org.kiwiproject.test.dropwizard.mockito.DropwizardMockitoMocks;
import org.mockito.ArgumentCaptor;

import java.util.List;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Session;

@DisplayName("ActiveMqProducerAndConsumer")
class ActiveMqProducerAndConsumerTest {

    @RegisterExtension
    final EmbeddedActiveMqExtension broker = new EmbeddedActiveMqExtension();

    private TestAppConfig appConfig;
    private ActiveMqConfig activeMqConfig;
    private Environment environment;
    private LifecycleEnvironment lifecycle;
    private HealthCheckRegistry healthChecks;
    private RegistryAwareClient registryAwareClient;
    private ActiveMqHelper activeMqHelper;
    private Connection connection;
    private Session session;
    private DropwizardActiveMq<TestAppConfig> dropwizardActiveMq;

    @BeforeEach
    void setUp() throws JMSException {
        activeMqConfig = new ActiveMqConfig();
        activeMqConfig.setTlsConfiguration(newTlsContextConfiguration());
        activeMqConfig.setBrokerUri("tcp://server.acme.com:61616");
        activeMqConfig.setHealthCheckNamePrefix("ACME");
        activeMqConfig.setConsumers(List.of("topic:STATUS_CHANGES"));
        activeMqConfig.setProducers(List.of("topic:STUFF", "topic:MORE_STUFF"));
        activeMqConfig.getDefaultProducers().add("queue:AUDIT");

        appConfig = TestAppConfig.builder()
                .activeMqConfig(activeMqConfig)
                .elucidationEnabled(false)
                .build();

        environment = DropwizardMockitoMocks.mockEnvironment();
        lifecycle = environment.lifecycle();
        healthChecks = environment.healthChecks();

        registryAwareClient = mock(RegistryAwareClient.class);

        var pooledFactory = broker.newPooledConnectionFactory();

        connection = pooledFactory.createConnection();
        connection.start();

        session = createNonTransactedSession(connection);

        activeMqHelper = mock(ActiveMqHelper.class);
        when(activeMqHelper.newPooledConnectionFactory(any(ActiveMqConfig.class)))
                .thenReturn(pooledFactory);
    }

    @AfterEach
    void tearDown() throws JMSException {
        if (nonNull(session)) {
            session.close();
        }

        if (nonNull(connection)) {
            connection.stop();
        }
    }

    @Nested
    class StartConsumers {

        @Test
        void shouldReturnSelf_ForFluentMethodChaining() {
            activeMqConfig.setConsumers(List.of("topic:dest1"));
            dropwizardActiveMq = newDropwizardActiveMq();

            var returnedValue = dropwizardActiveMq
                    .startConsumers(newMockActiveMqConsumer())
                    .startConsumer(newMockActiveMqConsumer(), "queue:destQ1", "queue:destQ2");

            assertThat(returnedValue).isSameAs(dropwizardActiveMq);
        }

        @Test
        void shouldStartSingleConsumer_ConfiguredInActiveMqConfig() {
            activeMqConfig.setConsumers(List.of("topic:dest1"));
            dropwizardActiveMq = newDropwizardActiveMq();

            var returnedValue = dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            assertThat(returnedValue).isSameAs(dropwizardActiveMq);
            assertThat(dropwizardActiveMq.getInitializedConsumers()).containsOnly("topic:dest1");

            verify(lifecycle).manage(isA(Consumer.class));
            verify(healthChecks).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
        }

        @Test
        void shouldStartMultipleConsumers_ConfiguredInActiveMqConfig() {
            activeMqConfig.setConsumers(List.of("topic:dest1", "topic:dest2"));
            dropwizardActiveMq = newDropwizardActiveMq();

            var returnedValue = dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            assertThat(returnedValue).isSameAs(dropwizardActiveMq);
            assertThat(dropwizardActiveMq.getInitializedConsumers()).containsOnly("topic:dest1", "topic:dest2");

            verify(lifecycle, times(2)).manage(isA(Consumer.class));
            verify(healthChecks).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
            verify(healthChecks).register(eq("consumer-topic:dest2"), isA(HealthCheck.class));
        }

        @Test
        void shouldStartToVarargsDestinations() {
            dropwizardActiveMq = newDropwizardActiveMq();

            var returnedValue = dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(),
                    "topic:dest1", "topic:dest2");

            assertThat(returnedValue).isSameAs(dropwizardActiveMq);
            assertThat(dropwizardActiveMq.getInitializedConsumers()).containsOnly("topic:dest1", "topic:dest2");

            verify(lifecycle, times(2)).manage(isA(Consumer.class));
            verify(healthChecks).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
            verify(healthChecks).register(eq("consumer-topic:dest2"), isA(HealthCheck.class));
        }

        @Test
        void shouldRequireVarargs_ToContain_AtLeastOneDestination() {
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> dropwizardActiveMq.startConsumer(newMockActiveMqConsumer()))
                    .withMessage("No destinations specified, which would result in no messages being consumed!");
        }

        @Test
        void shouldThrowIllegalState_WhenMultipleConsumersPerDestination_NotAllowed() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            // start one consumer
            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            // second should not be allowed
            assertThatIllegalStateException()
                    .isThrownBy(() -> dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(), destination))
                    .withMessage("A consumer for destination '%s' already exists", destination);

            verify(lifecycle).manage(isA(Consumer.class));
            verify(healthChecks).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
        }

        @Test
        void shouldStartSecondConsumer_WhenMultipleConsumersPerDestination_AreAllowed() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            activeMqConfig.setAllowMultipleConsumersPerDestination(true);
            dropwizardActiveMq = newDropwizardActiveMq();

            // start one consumer
            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            // second should be allowed
            dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(), destination);

            verify(lifecycle, times(2)).manage(isA(Consumer.class));
            verify(healthChecks, times(2)).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
        }

        @Test
        void shouldReturnFalse_fromIsConsumerConsuming_forUnknownDestination() {
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.isConsumerConsuming("topic:unknown")).isFalse();
        }

        @Test
        void shouldReturnFalse_fromIsConsumerConsuming_whenConsumerRegisteredButThreadNotYetStarted() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            // consumer is registered but Dropwizard has not called start() on the managed object yet
            assertThat(dropwizardActiveMq.isConsumerConsuming(destination)).isFalse();
        }

        @Test
        void shouldReturnTrue_fromIsConsumerConsuming_whenConsumerThreadIsRunning() throws Exception {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            var managedConsumer = captureConsumer();

            managedConsumer.start();
            try {
                await().atMost(Durations.TWO_SECONDS).until(
                        () -> dropwizardActiveMq.isConsumerConsuming(destination));

                assertThat(dropwizardActiveMq.isConsumerConsuming(destination)).isTrue();
            } finally {
                managedConsumer.stop();
            }
        }

        @Test
        void shouldReturnFalse_fromIsConsumerConsuming_afterConsumerStops() throws Exception {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            var managedConsumer = captureConsumer();

            managedConsumer.start();
            await().atMost(Durations.TWO_SECONDS).until(
                    () -> dropwizardActiveMq.isConsumerConsuming(destination));

            managedConsumer.stop();
            await().atMost(Durations.TWO_SECONDS).until(
                    () -> !dropwizardActiveMq.isConsumerConsuming(destination));

            assertThat(dropwizardActiveMq.isConsumerConsuming(destination)).isFalse();
        }

        @Test
        void shouldReturnTrue_fromIsConsumerConsuming_whenAtLeastOneOfMultipleConsumersIsRunning() throws Exception {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            activeMqConfig.setAllowMultipleConsumersPerDestination(true);
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());
            dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(), destination);

            var captor = ArgumentCaptor.forClass(Managed.class);
            verify(lifecycle, atLeastOnce()).manage(captor.capture());
            var consumers = captor.getAllValues().stream()
                    .filter(Consumer.class::isInstance)
                    .map(Consumer.class::cast)
                    .toList();

            assertThat(consumers).hasSize(2);

            var first = consumers.get(0);
            var second = consumers.get(1);

            first.start();
            try {
                await().atMost(Durations.TWO_SECONDS).until(
                        () -> dropwizardActiveMq.isConsumerConsuming(destination));

                // stop the first; second is not started — anyMatch should now return false
                first.stop();
                await().atMost(Durations.TWO_SECONDS).until(
                        () -> !dropwizardActiveMq.isConsumerConsuming(destination));

                assertThat(dropwizardActiveMq.isConsumerConsuming(destination)).isFalse();

                // start the second; should return true again
                second.start();
                await().atMost(Durations.TWO_SECONDS).until(
                        () -> dropwizardActiveMq.isConsumerConsuming(destination));

                assertThat(dropwizardActiveMq.isConsumerConsuming(destination)).isTrue();
            } finally {
                first.stop();
                second.stop();
            }
        }

        @Test
        void shouldReflectStartedConsumer_InHasConsumers_AndIsConsumerStarted() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.hasConsumersStarted()).isFalse();
            assertThat(dropwizardActiveMq.isConsumerStarted(destination)).isFalse();
            assertThat(dropwizardActiveMq.isConsumerStarted("topic:other")).isFalse();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            assertThat(dropwizardActiveMq.hasConsumersStarted()).isTrue();
            assertThat(dropwizardActiveMq.isConsumerStarted(destination)).isTrue();
            assertThat(dropwizardActiveMq.isConsumerStarted("topic:other")).isFalse();
        }

        private Consumer captureConsumer() {
            var captor = ArgumentCaptor.forClass(Managed.class);
            verify(lifecycle, atLeastOnce()).manage(captor.capture());
            return captor.getAllValues().stream()
                    .filter(Consumer.class::isInstance)
                    .map(Consumer.class::cast)
                    .findFirst()
                    .orElseThrow();
        }
    }

    private static MockActiveMqConsumer newMockActiveMqConsumer() {
        return MockActiveMqConsumer.builder().buildConsumer();
    }

    @Nested
    class StartProducers {

        @Test
        void shouldStartWhenNoDefaultProducer() {
            var producerDestinations = List.of("topic:dest1", "topic:dest2");
            activeMqConfig.setProducers(producerDestinations);
            activeMqConfig.setDefaultProducers(List.of());
            dropwizardActiveMq = newDropwizardActiveMq();

            var activeMqProducer = dropwizardActiveMq.startProducers();

            var delegate = assertIsExactType(activeMqProducer, ProducerDelegate.class);
            assertAll(
                    () -> assertThat(delegate.containsDestination(first(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDestination(second(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(activeMqConfig.getAllEventsQueue())).isTrue()
            );
        }

        @Test
        void shouldStartWithOneDefaultProducer() {
            var producerDestinations = List.of("topic:dest1", "topic:dest2");
            var defaultProducerDestinations = List.of("queue:defaultDest1");
            activeMqConfig.setProducers(producerDestinations);
            activeMqConfig.setDefaultProducers(defaultProducerDestinations);
            dropwizardActiveMq = newDropwizardActiveMq();

            var activeMqProducer = dropwizardActiveMq.startProducers();

            var delegate = assertIsExactType(activeMqProducer, ProducerDelegate.class);
            assertAll(
                    () -> assertThat(delegate.containsDestination(first(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDestination(second(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(first(defaultProducerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(activeMqConfig.getAllEventsQueue())).isFalse()
            );
        }

        @Test
        void shouldStartWhenSetDefaultProducerAsAllEvents() {
            var producerDestinations = List.of("topic:dest1", "topic:dest2");
            var defaultProducerDestinations = List.of(activeMqConfig.getAllEventsQueue());
            activeMqConfig.setProducers(producerDestinations);
            activeMqConfig.setDefaultProducers(defaultProducerDestinations);
            dropwizardActiveMq = newDropwizardActiveMq();

            var activeMqProducer = dropwizardActiveMq.startProducers();

            var delegate = assertIsExactType(activeMqProducer, ProducerDelegate.class);
            assertAll(
                    () -> assertThat(delegate.containsDestination(first(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDestination(second(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(activeMqConfig.getAllEventsQueue())).isTrue()
            );
        }

        @Test
        void shouldStartWhenMultipleDefaultProducers() {
            var producerDestinations = List.of("topic:dest1", "topic:dest2");
            var defaultProducerDestinations = List.of("queue:defaultDest1", "queue:defaultDest2");
            activeMqConfig.setProducers(producerDestinations);
            activeMqConfig.setDefaultProducers(defaultProducerDestinations);
            dropwizardActiveMq = newDropwizardActiveMq();

            var activeMqProducer = dropwizardActiveMq.startProducers();

            var delegate = assertIsExactType(activeMqProducer, ProducerDelegate.class);
            assertAll(
                    () -> assertThat(delegate.containsDestination(first(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDestination(second(producerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(first(defaultProducerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(second(defaultProducerDestinations))).isTrue(),
                    () -> assertThat(delegate.containsDefaultDestination(activeMqConfig.getAllEventsQueue())).isFalse()
            );
        }

        /**
         * Queues must be prefixed with "queue://" but topics can be prefixed with "topic://" or not.
         * <p>
         * Order should not matter.
         */
        @ParameterizedTest
        @ValueSource(strings = {
                "*:topic://topic-A,queue://queue-B",
                "*:queue://queue-B,topic://topic-A",
                "*:topic-A,queue://queue-B",
                "*:queue://queue-B,topic-A"
        })
        void shouldStartWhenProducingToDynamicDestinations(String dynamicDestination) {
            activeMqConfig.setAllowDynamicDestinations(true);
            activeMqConfig.setProducers(List.of());
            activeMqConfig.setDefaultProducers(List.of());
            dropwizardActiveMq = newDropwizardActiveMq();

            var listenerA = createTopicConsumerMessageListener(session, "topic-A");
            var listenerB = createQueueConsumerMessageListener(session, "queue-B");
            var listenerC = createQueueConsumerMessageListener(session, activeMqConfig.getAllEventsQueueName());

            var activeMqProducer = dropwizardActiveMq.startProducers();

            activeMqProducer.produceToDestinationAndAllEventsQueue(
                    dynamicDestination,
                    """
                            {
                                "messageType": "TESTING",
                                "value": 42
                            }
                            """);

            await().atMost(ONE_SECOND).until(() ->
                    listenerA.getCount() > 0 && listenerB.getCount() > 0 && listenerC.getCount() > 0);

            assertAll(
                    () -> assertThat(listenerA.getCount()).isOne(),
                    () -> assertThat(listenerB.getCount()).isOne(),
                    () -> assertThat(listenerC.getCount()).isOne()
            );
        }

        @Test
        void shouldThrowIllegalState_WhenStartProducers_CalledMoreThanOnce() {
            activeMqConfig.setProducers(List.of("topic:dest1"));
            activeMqConfig.setDefaultProducers(List.of());
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startProducers();

            assertThatIllegalStateException()
                    .isThrownBy(() -> dropwizardActiveMq.startProducers())
                    .withMessage("startProducers() has already been called; it should only be called once");
        }
    }

    @Nested
    class IsProducerStarted {

        @Test
        void shouldReturnFalse_BeforeStartProducers() {
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.isProducerStarted()).isFalse();
        }

        @Test
        void shouldReturnTrue_AfterStartProducers() {
            activeMqConfig.setProducers(List.of("topic:dest1"));
            activeMqConfig.setDefaultProducers(List.of());
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startProducers();

            assertThat(dropwizardActiveMq.isProducerStarted()).isTrue();
        }
    }

    @Nested
    class ConsumerCount {

        @Test
        void shouldReturnZero_WhenNoConsumersStarted() {
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.getConsumerCount()).isZero();
            assertThat(dropwizardActiveMq.getConsumerCountForDestination("topic:dest1")).isZero();
        }

        @Test
        void shouldReturnOne_WhenSingleConsumerStarted() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            assertThat(dropwizardActiveMq.getConsumerCount()).isOne();
            assertThat(dropwizardActiveMq.getConsumerCountForDestination(destination)).isOne();
            assertThat(dropwizardActiveMq.getConsumerCountForDestination("topic:other")).isZero();
        }

        @Test
        void shouldCountPerDestination_WhenMultipleConsumersAllowed() {
            var destination = "topic:dest1";
            activeMqConfig.setConsumers(List.of(destination));
            activeMqConfig.setAllowMultipleConsumersPerDestination(true);
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());
            dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(), destination);

            assertThat(dropwizardActiveMq.getConsumerCount()).isEqualTo(2);
            assertThat(dropwizardActiveMq.getConsumerCountForDestination(destination)).isEqualTo(2);
        }

        @Test
        void shouldCountAcrossMultipleDestinations() {
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumer(newMockActiveMqConsumer(), "topic:dest1", "topic:dest2", "topic:dest3");

            assertThat(dropwizardActiveMq.getConsumerCount()).isEqualTo(3);
            assertThat(dropwizardActiveMq.getConsumerCountForDestination("topic:dest1")).isOne();
            assertThat(dropwizardActiveMq.getConsumerCountForDestination("topic:dest2")).isOne();
            assertThat(dropwizardActiveMq.getConsumerCountForDestination("topic:dest3")).isOne();
        }
    }

    private DropwizardActiveMq<TestAppConfig> newDropwizardActiveMq() {
        return DropwizardActiveMq.<TestAppConfig>builder()
                .configuration(appConfig)
                .environment(environment)
                .registryAwareClient(registryAwareClient)
                .activeMqHelper(activeMqHelper)
                .build();
    }
}
