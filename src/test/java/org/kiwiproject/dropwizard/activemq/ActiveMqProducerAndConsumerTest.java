package org.kiwiproject.dropwizard.activemq;

import static java.util.Objects.nonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.internal.Consumer;
import org.kiwiproject.dropwizard.activemq.test.mock.MockActiveMqConsumer;
import org.kiwiproject.dropwizard.activemq.test.util.ActiveMqTestUtils;
import org.kiwiproject.jersey.client.RegistryAwareClient;
import org.kiwiproject.test.dropwizard.mockito.DropwizardMockitoMocks;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import java.util.List;

@DisplayName("ActiveMqProducerAndConsumer")
class ActiveMqProducerAndConsumerTest {

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

        var brokerUrl = "vm://embedded?broker.brokerName=test-broker" +
                "&broker.persistent=false" +
                "&broker.useShutdownHook=false" +
                "&broker.useJmx=false" +
                "&broker.enableStatistics=false";

        var amqFactory = new ActiveMQConnectionFactory(brokerUrl);
        var pooledFactory = new ActiveMqHelper().newPooledConnectionFactory(amqFactory);

        connection = pooledFactory.createConnection();
        connection = amqFactory.createConnection();
        connection.start();

        session = ActiveMqTestUtils.createNonTransactedSession(connection);

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

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

            verify(lifecycle).manage(isA(Consumer.class));
            verify(healthChecks).register(eq("consumer-topic:dest1"), isA(HealthCheck.class));
        }

        @Test
        void shouldStartMultipleConsumers_ConfiguredInActiveMqConfig() {
            activeMqConfig.setConsumers(List.of("topic:dest1", "topic:dest2"));
            dropwizardActiveMq = newDropwizardActiveMq();

            dropwizardActiveMq.startConsumers(newMockActiveMqConsumer());

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
    }

    private static MockActiveMqConsumer newMockActiveMqConsumer() {
        return MockActiveMqConsumer.builder().buildConsumer();
    }

    @Nested
    class StartProducers {
        // TODO
    }

    // TODO

    private DropwizardActiveMq<TestAppConfig> newDropwizardActiveMq() {
        return DropwizardActiveMq.<TestAppConfig>builder()
                .configuration(appConfig)
                .environment(environment)
                .registryAwareClient(registryAwareClient)
                .activeMqHelper(activeMqHelper)
                .build();
    }
}
