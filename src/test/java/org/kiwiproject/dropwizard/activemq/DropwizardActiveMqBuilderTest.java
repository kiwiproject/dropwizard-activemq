package org.kiwiproject.dropwizard.activemq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import org.apache.activemq.jms.pool.PooledConnectionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.internal.BrokerHealthCheck;
import org.kiwiproject.jersey.client.RegistryAwareClient;

import java.util.List;
import java.util.SortedSet;

@DisplayName("DropwizardActiveMqBuilder")
class DropwizardActiveMqBuilderTest {

    private TestAppConfig appConfig;
    private ActiveMqConfig activeMqConfig;
    private Environment environment;
    private LifecycleEnvironment lifecycle;
    private HealthCheckRegistry healthChecks;
    private RegistryAwareClient registryAwareClient;
    private PooledConnectionFactory pooledFactory;
    private ActiveMqHelper activeMqHelper;
    private DropwizardActiveMq<TestAppConfig> dropwizardActiveMq;

    @BeforeEach
    void setUp() {
        activeMqConfig = new ActiveMqConfig();
        activeMqConfig.setTlsConfiguration(newTlsContextConfiguration());
        activeMqConfig.setBrokerUri("tcp://server.acme.com:61616");
        activeMqConfig.setHealthCheckNamePrefix("ACME");
        activeMqConfig.setConsumers(List.of("topic:STATUS_CHANGES"));
        activeMqConfig.setProducers(List.of("topic:STUFF", "topic:MORE_STUFF"));
        activeMqConfig.getDefaultProducers().add("queue:AUDIT");

        appConfig = TestAppConfig.builder()
                .activeMqConfig(activeMqConfig)
                .build();

        environment = mock(Environment.class);

        lifecycle = new LifecycleEnvironment(new MetricRegistry());
        when(environment.lifecycle()).thenReturn(lifecycle);

        healthChecks = new HealthCheckRegistry();
        when(environment.healthChecks()).thenReturn(healthChecks);

        registryAwareClient = mock(RegistryAwareClient.class);

        pooledFactory = mock(PooledConnectionFactory.class);
        activeMqHelper = mock(ActiveMqHelper.class);
        when(activeMqHelper.newPooledConnectionFactory(any(ActiveMqConfig.class))).thenReturn(pooledFactory);
    }

    @Nested
    class Always {

        @BeforeEach
        void setUp() {
            dropwizardActiveMq = newDropwizardActiveMq();
        }

        @Test
        void shouldManageActiveMQConnectionFactory() throws Exception {
            var managedObjects = lifecycle.getManagedObjects();
            assertThat(managedObjects).hasSize(1);

            var managed = first(managedObjects);
            managed.start();
            managed.stop();

            verify(pooledFactory).start();
            verify(pooledFactory).start();
        }

        @Test
        void shouldNotHaveInitializedConsumers_AfterCreation() {
            assertThat(dropwizardActiveMq.getInitializedConsumers()).isEmpty();
        }

        @Test
        void shouldNotHaveConsumersStarted_AfterCreation() {
            assertThat(dropwizardActiveMq.hasConsumersStarted()).isFalse();
        }

        @Test
        void shouldNotHaveInitializedActiveMqProducer_AfterCreation() {
            assertThat(dropwizardActiveMq.getActiveMqProducer()).isEmpty();
        }

        @Test
        void shouldNotHaveProducerStarted_AfterCreation() {
            assertThat(dropwizardActiveMq.isProducerStarted()).isFalse();
        }
    }

    @Nested
    class IsAllowMultipleConsumersPerDestination {

        @Test
        void shouldReturnFalse_ByDefault() {
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.isAllowMultipleConsumersPerDestination()).isFalse();
        }

        @Test
        void shouldReturnTrue_WhenConfigured() {
            activeMqConfig.setAllowMultipleConsumersPerDestination(true);
            dropwizardActiveMq = newDropwizardActiveMq();

            assertThat(dropwizardActiveMq.isAllowMultipleConsumersPerDestination()).isTrue();
        }
    }

    @Nested
    class ThrowsIllegalArgumentException {

        @Test
        void whenConfigurationNotSupplied() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> DropwizardActiveMq.<TestAppConfig>builder()
                            .environment(environment)
                            .registryAwareClient(registryAwareClient)
                            .activeMqHelper(activeMqHelper)
                            .build());
        }

        @Test
        void whenEnvironmentNotSupplied() {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> DropwizardActiveMq.<TestAppConfig>builder()
                            .configuration(appConfig)
                            .registryAwareClient(registryAwareClient)
                            .activeMqHelper(activeMqHelper)
                            .build());
        }

        @Test
        void whenElucidationEnabled_ButRegistryAwareClientNotSupplied() {
            assertThat(appConfig.isElucidationEnabled())
                    .describedAs("precondition failed: elucidationEnabled")
                    .isTrue();

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> DropwizardActiveMq.<TestAppConfig>builder()
                            .configuration(appConfig)
                            .environment(environment)
                            .activeMqHelper(activeMqHelper)
                            .build());
        }
    }

    @Nested
    class WithDefaults {

        @BeforeEach
        void setUp() {
            dropwizardActiveMq = newDropwizardActiveMq();
        }

        @Test
        void shouldRegisterBrokerHealthCheck() {
            var name = "ACME ActiveMQ Producer/Consumer";

            assertAll(
                    () -> assertThat(healthChecks.getNames()).contains(name),
                    () -> assertThat(healthChecks.getHealthCheck(name)).isExactlyInstanceOf(BrokerHealthCheck.class)
            );
        }

        @Test
        void shouldRegisterStatsHealthChecks() {
            assertThat(healthChecks.getNames()).contains(
                    "ACME ActiveMQ Consumer Stats",
                    "ACME ActiveMQ Producer Stats"
            );
        }

        @Test
        void shouldInitializeElucidation() {
            assertThat(dropwizardActiveMq.getElucidationContext()).isPresent();
        }
    }

    @Nested
    class WithCustomConfig {

        private SortedSet<String> healthCheckNames;

        @BeforeEach
        void setUp() {
            appConfig = TestAppConfig.builder()
                    .activeMqConfig(activeMqConfig)
                    .elucidationEnabled(false)
                    .build();

            appConfig.getActiveMqConfig().setRegisterBrokerHealthCheck(false);
            appConfig.getActiveMqConfig().setEnableStatsHealthChecks(false);

            dropwizardActiveMq = newDropwizardActiveMq();
            healthCheckNames = healthChecks.getNames();
        }

        @Test
        void shouldAllowSkippingBrokerHealthCheckRegistration() {
            assertThat(healthCheckNames).doesNotContain("ACME ActiveMQ Producer/Consumer");
        }

        @Test
        void shouldAllowSkippingStatsHealthCheckRegistration() {
            assertThat(healthChecks.getNames()).doesNotContain(
                    "ACME ActiveMQ Consumer Stats",
                    "ACME ActiveMQ Producer Stats"
            );
        }

        @Test
        void shouldAllowSkippingElucidationInitialization() {
            assertThat(dropwizardActiveMq.getElucidationContext()).isEmpty();
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
