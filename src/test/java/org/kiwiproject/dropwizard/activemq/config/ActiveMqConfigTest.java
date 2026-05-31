package org.kiwiproject.dropwizard.activemq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.setTlsConfigSystemProperties;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoPropertyViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertOnePropertyViolation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.util.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.RestoreSystemProperties;
import org.kiwiproject.config.TlsContextConfiguration;
import org.kiwiproject.config.provider.TlsConfigProvider;
import org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory;
import org.kiwiproject.validation.KiwiValidations;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

@DisplayName("ActiveMqConfig")
class ActiveMqConfigTest {

    private ActiveMqConfig config;

    @BeforeEach
    void setUp() {
        config = new ActiveMqConfig();
    }

    @Nested
class DefaultValues {

    @Test
    void shouldHaveExpectedDefaults() {
        assertAll(
                () -> assertThat(config.getBrokerUri()).isEqualTo(ActiveMqConfig.DEFAULT_BROKER_URI),
                () -> assertThat(config.isRegisterBrokerHealthCheck()).isTrue(),
                () -> assertThat(config.getBrokerHealthCheckConsumerReceiveTimeout()).isEqualTo(Duration.milliseconds(400)),
                () -> assertThat(config.getHealthCheckNamePrefix()).isNull(),
                () -> assertThat(config.isEnableStatsHealthChecks()).isTrue(),
                () -> assertThat(config.isEnableElucidation()).isFalse(),
                () -> assertThat(config.getDestinationNormalizers()).isEmpty(),
                () -> assertThat(config.isAutoRegisterConsumers()).isTrue(),
                () -> assertThat(config.getConsumers()).isEmpty(),
                () -> assertThat(config.getConsumerReceiveTimeout()).isEqualTo(Duration.milliseconds(400)),
                () -> assertThat(config.getProducers()).isEmpty(),
                () -> assertThat(config.getDefaultProducers()).isEmpty(),
                () -> assertThat(config.isAllowDynamicDestinations()).isFalse(),
                () -> assertThat(config.isAllowMultipleConsumersPerDestination()).isFalse(),
                () -> assertThat(config.getTimeToLive()).isEqualTo(Duration.hours(1)),
                () -> assertThat(config.getHealthConfig()).isNotNull(),
                () -> assertThat(config.isUseSecureActiveMQConnections()).isTrue(),
                () -> assertThat(config.isVerifyActiveMQBrokerHostnames()).isTrue(),
                () -> assertThat(config.getAllEventsQueueName()).isEqualTo(ActiveMqConfig.DEFAULT_ALL_EVENTS_QUEUE_NAME),
                () -> assertThat(config.getAllEventsQueue()).isEqualTo("queue:" + ActiveMqConfig.DEFAULT_ALL_EVENTS_QUEUE_NAME),
                () -> assertThat(config.getJolokiaPort()).isEqualTo(8161),
                () -> assertThat(config.isUseSecureRestConnections()).isTrue(),
                () -> assertThat(config.isVerifyRestConnectionHostnames()).isTrue()
        );
    }
}

@Nested
class Validation {

    @Nested
    class BrokerUri {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"  ", "\t"})
        void shouldFailValidation_WhenBlankOrNull(String value) {
            config.setBrokerUri(value);

            assertOnePropertyViolation(config, "brokerUri");
        }

        @Test
        void shouldPassValidation_WhenNotBlank() {
            assertNoPropertyViolations(config, "brokerUri");
        }
    }

    @Nested
    class AllEventsQueueName {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"  ", "\t"})
        void shouldFailValidation_WhenBlankOrNull(String value) {
            config.setAllEventsQueueName(value);

            assertOnePropertyViolation(config, "allEventsQueueName");
        }

        @Test
        void shouldPassValidation_WhenNotBlank() {
            assertNoPropertyViolations(config, "allEventsQueueName");
        }
    }

    @Nested
    class BrokerHealthCheckConsumerReceiveTimeout {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setBrokerHealthCheckConsumerReceiveTimeout(null);

            assertOnePropertyViolation(config, "brokerHealthCheckConsumerReceiveTimeout");
        }

        @Test
        void shouldFailValidation_WhenBelowMinimum() {
            config.setBrokerHealthCheckConsumerReceiveTimeout(Duration.milliseconds(9));

            assertOnePropertyViolation(config, "brokerHealthCheckConsumerReceiveTimeout");
        }

        @Test
        void shouldPassValidation_WhenAtMinimum() {
            config.setBrokerHealthCheckConsumerReceiveTimeout(Duration.milliseconds(10));

            assertNoPropertyViolations(config, "brokerHealthCheckConsumerReceiveTimeout");
        }
    }

    @Nested
    class ConsumerReceiveTimeout {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setConsumerReceiveTimeout(null);

            assertOnePropertyViolation(config, "consumerReceiveTimeout");
        }

        @Test
        void shouldFailValidation_WhenBelowMinimum() {
            config.setConsumerReceiveTimeout(Duration.milliseconds(9));

            assertOnePropertyViolation(config, "consumerReceiveTimeout");
        }

        @Test
        void shouldPassValidation_WhenAtMinimum() {
            config.setConsumerReceiveTimeout(Duration.milliseconds(10));

            assertNoPropertyViolations(config, "consumerReceiveTimeout");
        }
    }

    @Nested
    class Consumers {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setConsumers(null);

            assertOnePropertyViolation(config, "consumers");
        }

        @Test
        void shouldPassValidation_WhenEmpty() {
            assertNoPropertyViolations(config, "consumers");
        }
    }

    @Nested
    class Producers {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setProducers(null);

            assertOnePropertyViolation(config, "producers");
        }

        @Test
        void shouldPassValidation_WhenEmpty() {
            assertNoPropertyViolations(config, "producers");
        }
    }

    @Nested
    class DefaultProducers {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setDefaultProducers(null);

            assertOnePropertyViolation(config, "defaultProducers");
        }

        @Test
        void shouldPassValidation_WhenEmpty() {
            assertNoPropertyViolations(config, "defaultProducers");
        }
    }

    @Nested
    class DestinationNormalizers {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setDestinationNormalizers(null);

            assertOnePropertyViolation(config, "destinationNormalizers");
        }

        @Test
        void shouldPassValidation_WhenEmpty() {
            assertNoPropertyViolations(config, "destinationNormalizers");
        }

        @Test
        void shouldCascadeValidation_WhenNormalizerHasBlankPattern() {
            var normalizer = new DestinationNormalizerConfig();
            normalizer.setPattern("  ");
            normalizer.setReplacement("$1.##");
            config.setDestinationNormalizers(List.of(normalizer));

            var violations = KiwiValidations.validate(config);
            assertThat(violations)
                    .extracting(v -> v.getPropertyPath().toString())
                    .contains("destinationNormalizers[0].pattern");
        }

        @Test
        void shouldCascadeValidation_WhenNormalizerHasInvalidPattern() {
            var normalizer = new DestinationNormalizerConfig();
            normalizer.setPattern("[invalid");
            normalizer.setReplacement("$1.##");
            config.setDestinationNormalizers(List.of(normalizer));

            var violations = KiwiValidations.validate(config);
            assertThat(violations)
                    .extracting(v -> v.getPropertyPath().toString())
                    .contains("destinationNormalizers[0].patternValid");
        }
    }

    @Nested
    class TimeToLive {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setTimeToLive(null);

            assertOnePropertyViolation(config, "timeToLive");
        }

        @Test
        void shouldPassValidation_WhenDefault() {
            assertNoPropertyViolations(config, "timeToLive");
        }

        @Test
        void shouldPassValidation_WhenZero() {
            config.setTimeToLive(Duration.milliseconds(0));

            assertNoPropertyViolations(config, "timeToLive");
        }
    }

    @Nested
    class HealthConfig {

        @Test
        void shouldFailValidation_WhenNull() {
            config.setHealthConfig(null);

            assertOnePropertyViolation(config, "healthConfig");
        }

        @Test
        void shouldPassValidation_WhenNotNull() {
            assertNoPropertyViolations(config, "healthConfig");
        }

        @Test
        void shouldCascadeValidation_WhenHealthConfig_HasViolations() {
            config.getHealthConfig().setDlqName(null);

            var violations = KiwiValidations.validate(config);
            assertThat(violations)
                    .extracting(v -> v.getPropertyPath().toString())
                    .contains("healthConfig.dlqName");
        }
    }

    @Nested
    class JolokiaPort {

        @Test
        void shouldFailValidation_WhenNegative() {
            config.setJolokiaPort(-1);

            assertOnePropertyViolation(config, "jolokiaPort");
        }

        @Test
        void shouldPassValidation_WhenZero() {
            config.setJolokiaPort(0);

            assertNoPropertyViolations(config, "jolokiaPort");
        }

        @Test
        void shouldPassValidation_WhenPositive() {
            assertNoPropertyViolations(config, "jolokiaPort");
        }
    }
}

    @Test
    void shouldProvideNonNull_ButDefault_TlsContextConfiguration_WhenProviderCannotProvide() {
        assertThat(TlsConfigProvider.builder().build().canProvide())
                .describedAs("precondition failed: expected TlsConfigProvider not to able able to provide")
                .isFalse();

        var tlsConfiguration = config.getTlsConfiguration();
        assertThat(tlsConfiguration).isNotNull();

        assertAll(
                () -> assertThat(tlsConfiguration.getKeyStorePath()).isNull(),
                () -> assertThat(tlsConfiguration.getKeyStorePassword()).isNull(),
                () -> assertThat(tlsConfiguration.getTrustStorePath()).isNull(),
                () -> assertThat(tlsConfiguration.getTrustStorePassword()).isNull()
        );
    }

    @Test
    @RestoreSystemProperties
    void shouldProvideValid_TlsConfiguration_WhenProviderCanProvide() {
        setTlsConfigSystemProperties();

        assertThat(TlsConfigProvider.builder().build().canProvide())
                .describedAs("precondition failed: expected TlsConfigProvider to able able to provide")
                .isTrue();

        // Create a new instance which will initialize using the provider.
        config = new ActiveMqConfig();

        var tlsConfiguration = config.getTlsConfiguration();
        assertThat(tlsConfiguration).isNotNull();

        var expectedTlsConfiguration = TestObjectFactory.newTlsContextConfiguration();
        assertThat(tlsConfiguration).usingRecursiveComparison().isEqualTo(expectedTlsConfiguration);
    }

    @Nested
    class IsTlsConfigurationValid {

        @ParameterizedTest(name = "[{index}] secureAMQ: {0}, secureRest: {1}, ssl: {2} -> should be {3}")
        @MethodSource("org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigTest#tlsConfigurationArgumentFactory")
        void shouldReturnExpectedResult(boolean useSecureAmqConnections,
                                        boolean useSecureRestConnections,
                                        boolean tlsConfigExists,
                                        boolean expectedResult) {

            config.setUseSecureActiveMQConnections(useSecureAmqConnections);
            config.setUseSecureRestConnections(useSecureRestConnections);
            config.setTlsConfiguration(tlsConfigExists ? newTlsContextConfiguration() : null);

            assertThat(config.isTlsConfigurationValid()).isEqualTo(expectedResult);
        }

        @Test
        void shouldBeFalse_WhenTlsConfiguration_IsNotValid() {
            config.setUseSecureActiveMQConnections(true);
            config.setUseSecureRestConnections(true);
            config.setTlsConfiguration(new TlsContextConfiguration());

            assertThat(config.isTlsConfigurationValid()).isFalse();
        }
    }

    // each Object[] contains: useSecureAmqConnections, useSecureRestConnections, tlsConfigExists, expectedResult
    @SuppressWarnings("unused")
    private static Stream<Object[]> tlsConfigurationArgumentFactory() {
        return Stream.of(
                new Object[] { true, true, true, true },
                new Object[] { true, true, false, false },
                new Object[] { true, false, true, true },
                new Object[] { true, false, false, false },
                new Object[] { false, true, true, true },
                new Object[] { false, true, false, false },
                new Object[] { false, false, true, true },
                new Object[] { false, false, false, true }
        );
    }

    @Nested
    class IsBrokerUriForSslProbablyValid {

        @ParameterizedTest
        @CsvSource(textBlock = """
                false, tcp://host1.prod:61616, true
                false, 'failover:(tcp://host1.prod:61616,tcp://host2.prod:61616)?randomize=false', true
                false, 'ssl://host1.prod:61617', false
                false, 'ssl://host1.prod:61617?verifyHostName=false', false
                false, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false&nested.verifyHostName=false', false

                true, tcp://host1.prod:61616, false
                true, 'failover:(tcp://host1.prod:61616,tcp://host2.prod:61616)?randomize=false', false
                true, 'ssl://host1.prod:61617', true
                true, 'ssl://host1.prod:61617?verifyHostName=false', true
                true, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false&nested.verifyHostName=false', true
                """)
        void shouldReturnExpectedResult(boolean useSecureActiveMQConnections,
                                        String brokerUri,
                                        boolean expectedResult) {

            config.setUseSecureActiveMQConnections(useSecureActiveMQConnections);
            config.setBrokerUri(brokerUri);

            assertThat(config.isBrokerUriForSslProbablyValid()).isEqualTo(expectedResult);
        }
    }

    @Nested
    class IsVerifyActiveMQBrokerHostnamesConsistent {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"  "})
        void shouldReturnTrue_WhenBrokerUriIsBlank(String brokerUri) {
            config.setBrokerUri(brokerUri);

            assertThat(config.isVerifyActiveMQBrokerHostnamesConsistent()).isTrue();
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                false, ssl://host1.prod:61617, true
                false, ssl://host1.prod:61617?verifyHostName=false, true
                false, ssl://host1.prod:61617?verifyHostName=true, false
                false, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)', true
                false, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=false', true
                false, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=true', false
                true, ssl://host1.prod:61617, true
                true, ssl://host1.prod:61617?verifyHostName=true, true
                true, ssl://host1.prod:61617?verifyHostName=false, false
                true, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)', true
                true, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=true', true
                true, 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=false', false
                """)
        void shouldReturnExpectedResult(boolean verifyActiveMQBrokerHostnames,
                                        String brokerUri,
                                        boolean expectedResult) {
            config.setVerifyActiveMQBrokerHostnames(verifyActiveMQBrokerHostnames);
            config.setBrokerUri(brokerUri);

            assertThat(config.isVerifyActiveMQBrokerHostnamesConsistent()).isEqualTo(expectedResult);
        }
    }

    private static final ObjectMapper YAML_MAPPER = Jackson.newObjectMapper().copyWith(new YAMLFactory());

    private static ActiveMqConfig loadYamlConfig(String filename) throws IOException {
        return YAML_MAPPER.readValue(
                Path.of("src/test/resources/ActiveMqConfigTest", filename).toFile(),
                ActiveMqConfig.class);
    }

    @Nested
    class Builder {

        @Test
        void shouldBuildWithDefaults() {
            var built = ActiveMqConfig.builder().build();
            var defaulted = new ActiveMqConfig();

            assertAll(
                    () -> assertThat(built.getBrokerUri()).isEqualTo(defaulted.getBrokerUri()),
                    () -> assertThat(built.isRegisterBrokerHealthCheck()).isEqualTo(defaulted.isRegisterBrokerHealthCheck()),
                    () -> assertThat(built.getBrokerHealthCheckConsumerReceiveTimeout()).isEqualTo(defaulted.getBrokerHealthCheckConsumerReceiveTimeout()),
                    () -> assertThat(built.getHealthCheckNamePrefix()).isEqualTo(defaulted.getHealthCheckNamePrefix()),
                    () -> assertThat(built.isEnableStatsHealthChecks()).isEqualTo(defaulted.isEnableStatsHealthChecks()),
                    () -> assertThat(built.isEnableElucidation()).isEqualTo(defaulted.isEnableElucidation()),
                    () -> assertThat(built.getDestinationNormalizers()).isEmpty(),
                    () -> assertThat(built.isAutoRegisterConsumers()).isEqualTo(defaulted.isAutoRegisterConsumers()),
                    () -> assertThat(built.getConsumers()).isEmpty(),
                    () -> assertThat(built.getConsumerReceiveTimeout()).isEqualTo(defaulted.getConsumerReceiveTimeout()),
                    () -> assertThat(built.getProducers()).isEmpty(),
                    () -> assertThat(built.getDefaultProducers()).isEmpty(),
                    () -> assertThat(built.getAllEventsQueueName()).isEqualTo(defaulted.getAllEventsQueueName()),
                    () -> assertThat(built.isAllowDynamicDestinations()).isEqualTo(defaulted.isAllowDynamicDestinations()),
                    () -> assertThat(built.isAllowMultipleConsumersPerDestination()).isEqualTo(defaulted.isAllowMultipleConsumersPerDestination()),
                    () -> assertThat(built.getTimeToLive()).isEqualTo(defaulted.getTimeToLive()),
                    () -> assertThat(built.getHealthConfig()).usingRecursiveComparison().isEqualTo(defaulted.getHealthConfig()),
                    () -> assertThat(built.isUseSecureActiveMQConnections()).isEqualTo(defaulted.isUseSecureActiveMQConnections()),
                    () -> assertThat(built.isVerifyActiveMQBrokerHostnames()).isEqualTo(defaulted.isVerifyActiveMQBrokerHostnames()),
                    () -> assertThat(built.getJolokiaPort()).isEqualTo(defaulted.getJolokiaPort()),
                    () -> assertThat(built.isUseSecureRestConnections()).isEqualTo(defaulted.isUseSecureRestConnections()),
                    () -> assertThat(built.isVerifyRestConnectionHostnames()).isEqualTo(defaulted.isVerifyRestConnectionHostnames()),
                    () -> assertThat(built.getTlsConfiguration()).isNotNull()
            );
        }

        @Test
        @RestoreSystemProperties
        void shouldBuildWithExplicitValues() {
            setTlsConfigSystemProperties();
            var tls = newTlsContextConfiguration();
            var healthConfig = ActiveMqHealthConfig.builder()
                    .jmxUser("monitor")
                    .jmxCred("secret")
                    .build();
            var normalizer = new DestinationNormalizerConfig();
            normalizer.setPattern("(myapp\\.group).*");
            normalizer.setReplacement("$1.##");

            var built = ActiveMqConfig.builder()
                    .brokerUri("ssl://broker.example.com:61617")
                    .registerBrokerHealthCheck(false)
                    .brokerHealthCheckConsumerReceiveTimeout(Duration.milliseconds(200))
                    .healthCheckNamePrefix("internal")
                    .enableStatsHealthChecks(false)
                    .enableElucidation(true)
                    .destinationNormalizers(List.of(normalizer))
                    .autoRegisterConsumers(false)
                    .consumers(List.of("queue:orders"))
                    .consumerReceiveTimeout(Duration.milliseconds(300))
                    .producers(List.of("queue:notifications"))
                    .defaultProducers(List.of("queue:all_events"))
                    .allEventsQueueName("all_events_custom")
                    .allowDynamicDestinations(true)
                    .allowMultipleConsumersPerDestination(true)
                    .timeToLive(Duration.minutes(30))
                    .healthConfig(healthConfig)
                    .useSecureActiveMQConnections(true)
                    .verifyActiveMQBrokerHostnames(false)
                    .jolokiaPort(9161)
                    .useSecureRestConnections(true)
                    .verifyRestConnectionHostnames(false)
                    .tlsConfiguration(tls)
                    .build();

            assertAll(
                    () -> assertThat(built.getBrokerUri()).isEqualTo("ssl://broker.example.com:61617"),
                    () -> assertThat(built.isRegisterBrokerHealthCheck()).isFalse(),
                    () -> assertThat(built.getBrokerHealthCheckConsumerReceiveTimeout()).isEqualTo(Duration.milliseconds(200)),
                    () -> assertThat(built.getHealthCheckNamePrefix()).isEqualTo("internal"),
                    () -> assertThat(built.isEnableStatsHealthChecks()).isFalse(),
                    () -> assertThat(built.isEnableElucidation()).isTrue(),
                    () -> assertThat(built.getDestinationNormalizers()).hasSize(1),
                    () -> assertThat(built.isAutoRegisterConsumers()).isFalse(),
                    () -> assertThat(built.getConsumers()).containsExactly("queue:orders"),
                    () -> assertThat(built.getConsumerReceiveTimeout()).isEqualTo(Duration.milliseconds(300)),
                    () -> assertThat(built.getProducers()).containsExactly("queue:notifications"),
                    () -> assertThat(built.getDefaultProducers()).containsExactly("queue:all_events"),
                    () -> assertThat(built.getAllEventsQueueName()).isEqualTo("all_events_custom"),
                    () -> assertThat(built.isAllowDynamicDestinations()).isTrue(),
                    () -> assertThat(built.isAllowMultipleConsumersPerDestination()).isTrue(),
                    () -> assertThat(built.getTimeToLive()).isEqualTo(Duration.minutes(30)),
                    () -> assertThat(built.getHealthConfig()).isSameAs(healthConfig),
                    () -> assertThat(built.isUseSecureActiveMQConnections()).isTrue(),
                    () -> assertThat(built.isVerifyActiveMQBrokerHostnames()).isFalse(),
                    () -> assertThat(built.getJolokiaPort()).isEqualTo(9161),
                    () -> assertThat(built.isUseSecureRestConnections()).isTrue(),
                    () -> assertThat(built.isVerifyRestConnectionHostnames()).isFalse(),
                    () -> assertThat(built.getTlsConfiguration()).isSameAs(tls)
            );
        }

        @Test
        @RestoreSystemProperties
        void shouldPassBeanValidation_WhenAllRequiredFieldsAreSet() {
            setTlsConfigSystemProperties();

            var healthConfig = ActiveMqHealthConfig.builder()
                    .jmxUser("admin")
                    .jmxCred("secret")
                    .build();
            var built = ActiveMqConfig.builder()
                    .brokerUri("ssl://broker.example.com:61617")
                    .consumers(List.of("queue:orders"))
                    .healthConfig(healthConfig)
                    .build();

            var violations = KiwiValidations.validate(built);
            assertThat(violations).isEmpty();
        }
    }

    @Nested
    class YamlDeserialization {

        @Test
        void shouldDeserialize_WithConsumersOnly() throws IOException {
            var loaded = loadYamlConfig("minimal-consumers-only.yml");

            assertAll(
                    () -> assertThat(loaded.getBrokerUri()).isEqualTo("tcp://localhost:61616"),
                    () -> assertThat(loaded.getConsumers()).containsExactly("queue:orders"),
                    () -> assertThat(loaded.getProducers()).isEmpty(),
                    () -> assertThat(loaded.isUseSecureActiveMQConnections()).isFalse(),
                    () -> assertThat(loaded.isUseSecureRestConnections()).isFalse(),
                    () -> assertThat(loaded.getHealthConfig().getJmxUser()).isEqualTo("admin"),
                    () -> assertThat(loaded.getHealthConfig().getJmxCred()).isEqualTo("secret"),
                    () -> assertThat(loaded.isRegisterBrokerHealthCheck()).isTrue(),
                    () -> assertThat(loaded.isEnableStatsHealthChecks()).isTrue(),
                    () -> assertThat(loaded.getTimeToLive()).isEqualTo(Duration.hours(1))
            );
        }

        @Test
        void shouldDeserialize_WithProducersOnly() throws IOException {
            var loaded = loadYamlConfig("minimal-producers-only.yml");

            assertAll(
                    () -> assertThat(loaded.getBrokerUri()).isEqualTo("tcp://localhost:61616"),
                    () -> assertThat(loaded.getConsumers()).isEmpty(),
                    () -> assertThat(loaded.getProducers()).containsExactly("queue:notifications"),
                    () -> assertThat(loaded.isUseSecureActiveMQConnections()).isFalse(),
                    () -> assertThat(loaded.isUseSecureRestConnections()).isFalse(),
                    () -> assertThat(loaded.getHealthConfig().getJmxUser()).isEqualTo("admin"),
                    () -> assertThat(loaded.getHealthConfig().getJmxCred()).isEqualTo("secret")
            );
        }

        @Test
        void shouldDeserialize_WithConsumersAndProducers() throws IOException {
            var loaded = loadYamlConfig("minimal-consumers-and-producers.yml");

            assertAll(
                    () -> assertThat(loaded.getBrokerUri()).isEqualTo("tcp://localhost:61616"),
                    () -> assertThat(loaded.getConsumers()).containsExactly("queue:orders", "topic:events"),
                    () -> assertThat(loaded.getProducers()).containsExactly("queue:notifications"),
                    () -> assertThat(loaded.isUseSecureActiveMQConnections()).isFalse(),
                    () -> assertThat(loaded.isUseSecureRestConnections()).isFalse(),
                    () -> assertThat(loaded.getHealthConfig().getJmxUser()).isEqualTo("admin"),
                    () -> assertThat(loaded.getHealthConfig().getJmxCred()).isEqualTo("secret")
            );
        }

        @Test
        void shouldDeserialize_FullConfig() throws IOException {
            var loaded = loadYamlConfig("full-config.yml");

            assertAll(
                    () -> assertThat(loaded.getBrokerUri()).isEqualTo("ssl://broker.example.com:61617"),
                    () -> assertThat(loaded.isRegisterBrokerHealthCheck()).isFalse(),
                    () -> assertThat(loaded.getBrokerHealthCheckConsumerReceiveTimeout()).isEqualTo(Duration.milliseconds(200)),
                    () -> assertThat(loaded.getHealthCheckNamePrefix()).isEqualTo("internal"),
                    () -> assertThat(loaded.isEnableStatsHealthChecks()).isFalse(),
                    () -> assertThat(loaded.isEnableElucidation()).isTrue(),
                    () -> assertThat(loaded.isAutoRegisterConsumers()).isFalse(),
                    () -> assertThat(loaded.getConsumers()).containsExactly("queue:orders", "topic:events"),
                    () -> assertThat(loaded.getProducers()).containsExactly("queue:notifications"),
                    () -> assertThat(loaded.getDefaultProducers()).containsExactly("queue:all_events"),
                    () -> assertThat(loaded.getAllEventsQueueName()).isEqualTo("all_events_custom"),
                    () -> assertThat(loaded.isAllowDynamicDestinations()).isTrue(),
                    () -> assertThat(loaded.isAllowMultipleConsumersPerDestination()).isTrue(),
                    () -> assertThat(loaded.getTimeToLive()).isEqualTo(Duration.minutes(30)),
                    () -> assertThat(loaded.isUseSecureActiveMQConnections()).isTrue(),
                    () -> assertThat(loaded.isVerifyActiveMQBrokerHostnames()).isFalse(),
                    () -> assertThat(loaded.getJolokiaPort()).isEqualTo(9161),
                    () -> assertThat(loaded.isUseSecureRestConnections()).isTrue(),
                    () -> assertThat(loaded.isVerifyRestConnectionHostnames()).isFalse(),
                    () -> assertThat(loaded.getDestinationNormalizers()).satisfiesExactly(
                            n -> {
                                assertThat(n.getPattern()).isEqualTo("(myapp\\.group).*");
                                assertThat(n.getReplacement()).isEqualTo("$1.##");
                            }),
                    () -> assertThat(loaded.getHealthConfig().getJmxUser()).isEqualTo("monitor"),
                    () -> assertThat(loaded.getHealthConfig().getJmxCred()).isEqualTo("s3cr3t"),
                    () -> assertThat(loaded.getHealthConfig().getIgnoredDestinations()).containsExactly("queue:ignored-one"),
                    () -> assertThat(loaded.getHealthConfig().getMinConsumerThreshold()).isEqualTo(2),
                    () -> assertThat(loaded.getHealthConfig().getMaxPendingThreshold()).isEqualTo(500),
                    () -> assertThat(loaded.getHealthConfig().getRefreshInterval()).isEqualTo(Duration.minutes(5)),
                    () -> assertThat(loaded.getHealthConfig().isIgnoreEmptyQueuesWithNoConsumers()).isFalse(),
                    () -> assertThat(loaded.getHealthConfig().getStatsTimeout()).isEqualTo(Duration.seconds(30)),
                    () -> assertThat(loaded.getHealthConfig().getDlqName()).isEqualTo("ActiveMQ.DLQ.custom"),
                    () -> assertThat(loaded.getTlsConfiguration().getKeyStorePath()).isEqualTo("/test/keystore.p12"),
                    () -> assertThat(loaded.getTlsConfiguration().getKeyStorePassword()).isEqualTo("kspass"),
                    () -> assertThat(loaded.getTlsConfiguration().getKeyStoreType()).isEqualTo("PKCS12"),
                    () -> assertThat(loaded.getTlsConfiguration().getTrustStorePath()).isEqualTo("/test/truststore.p12"),
                    () -> assertThat(loaded.getTlsConfiguration().getTrustStorePassword()).isEqualTo("tspass"),
                    () -> assertThat(loaded.getTlsConfiguration().getTrustStoreType()).isEqualTo("PKCS12"),
                    () -> assertThat(loaded.getTlsConfiguration().isVerifyHostname()).isFalse()
            );
        }
    }

    @Nested
    class GetResolvedBrokerUri {

        @ParameterizedTest
        @ValueSource(strings = {
                "ssl://host1.prod:61617",
                "failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false"
        })
        void shouldReturnBrokerUri_WhenVerifyActiveMQBrokerHostnamesIsTrue(String brokerUri) {
            config.setBrokerUri(brokerUri);
            config.setVerifyActiveMQBrokerHostnames(true);

            assertThat(config.getResolvedBrokerUri()).isEqualTo(brokerUri);
        }

        @ParameterizedTest
        @CsvSource(textBlock = """
                ssl://host1.prod:61617, ssl://host1.prod:61617?verifyHostName=false
                ssl://host1.prod:61617?someParam=true, ssl://host1.prod:61617?someParam=true&verifyHostName=false
                ssl://host1.prod:61617?verifyHostName=false, ssl://host1.prod:61617?verifyHostName=false
                'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)', 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=false'
                'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false', 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?randomize=false&nested.verifyHostName=false'
                'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=false', 'failover:(ssl://host1.prod:61617,ssl://host2.prod:61617)?nested.verifyHostName=false'
                """)
        void shouldAppendVerifyHostName_WhenVerifyActiveMQBrokerHostnamesIsFalse(String brokerUri, String expectedUri) {
            config.setBrokerUri(brokerUri);
            config.setVerifyActiveMQBrokerHostnames(false);

            assertThat(config.getResolvedBrokerUri()).isEqualTo(expectedUri);
        }

        @Test
        void shouldCacheResolvedBrokerUri() {
            config.setBrokerUri("ssl://host1.prod:61617");
            config.setVerifyActiveMQBrokerHostnames(false);

            var first = config.getResolvedBrokerUri();
            var second = config.getResolvedBrokerUri();

            assertThat(first).isSameAs(second);
        }
    }
}
