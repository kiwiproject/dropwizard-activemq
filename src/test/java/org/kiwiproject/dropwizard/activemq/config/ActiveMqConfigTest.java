package org.kiwiproject.dropwizard.activemq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.setTlsConfigSystemProperties;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoPropertyViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertOnePropertyViolation;

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
                () -> assertThat(config.getJolokiaPort()).isEqualTo(8161),
                () -> assertThat(config.isUseSecureRestConnections()).isTrue(),
                () -> assertThat(config.isVerifyRestConnectionHostnames()).isFalse()
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
}
