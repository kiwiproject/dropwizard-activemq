package org.kiwiproject.dropwizard.activemq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoPropertyViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertOnePropertyViolation;

import io.dropwizard.util.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@DisplayName("ActiveMqHealthConfig")
class ActiveMqHealthConfigTest {

    private ActiveMqHealthConfig config;

    @BeforeEach
    void setUp() {
        config = new ActiveMqHealthConfig();
    }

    @Nested
    class DefaultValues {

        @Test
        void shouldHaveExpectedDlqNameConstant() {
            assertThat(ActiveMqHealthConfig.DEFAULT_DLQ_NAME).isEqualTo("ActiveMQ.DLQ");
        }

        @Test
        void shouldHaveExpectedDefaults() {
            assertAll(
                    () -> assertThat(config.getJmxUser()).isNull(),
                    () -> assertThat(config.getJmxCred()).isNull(),
                    () -> assertThat(config.getIgnoredDestinations()).isEmpty(),
                    () -> assertThat(config.getMinConsumerThreshold()).isZero(),
                    () -> assertThat(config.getMaxPendingThreshold()).isEqualTo(100),
                    () -> assertThat(config.getRefreshInterval()).isEqualTo(Duration.minutes(2)),
                    () -> assertThat(config.isIgnoreEmptyQueuesWithNoConsumers()).isTrue(),
                    () -> assertThat(config.getStatsTimeout()).isEqualTo(Duration.seconds(10)),
                    () -> assertThat(config.getDlqName()).isEqualTo(ActiveMqHealthConfig.DEFAULT_DLQ_NAME)
            );
        }
    }

    @Nested
    class Validation {

        @Test
        void shouldPassValidation_WhenAllRequiredFieldsArePresent() {
            config.setJmxUser("admin");
            config.setJmxCred("secret");

            assertNoViolations(config);
        }

        @Nested
        class JmxUser {

            @ParameterizedTest
            @NullAndEmptySource
            @ValueSource(strings = {"  ", "\t"})
            void shouldFailValidation_WhenBlankOrNull(String value) {
                config.setJmxUser(value);

                assertOnePropertyViolation(config, "jmxUser");
            }

            @Test
            void shouldPassValidation_WhenNotBlank() {
                config.setJmxUser("admin");

                assertNoPropertyViolations(config, "jmxUser");
            }
        }

        @Nested
        class JmxCred {

            @ParameterizedTest
            @NullAndEmptySource
            @ValueSource(strings = {"  ", "\t"})
            void shouldFailValidation_WhenBlankOrNull(String value) {
                config.setJmxCred(value);

                assertOnePropertyViolation(config, "jmxCred");
            }

            @Test
            void shouldPassValidation_WhenNotBlank() {
                config.setJmxCred("secret");

                assertNoPropertyViolations(config, "jmxCred");
            }
        }

        @Nested
        class IgnoredDestinations {

            @Test
            void shouldFailValidation_WhenNull() {
                config.setIgnoredDestinations(null);

                assertOnePropertyViolation(config, "ignoredDestinations");
            }

            @Test
            void shouldPassValidation_WhenEmpty() {
                assertNoPropertyViolations(config, "ignoredDestinations");
            }
        }

        @Nested
        class RefreshInterval {

            @Test
            void shouldFailValidation_WhenNull() {
                config.setRefreshInterval(null);

                assertOnePropertyViolation(config, "refreshInterval");
            }

            @Test
            void shouldPassValidation_WhenNotNull() {
                assertNoPropertyViolations(config, "refreshInterval");
            }
        }

        @Nested
        class StatsTimeout {

            @Test
            void shouldFailValidation_WhenNull() {
                config.setStatsTimeout(null);

                assertOnePropertyViolation(config, "statsTimeout");
            }

            @Test
            void shouldPassValidation_WhenNotNull() {
                assertNoPropertyViolations(config, "statsTimeout");
            }
        }

        @Nested
        class DlqName {

            @ParameterizedTest
            @NullAndEmptySource
            @ValueSource(strings = {"  ", "\t"})
            void shouldFailValidation_WhenBlankOrNull(String value) {
                config.setDlqName(value);

                assertOnePropertyViolation(config, "dlqName");
            }

            @Test
            void shouldPassValidation_WhenDefault() {
                assertNoPropertyViolations(config, "dlqName");
            }

            @Test
            void shouldPassValidation_WhenCustomValue() {
                config.setDlqName("ActiveMQ.DLQ.myqueue");

                assertNoPropertyViolations(config, "dlqName");
            }
        }
    }
}
