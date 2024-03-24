package org.kiwiproject.dropwizard.activemq.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.newTlsContextConfiguration;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThatHealthCheck;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.jaxrs.exception.JaxrsNotFoundException;

@DisplayName("DeadLetterQueueHealthCheck")
class DeadLetterQueueHealthCheckTest {

    private DeadLetterQueueHealthCheck healthCheck;
    private StatHelper statHelper;

    @BeforeEach
    void setUp() {
        statHelper = mock(StatHelper.class);
        healthCheck = new DeadLetterQueueHealthCheck(statHelper);
    }

    @Test
    void shouldConstruct() {
        var activeMqConfig = new ActiveMqConfig();
        activeMqConfig.setTlsConfiguration(newTlsContextConfiguration());
        var healthCheck = new DeadLetterQueueHealthCheck(activeMqConfig);

        assertThat(healthCheck).isNotNull();
    }

    @Test
    void shouldReturnQueueName() {
        assertThat(healthCheck.getQueueName()).isEqualTo("ActiveMQ.DLQ");
    }

    @Nested
    class ShouldBeHealthy {

        @Test
        void whenResultIsNull() {
            when(statHelper.getStatsSingleResultOrNull(anyString())).thenReturn(null);

            assertThatHealthCheck(healthCheck)
                    .isHealthy()
                    .hasMessage("No stats available for DLQ");

            verifyGetStatsMethodCall();
        }

        @Test
        void whenQueueSizeIsZero() {
            when(statHelper.getStatsSingleResultOrNull(anyString())).thenReturn(newJolokiaResponseValue(0L));

            assertThatHealthCheck(healthCheck)
                    .isHealthy()
                    .hasMessage("Dead-letter queue is empty");

            verifyGetStatsMethodCall();
        }

        @Test
        void whenTheDLQIsNotFound() {
            when(statHelper.getStatsSingleResultOrNull(anyString()))
                    .thenThrow(new JaxrsNotFoundException("Not Found"));

            assertThatHealthCheck(healthCheck)
                    .isHealthy()
                    .hasMessage("Dead-letter queue does not exist");

            verifyGetStatsMethodCall();
        }
    }

    @Nested
    class ShouldBeUnhealthy {

        @Test
        void whenQueueSizeIsNull() {
            when(statHelper.getStatsSingleResultOrNull(anyString())).thenReturn(newJolokiaResponseValue(null));

            assertThatHealthCheck(healthCheck)
                    .isUnhealthy()
                    .hasMessage("No QueueSize in response from ActiveMQ");

            verifyGetStatsMethodCall();
        }

        @ParameterizedTest
        @ValueSource(longs = { 1, 5, 15 })
        void whenQueueSizeIsGreaterThanZero(long queueSize) {
            when(statHelper.getStatsSingleResultOrNull(anyString())).thenReturn(newJolokiaResponseValue(queueSize));

            assertThatHealthCheck(healthCheck)
                    .isUnhealthy()
                    .hasMessage("Dead-letter queue contains messages. Current count: {}", queueSize);

            verifyGetStatsMethodCall();
        }
    }

    private static JolokiaResponseValue newJolokiaResponseValue(Long queueSize) {
        var value = new JolokiaResponseValue();
        value.setQueueSize(queueSize);
        return value;
    }

    private void verifyGetStatsMethodCall() {
        verify(statHelper).getStatsSingleResultOrNull("ActiveMQ.DLQ");
    }
}
