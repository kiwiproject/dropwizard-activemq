package org.kiwiproject.dropwizard.activemq.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.test.assertj.dropwizard.metrics.HealthCheckResultAssertions.assertThatResult;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.health.HealthCheck;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import org.apache.commons.collections4.MapUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.activemq.TestAppConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.test.constants.KiwiTestConstants;
import org.kiwiproject.test.junit.jupiter.AsyncModeDisablingExtension;
import org.kiwiproject.test.util.Fixtures;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@DisplayName("StatsHealthCheck")
@ExtendWith(AsyncModeDisablingExtension.class)
class StatsHealthCheckTest {

    private static final String DEVELOPMENT_SERVERS =
            "failover(tcp://unit-test-1:61616,tcp://unit-test-2:61616)?randomize=false";
    private static final String HEALTHY_MSG = "All ActiveMQ connections are healthy";
    private static final String UNHEALTHY_MSG_PREFIX = "<strong>Unhealthy</strong>";

    private StatHelper statHelper;
    private TestAppConfig appConfig;

    @BeforeEach
    void setUp() {
        var client = mock(Client.class);
        statHelper = spy(new StatHelper(DEVELOPMENT_SERVERS, "http", 8011, client, JSON_HELPER));
        appConfig = TestAppConfig.builder().build();
    }

    @Test
    void shouldFetchStats() {
        ArgumentCaptor<String> urlCaptor = configureMockResponse();
        List<JolokiaResponseValue> statResults = statHelper.getStats("test");

        assertAll(
                () -> assertThat(urlCaptor.getValue())
                        .isEqualTo("http://unit-test-1:8011/api/jolokia/read/org.apache.activemq:type=Broker,brokerName=*,destinationType=*,destinationName=test"),
                () -> assertThat(statResults).hasSize(1)
        );

        var value = first(statResults);
        assertAll(
                () -> assertThat(value.getName()).isEqualTo("test"),
                () -> assertThat(value.getQueueSize()).isEqualTo(3),
                () -> assertThat(value.getConsumerCount()).isEqualTo(2),
                () -> assertThat(value.getEnqueueCount()).isEqualTo(5),
                () -> assertThat(value.getDequeueCount()).isEqualTo(2)
        );
    }

    @Test
    void shouldVerifyHealthy_ProducerHealthCheck() {
        var config = new ActiveMqConfig();
        config.setProducers(List.of("queue:test"));
        appConfig.setActiveMqConfig(config);

        assertHealthyHealthCheck(new ProducerStatsHealthCheck<>(appConfig, statHelper));
    }

    @Test
    void shouldVerifyHealthy_ConsumerHealthCheck() {
        var config = new ActiveMqConfig();
        config.setConsumers(List.of("queue:test"));
        appConfig.setActiveMqConfig(config);

        assertHealthyHealthCheck(new ConsumerStatsHealthCheck<>(appConfig, statHelper));
    }

    @SuppressWarnings("unchecked")
    private void assertHealthyHealthCheck(StatsHealthCheck<TestAppConfig> healthCheck) {
        configureMockResponse();

        assertAll(
                () -> assertThat(healthCheck.isProducer()).isEqualTo(healthCheck instanceof ProducerStatsHealthCheck),
                () -> assertThat(healthCheck.getDestinationList()).containsExactly("queue:test")
        );

        var result = healthCheck.check();

        assertThatResult(result)
                .isHealthy()
                .hasMessage(HEALTHY_MSG)
                .hasDetailsContainingKeys("healthyResults", "unhealthyResults", "ignoredDestinations")
                .hasNoError();

        var details = result.getDetails();
        assertThat(details).isNotNull();

        assertAll(
                () -> assertThat(details).hasSize(3),

                () -> {
                    var healthyResults = MapUtils.getMap(details, "healthyResults");
                    assertThat(healthyResults).hasSize(1);
                    verifyDetailMessage(result, "details.healthyResults.test.message",
                            "test - Pending messages: 3 (threshold: 100), Active consumers: 2 (threshold: 0)");
                },

                () -> assertThat(MapUtils.getMap(details, "unhealthyResults")).isEmpty(),

                () -> assertThat((List<String>) details.get("ignoredDestinations")).isEmpty()
        );
    }

    @Test
    void shouldVerify_UnhealthyProducerHealthCheck() {
        var healthConfig = new ActiveMqHealthConfig();
        healthConfig.setMaxPendingThreshold(1);
        healthConfig.setMinConsumerThreshold(3);

        var config = new ActiveMqConfig();
        config.setProducers(List.of("queue:test"));
        config.setHealthConfig(healthConfig);
        appConfig.setActiveMqConfig(config);

        assertUnhealthyHealthCheck(new ProducerStatsHealthCheck<>(appConfig, statHelper));
    }

    @Test
    void shouldVerify_UnhealthyConsumerHealthCheck() {
        var healthConfig = new ActiveMqHealthConfig();
        healthConfig.setMaxPendingThreshold(1);
        healthConfig.setMinConsumerThreshold(3);

        var config = new ActiveMqConfig();
        config.setConsumers(List.of("queue:test"));
        config.setHealthConfig(healthConfig);
        appConfig.setActiveMqConfig(config);

        assertUnhealthyHealthCheck(new ConsumerStatsHealthCheck<>(appConfig, statHelper));
    }

    @SuppressWarnings("unchecked")
    private void assertUnhealthyHealthCheck(StatsHealthCheck<TestAppConfig> healthCheck) {
        configureMockResponse();

        assertAll(
                () -> assertThat(healthCheck.isProducer()).isEqualTo(healthCheck instanceof ProducerStatsHealthCheck),
                () -> assertThat(healthCheck.getDestinationList()).containsExactly("queue:test")
        );

        var result = healthCheck.check();

        assertThatResult(result)
                .isUnhealthy()
                .hasMessageContaining(UNHEALTHY_MSG_PREFIX)
                .hasDetailsContainingKeys("healthyResults", "unhealthyResults", "ignoredDestinations")
                .hasNoError();

        var details = result.getDetails();
        assertThat(details).isNotNull();

        assertAll(
                () -> assertThat(details).hasSize(3),

                () -> assertThat(MapUtils.getMap(details, "healthyResults")).isEmpty(),

                () -> {
                    var unhealthyResults = MapUtils.getMap(details, "unhealthyResults");
                    assertThat(unhealthyResults).hasSize(1);
                    verifyDetailMessage(result, "details.unhealthyResults.test.message",
                            "test - Pending messages: 3 (threshold: 1), Active consumers: 2 (threshold: 3)");
                },

                () -> assertThat((List<String>) details.get("ignoredDestinations")).isEmpty()
        );
    }

    @Test
    void shouldIgnore_IgnoredQueueDestinations() {
        var healthConfig = new ActiveMqHealthConfig();
        healthConfig.setIgnoredDestinations(List.of("queue:test"));

        var config = new ActiveMqConfig();
        config.setConsumers(List.of("queue:test"));
        config.setHealthConfig(healthConfig);
        appConfig.setActiveMqConfig(config);

        configureMockResponse();
        var result = new ConsumerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isHealthy()
                .hasMessage(HEALTHY_MSG)
                .hasNoError();

        assertThat(result.getDetails()).hasSize(3).containsKey("ignoredDestinations");

        verifyDetailMessage(result, "details.ignoredDestinations.[0]", "queue:test");
    }

    @Test
    void shouldReportUnhealthy_WhenInvalidDestination() {
        var healthConfig = new ActiveMqHealthConfig();

        var config = new ActiveMqConfig();
        config.setConsumers(List.of("not_a_thing:test"));
        config.setHealthConfig(healthConfig);
        appConfig.setActiveMqConfig(config);

        configureMockResponse();
        var result = new ConsumerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isUnhealthy()
                .hasMessage("""
                        <strong>Unhealthy</strong>
                        <br />Unable to retrieve stats for: null""");
    }

    @Test
    void shouldReportHealthy_WhenNoConsumers_AndNoPendingMessages() {
        var config = new ActiveMqConfig();
        config.setConsumers(List.of("queue:test"));
        appConfig.setActiveMqConfig(config);

        var responseJson = getSampleJsonForNoSubscriptionsWithQueueCount(0);
        configureMockResponse(responseJson);

        var result = new ConsumerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isHealthy()
                .hasMessage(HEALTHY_MSG)
                .hasNoError();

        assertThat(result.getDetails()).hasSize(3);

        verifyDetailMessage(result, "details.healthyResults.test.message",
                "test - Pending messages: 0 (threshold: 100), Active consumers: 0 (threshold: 0)");
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 5, 15 })
    void shouldReportUnhealthy_WhenNoConsumers_AndSomePendingMessages(int queueCount) {
        var config = new ActiveMqConfig();
        config.setConsumers(List.of("queue:test"));
        appConfig.setActiveMqConfig(config);

        var responseJson = getSampleJsonForNoSubscriptionsWithQueueCount(queueCount);
        configureMockResponse(responseJson);

        var result = new ConsumerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isUnhealthy()
                .hasMessageContaining(UNHEALTHY_MSG_PREFIX)
                .hasNoError();

        assertThat(result.getDetails()).hasSize(3);

        var expectedMessage = f("test - Pending messages: {} (threshold: 100), Active consumers: 0 (threshold: 0)", queueCount);
        verifyDetailMessage(result, "details.unhealthyResults.test.message",
                expectedMessage);
    }

    @Test
    void shouldReportHealthy_AndIgnoreNotFoundResponse_ForProducerVirtualTopic() {
        var config = new ActiveMqConfig();
        config.setProducers(List.of("topic:test"));
        appConfig.setActiveMqConfig(config);

        var responseJson = getVirtualTopicResponse();
        configureMockResponse(responseJson);

        var result = new ProducerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isHealthy()
                .hasMessage(HEALTHY_MSG)
                .hasNoError();

        assertThat(result.getDetails()).hasSize(3);
    }

    @Test
    void shouldReportUnhealthy_ForConsumerVirtualTopic_WhenCannotGetStats() {
        var topicName = "test";
        var config = new ActiveMqConfig();
        config.setConsumers(List.of("topic:" + topicName));
        appConfig.setActiveMqConfig(config);

        var response = mock(Response.class);
        when(response.getStatus()).thenReturn(200);
        when(response.readEntity(String.class)).thenReturn(null);  // causes an NPE in checkStatsForDestination

        var urlCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(response).when(statHelper).doGet(any(), urlCaptor.capture());

        var result = new ConsumerStatsHealthCheck<>(appConfig, statHelper).check();

        assertThatResult(result)
                .isUnhealthy()
                .hasMessageContaining(UNHEALTHY_MSG_PREFIX)
                .hasNoError();

        Map<String, Object> details = result.getDetails();
        assertThat(details).hasSize(3);

        @SuppressWarnings("unchecked")
        var unhealthyResults = (Map<String, Object>) MapUtils.getMap(details, "unhealthyResults");

        var expectedKey = f("Consumer.{}.VirtualTopic.{}", appConfig.getServiceName(), topicName);
        assertThat(unhealthyResults).hasSize(1).containsKey(expectedKey);

        @SuppressWarnings("unchecked")
        var virtualTopicDetail = (Map<String, Object>) MapUtils.getMap(unhealthyResults, expectedKey);

        assertThat(virtualTopicDetail)
                .containsEntry("message", f("Unable to retrieve stats for: {}", expectedKey))
                .containsEntry("details", null);
    }

    @Test
    void shouldReportUnhealthy_WhenExceptionThrown_WhileGettingFuture() throws Exception {
        var config = new ActiveMqConfig();
        config.setProducers(List.of("topic:test"));

        appConfig.setActiveMqConfig(config);

        configureMockResponse();

        var healthCheckSpy = spy(new ConsumerStatsHealthCheck<>(appConfig, statHelper));

        doThrow(new ExecutionException("who knows", new RuntimeException("random error")))
                .when(healthCheckSpy)
                .getResult(any());

        var result = healthCheckSpy.check();

        assertThatResult(result)
                .isUnhealthy()
                .hasMessage("Failed to retrieve stats: who knows");

        verify(healthCheckSpy).getResult(any());
    }

    @Test
    void shouldReturnLastResult_WhenInterruptedExceptionThrown_WhileGettingFuture() throws Exception {
          var config = new ActiveMqConfig();
        config.setProducers(List.of("topic:test"));

        appConfig.setActiveMqConfig(config);

        configureMockResponse();

        var kiwiEnvironment = mock(KiwiEnvironment.class);
        var thread = mock(Thread.class);
        when(kiwiEnvironment.currentThread()).thenReturn(thread);

        var healthCheckSpy = spy(new ConsumerStatsHealthCheck<>(appConfig, statHelper, kiwiEnvironment));

        var lastResult = HealthCheck.Result.healthy();
        healthCheckSpy.lastResult = lastResult;

        doThrow(new InterruptedException("I interrupt you!"))
                .when(healthCheckSpy)
                .getResult(any());
        
        var result = healthCheckSpy.check();

        assertThat(result).isSameAs(lastResult);

        verify(healthCheckSpy).getResult(any());
        verify(kiwiEnvironment, only()).currentThread();
        verify(thread, only()).interrupt();
    }

    @Test
    void shouldSkipExecution_WhenRefreshInterval_HasNotElapsed() {
        var config = new ActiveMqConfig();
        config.setProducers(List.of("topic:test"));

        appConfig.setActiveMqConfig(config);

        var lastResult = HealthCheck.Result.healthy();

        var healthCheck = new ConsumerStatsHealthCheck<>(appConfig, statHelper);
        
        healthCheck.lastResult = lastResult;
        healthCheck.lastUpdateTimestamp.set(System.currentTimeMillis());

        var result = healthCheck.check();

        assertThat(result).isSameAs(lastResult);
    }

    private ArgumentCaptor<String> configureMockResponse() {
        return configureMockResponse(getSampleJson());
    }

    private ArgumentCaptor<String> configureMockResponse(String responseJson) {
        var mockResponse = mock(Response.class);
        when(mockResponse.getStatus()).thenReturn(200);
        when(mockResponse.readEntity(String.class)).thenReturn(responseJson);

        var urlCaptor = ArgumentCaptor.forClass(String.class);
        doReturn(mockResponse).when(statHelper).doGet(any(), urlCaptor.capture());

        return urlCaptor;
    }

    private String getSampleJson() {
        return Fixtures.fixture("sample-jolokia-stats-response.json");
    }

    private String getSampleJsonForNoSubscriptionsWithQueueCount(int queueCount) {
        var template = Fixtures.fixture("sample-jolokia-stats-no-subscriptions-response.json");
        return template
                .replaceAll("\\{CONSUMER_COUNT}", "0")
                .replaceAll("\\{QUEUE_COUNT}", Integer.toString(queueCount));
    }

    private String getVirtualTopicResponse() {
        return Fixtures.fixture("sample-jolokia-stats-virtual-topic-response.json");
    }

    private void verifyDetailMessage(HealthCheck.Result result, String path, String expectedValue) {
        var value = KiwiTestConstants.JSON_HELPER.getPath(result, path, String.class);
        assertThat(value).isEqualTo(expectedValue);
    }
}
