package org.kiwiproject.dropwizard.activemq.health;

import static com.google.common.base.Preconditions.checkArgument;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.test.assertj.KiwiAssertJ.assertIsExactType;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;
import static org.mockito.Mockito.mock;

import io.dropwizard.testing.junit5.DropwizardClientExtension;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;
import org.apache.commons.collections4.IterableUtils;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.base.KiwiStrings;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.dropwizard.activemq.test.util.HostnameVerification;
import org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory;
import org.kiwiproject.jaxrs.KiwiResponses;
import org.kiwiproject.jaxrs.exception.JaxrsNotFoundException;
import org.kiwiproject.test.util.Fixtures;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

@DisplayName("StatHelper")
class StatHelperTest {

    private static final String BASE_HTTP_URL_1 = "http://host1/api/jolokia/";
    private static final String BASE_HTTP_URL_2 = "http://host2/api/jolokia/";
    private static final String BASE_HTTP_URL_3 = "http://host3/api/jolokia/";
    private static final String DESTINATION_URL_FOO =
            "read/org.apache.activemq:type=Broker,brokerName=*,destinationType=*,destinationName=foo";

    private static final String STATS_URL_1 = BASE_HTTP_URL_1 + DESTINATION_URL_FOO;
    private static final String STATS_URL_2 = BASE_HTTP_URL_2 + DESTINATION_URL_FOO;
    private static final String STATS_URL_3 = BASE_HTTP_URL_3 + DESTINATION_URL_FOO;

    @Path("/api/jolokia")
    @Produces(APPLICATION_JSON)
    public static class StatsStubResource {

        static final String JOLOKIA_RESPONSE_VALUE = Fixtures.fixture("sample-jolokia-stats-response.json");

        private static final AtomicBoolean SIMULATE_ERROR = new AtomicBoolean(false);

        static void simulateOneTimeError() {
            SIMULATE_ERROR.set(true);
        }

        @GET
        @Path("/read/{details}")
        public Response getStats(@PathParam("details") String details) {
            if (SIMULATE_ERROR.getAndSet(false)) {
                return Response.serverError().entity("oops").build();
            }

            return Response.ok(JOLOKIA_RESPONSE_VALUE).build();
        }
    }

    // TODO After kiwi-test 3.3.0 is released, add ResetLogbackLoggingExtension to reset Logback
    private static final DropwizardClientExtension CLIENT = new DropwizardClientExtension(new StatsStubResource());

    @Nested
    @ExtendWith(DropwizardExtensionsSupport.class)
    class IntegrationTests {

        private StatHelper statHelper;
        private Client client;
        private URI baseUri;

        @BeforeEach
        void setUp() {
            client = ClientBuilder.newClient();
            baseUri = CLIENT.baseUri();

            statHelper = new StatHelper("tcp://host1:61616,tcp://host2:61616", "http", client, JSON_HELPER);
        }

        @AfterEach
        void tearDown() {
            client.close();
        }

        @Test
        void getStatsForDestination_shouldReturnTrue_OnSuccess() {
            var urls = List.of(baseUri + "/api/jolokia/" + DESTINATION_URL_FOO);

            List<JolokiaResponseValue> stats = statHelper.getStatsForDestination(urls);

            assertThat(stats).hasSize(1);

            JolokiaResponse expectedResponse = JSON_HELPER.toObject(StatsStubResource.JOLOKIA_RESPONSE_VALUE, JolokiaResponse.class);
            JolokiaResponseValue expectedResponseValue = IterableUtils.first(expectedResponse.getValue().values());

            JolokiaResponseValue value = first(stats);
            assertThat(value).usingRecursiveComparison().isEqualTo(expectedResponseValue);
        }

        @Test
        void getStatsForDestination_shouldThrow_OnUnsuccessfulResponse() {
            StatsStubResource.simulateOneTimeError();

            var urls = List.of(baseUri + "/api/jolokia/" + DESTINATION_URL_FOO);

            assertUnableToFetchStatsFor(urls);
        }

        @Test
        void getStatsForDestination_shouldThrow_OnConnectException() {
            var urls = List.of("http://localhost/api/jolokia");  // this uses port 80 (the test server won't be listening there)

            assertUnableToFetchStatsFor(urls);
        }

        @Test
        void getStatsForDestination_shouldThrow_OnException() {
            var urls = List.of("http://localhost:67000/api/jolokia");  // invalid port

            assertUnableToFetchStatsFor(urls);
        }

        private void assertUnableToFetchStatsFor(List<String> urls) {
            assertThatExceptionOfType(JaxrsNotFoundException.class)
                    .isThrownBy(() -> statHelper.getStatsForDestination(urls))
                    .withMessage("Unable to fetch stats from ActiveMQ urls: " + urls);
        }

        @Test
        void attemptClientGet_shouldReturnTrue_OnSuccessfulResponse() {
            var urls = List.of(baseUri + "/api/jolokia/" + DESTINATION_URL_FOO);

            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, first(urls), successfulResponses);
            assertThat(successfulResponses).hasSize(1);
            KiwiResponses.closeQuietly(first(successfulResponses));

            assertThat(succeeded).isTrue();
        }

        @Test
        void attemptClientGet_shouldReturnFalse_OnUnsuccessfulResponse() {
            StatsStubResource.simulateOneTimeError();

            var urls = List.of(baseUri + "/api/jolokia/" + DESTINATION_URL_FOO);

            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, first(urls), successfulResponses);
            assertThat(successfulResponses).isEmpty();
            assertThat(succeeded).isFalse();
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "http://localhost/api/jolokia",
                "http://localhost:67000/api/jolokia"
        })
        void attemptClientGet_shouldReturnFalse_OnInvalidUrls(String url) {
            var urls = List.of(url);

            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, first(urls), successfulResponses);
            assertThat(successfulResponses).isEmpty();
            assertThat(succeeded).isFalse();
        }
    }

    // NOTE: Some of the values need single quotes around them, because they contain commas
    @ParameterizedTest
    @CsvSource(textBlock = """
            http://messages.acme.com/api/jolokia, http, http://messages.acme.com/api/jolokia/
            tcp://localhost:61616, http, http://localhost/api/jolokia/
            ssl://localhost:61617, https, https://localhost/api/jolokia/
            vm://embedded?, http, http://embedded/api/jolokia/
            tcp://msg1.acme.com:61616?randomize=false&verifyHostName=false, http, http://msg1.acme.com/api/jolokia/
            'failover:(ssl://msg1.acme.com:61617,ssl://msg2.acme.com:61617)?randomize=false&nested.verifyHostName=false', https, 'https://msg1.acme.com/api/jolokia/,https://msg2.acme.com/api/jolokia/'
            """)
    void shouldBuildJolokiaUrls(String brokerUri, String uriScheme, String expectedJolokiaUrlCsv) {
        List<String> jolokiaUrls = StatHelper.buildJolokiaUrls(brokerUri, uriScheme);

        var expectedJolokiaUrls = KiwiStrings.splitOnCommas(expectedJolokiaUrlCsv);
        assertThat(jolokiaUrls).isEqualTo(expectedJolokiaUrls);
    }

    @Nested
    class CircularUrls {

        private Client client;

        @BeforeEach
        void setUp() {
            client = mock(Client.class);
        }

        @Test
        void shouldGetUrlsForDestination() {
            var statHelper = new StatHelper(
                    "tcp://host1:61616,tcp://host2:61616", "http", client, JSON_HELPER);
            assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                    STATS_URL_1,
                    STATS_URL_2);
        }

        @Test
        void shouldWrapAroundToFirstOfTwoUrls_AfterIncrementingLastUrl() {
            var statHelper = new StatHelper(
                    "tcp://host1:61616,tcp://host2:61616", "http", client, JSON_HELPER);

            // Start at base URL 1
            assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_1);
            statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_1);

            // After increment, should now be at base URL 2
            assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_2);
            assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                    STATS_URL_2,
                    STATS_URL_1);

            // Test incrementing 20 more times (after the above, we start at base URL 2)
            IntStream.rangeClosed(1, 10).forEach(ignored -> {
                statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_2);
                assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_1);
                assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                        STATS_URL_1,
                        STATS_URL_2);

                statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_1);
                assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_2);
                assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                        STATS_URL_2,
                        STATS_URL_1);
            });
        }

        @Test
        void shouldWrapAroundToFirstOfThreeUrls_AfterIncrementingLastUrl() {
            var statHelper = new StatHelper(
                    "tcp://host1:61616,tcp://host2:61616,tcp://host3:61616", "http", client, JSON_HELPER);

            // Test incrementing 30 times, starting at base URL 1
            IntStream.rangeClosed(1, 10).forEach(ignored -> {

                assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_1);
                assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                        STATS_URL_1,
                        STATS_URL_2,
                        STATS_URL_3);

                statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_1);
                assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_2);
                assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                        STATS_URL_2,
                        STATS_URL_3,
                        STATS_URL_1);

                statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_2);
                assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_3);
                assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                        STATS_URL_3,
                        STATS_URL_1,
                        STATS_URL_2);

                statHelper.incrementBaseUrlIndexIfCurrentUrlMatches(BASE_HTTP_URL_3);
            });
        }

        @Test
        void shouldNotIncrementWhenCurrentUrlDoesNotMatch() {
            var statHelper = new StatHelper(
                    "tcp://host1:61616,tcp://host2:61616", "http", client, JSON_HELPER);

            assertThat(statHelper.getCurrentBaseUrl()).isEqualTo(BASE_HTTP_URL_1);

            statHelper.incrementBaseUrlIndexIfCurrentUrlMatches("no-match-url");

            assertThat(statHelper.getCurrentBaseUrl())
                    .describedAs("the current URL should not have been incremented")
                    .isEqualTo(BASE_HTTP_URL_1);
        }
    }

    /**
     * Tests that the TLS configuration is used to correctly set up the StatHelper. It is
     * parameterized by the {@link HostnameVerification} option, which is used to verify the expected
     * {@link javax.net.ssl.HostnameVerifier} that is set on the {@link Client}.
     */
    @ParameterizedTest
    @EnumSource(HostnameVerification.class)
    void shouldConstructUsingSecureClient(HostnameVerification hostnameVerification) {
        var config = new ActiveMqConfig();
        config.setBrokerUri("failover:(ssl://host1:61617,ssl://host2:61617,ssl://host3:61617)?randomize=false");
        config.setHealthConfig(new ActiveMqHealthConfig());
        config.setUseSecureRestConnections(true);
        config.setVerifyRestConnectionHostnames(hostnameVerification.verifyHostname);
        config.setTlsConfiguration(TestObjectFactory.newTlsContextConfiguration());

        var statHelper = new StatHelper(config);

        var client = assertIsExactType(statHelper.client, JerseyClient.class);
        assertThat(client.isDefaultSslContext()).isFalse();
        assertThat(client.getHostnameVerifier()).isExactlyInstanceOf(hostnameVerification.hostnameVerifierClass);
        assertThat(client.getConfiguration().isRegistered(HttpAuthenticationFeature.class)).isTrue();

        var sslContext = client.getSslContext();
        assertThat(sslContext.getProtocol()).isEqualTo("TLSv1.2");

        var urls = statHelper.getUrlsForDestination("foo");
        assertThat(urls).containsOnly(
                httpUrlAsHttps(STATS_URL_1),
                httpUrlAsHttps(STATS_URL_2),
                httpUrlAsHttps(STATS_URL_3)
        );
    }

    private static String httpUrlAsHttps(String httpUrl) {
        checkArgument(httpUrl.startsWith("http://"));
        return httpUrl.replace("http", "https");
    }

    @Test
    void shouldConstructUsingInsecureClient() {
        var config = new ActiveMqConfig();
        config.setBrokerUri("failover:(ssl://host1:61617,ssl://host2:61617,ssl://host3:61617)?randomize=false");
        config.setHealthConfig(new ActiveMqHealthConfig());
        config.setUseSecureRestConnections(false);

        var statHelper = new StatHelper(config);

        var client = assertIsExactType(statHelper.client, JerseyClient.class);
        assertThat(client.isDefaultSslContext()).isTrue();
        assertThat(client.getHostnameVerifier()).isNull();
        assertThat(client.getConfiguration().isRegistered(HttpAuthenticationFeature.class)).isTrue();

        var urls = statHelper.getUrlsForDestination("foo");
        assertThat(urls).containsOnly(
                STATS_URL_1,
                STATS_URL_2,
                STATS_URL_3
        );
    }
}
