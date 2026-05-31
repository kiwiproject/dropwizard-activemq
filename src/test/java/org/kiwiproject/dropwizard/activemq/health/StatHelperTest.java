package org.kiwiproject.dropwizard.activemq.health;

import static com.google.common.base.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.test.assertj.KiwiAssertJ.assertIsExactType;
import static org.kiwiproject.test.constants.KiwiTestConstants.JSON_HELPER;
import static org.mockito.Mockito.mock;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.Response;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import okhttp3.Headers;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.base.KiwiStrings;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.dropwizard.activemq.test.util.HostnameVerification;
import org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory;
import org.kiwiproject.jaxrs.KiwiResponses;
import org.kiwiproject.jaxrs.exception.JaxrsNotFoundException;
import org.kiwiproject.net.KiwiUrls;
import org.kiwiproject.test.okhttp3.mockwebserver3.MockWebServerExtension;
import org.kiwiproject.test.util.Fixtures;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@SuppressWarnings("HttpUrlsUsage")
@DisplayName("StatHelper")
class StatHelperTest {

    private static final String BASE_HTTP_URL_1 = "http://host1:8011/api/jolokia/";
    private static final String BASE_HTTP_URL_2 = "http://host2:8011/api/jolokia/";
    private static final String BASE_HTTP_URL_3 = "http://host3:8011/api/jolokia/";
    private static final String DESTINATION_URL_FOO =
            "read/org.apache.activemq:type=Broker,brokerName=*,destinationType=*,destinationName=foo";

    private static final String STATS_URL_1 = BASE_HTTP_URL_1 + DESTINATION_URL_FOO;
    private static final String STATS_URL_2 = BASE_HTTP_URL_2 + DESTINATION_URL_FOO;
    private static final String STATS_URL_3 = BASE_HTTP_URL_3 + DESTINATION_URL_FOO;

    private static final String JOLOKIA_RESPONSE_FIXTURE_AMQ5 = Fixtures.fixture("sample-jolokia-stats-response.json");
    private static final String JOLOKIA_RESPONSE_FIXTURE_AMQ6 = Fixtures.fixture("sample-jolokia-stats-response-amq6.json");

    @Nested
    class IntegrationTests {

        @RegisterExtension
        final MockWebServerExtension mockServer = new MockWebServerExtension();

        private MockWebServer server;
        private StatHelper statHelper;
        private Client client;

        @BeforeEach
        void setUp() {
            server = mockServer.server();
            client = ClientBuilder.newClient();
            statHelper = new StatHelper(
                "tcp://host1:61616,tcp://host2:61616", "http", 8011, ActiveMqHealthConfig.DEFAULT_DLQ_NAME, client, JSON_HELPER);
        }

        @AfterEach
        void tearDown() {
            client.close();
        }

        static List<String> jolokiaResponseFixtures() {
            return List.of(JOLOKIA_RESPONSE_FIXTURE_AMQ5, JOLOKIA_RESPONSE_FIXTURE_AMQ6);
        }

        @ParameterizedTest
        @MethodSource("jolokiaResponseFixtures")
        void getStatsForDestination_shouldReturnTrue_OnSuccess(String fixture) {
            server.enqueue(new MockResponse(200, new Headers.Builder().build(), fixture));

            var urls = List.of(server.url("/api/jolokia/read/foo").toString());
            List<JolokiaResponseValue> stats = statHelper.getStatsForDestination(urls);

            assertThat(stats).hasSize(1);

            JolokiaResponse expectedResponse = JSON_HELPER.toObject(fixture, JolokiaResponse.class);
            JolokiaResponseValue expectedValue = first(List.copyOf(expectedResponse.getValue().values()));

            assertThat(first(stats)).usingRecursiveComparison().isEqualTo(expectedValue);
        }

        @Test
        void getStatsForDestination_shouldThrow_OnUnsuccessfulResponse() {
            server.enqueue(new MockResponse(500, new Headers.Builder().build(), "oops"));

            var urls = List.of(server.url("/api/jolokia/read/foo").toString());
            assertUnableToFetchStatsFor(urls);
        }

        @Test
        void getStatsForDestination_shouldThrow_OnConnectException() {
            var urls = List.of("http://localhost/api/jolokia");  // port 80, not the test server
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
            server.enqueue(new MockResponse(200, new Headers.Builder().build(), JOLOKIA_RESPONSE_FIXTURE_AMQ5));

            var url = server.url("/api/jolokia/read/foo").toString();
            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, url, successfulResponses);
            assertThat(successfulResponses).hasSize(1);
            KiwiResponses.closeQuietly(first(successfulResponses));

            assertThat(succeeded).isTrue();
        }

        @Test
        void attemptClientGet_shouldReturnFalse_OnUnsuccessfulResponse() {
            server.enqueue(new MockResponse(500, new Headers.Builder().build(), "oops"));

            var url = server.url("/api/jolokia/read/foo").toString();
            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, url, successfulResponses);
            assertThat(successfulResponses).isEmpty();
            assertThat(succeeded).isFalse();
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "http://localhost/api/jolokia",
                "http://localhost:67000/api/jolokia"
        })
        void attemptClientGet_shouldReturnFalse_OnInvalidUrls(String url) {
            var successfulResponses = new ArrayList<Response>();
            var succeeded = statHelper.attemptClientGet(client, url, successfulResponses);
            assertThat(successfulResponses).isEmpty();
            assertThat(succeeded).isFalse();
        }
    }

    // NOTE: Some values need single quotes around them because they contain commas
    @ParameterizedTest
    @CsvSource(textBlock = """
            http://messages.acme.com/api/jolokia, http, 8161, http://messages.acme.com:8161/api/jolokia/
            tcp://localhost:61616, http, 8011, http://localhost:8011/api/jolokia/
            ssl://localhost:61617, https, 8001, https://localhost:8001/api/jolokia/
            vm://embedded?, http, 8042, http://localhost:8042/api/jolokia/
            tcp://msg1.acme.com:61616?randomize=false&verifyHostName=false, http, 8042, http://msg1.acme.com:8042/api/jolokia/
            'failover:(ssl://msg1.acme.com:61617,ssl://msg2.acme.com:61617)?randomize=false&nested.verifyHostName=false', https, 8142, 'https://msg1.acme.com:8142/api/jolokia/,https://msg2.acme.com:8142/api/jolokia/'
            """)
    void shouldBuildJolokiaUrls(String brokerUri, String uriScheme, int port, String expectedJolokiaUrlCsv) {
        List<String> jolokiaUrls = StatHelper.buildJolokiaUrls(brokerUri, uriScheme, port);

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
                    "tcp://host1:61616,tcp://host2:61616", "http", 8011, ActiveMqHealthConfig.DEFAULT_DLQ_NAME, client, JSON_HELPER);
            assertThat(statHelper.getUrlsForDestination("foo")).containsExactly(
                    STATS_URL_1,
                    STATS_URL_2);
        }

        @Test
        void shouldWrapAroundToFirstOfTwoUrls_AfterIncrementingLastUrl() {
            var statHelper = new StatHelper(
                    "tcp://host1:61616,tcp://host2:61616", "http", 8011, ActiveMqHealthConfig.DEFAULT_DLQ_NAME, client, JSON_HELPER);

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
                    "tcp://host1:61616,tcp://host2:61616,tcp://host3:61616", "http", 8011, ActiveMqHealthConfig.DEFAULT_DLQ_NAME, client, JSON_HELPER);

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
                    "tcp://host1:61616,tcp://host2:61616", "http", 8011, ActiveMqHealthConfig.DEFAULT_DLQ_NAME, client, JSON_HELPER);

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
        var jolokiaPort = 8142;
        config.setJolokiaPort(jolokiaPort);
        config.setUseSecureRestConnections(true);
        config.setVerifyRestConnectionHostnames(hostnameVerification.verifyHostname);
        config.setTlsConfiguration(TestObjectFactory.newTlsContextConfiguration());

        var statHelper = new StatHelper(config);

        var client = assertIsExactType(statHelper.client, JerseyClient.class);
        assertThat(client.isDefaultSslContext()).isFalse();
        assertThat(client.getHostnameVerifier()).isExactlyInstanceOf(hostnameVerification.hostnameVerifierClass);
        assertThat(client.getConfiguration().isRegistered(HttpAuthenticationFeature.class)).isTrue();

        var sslContext = client.getSslContext();
        assertThat(sslContext.getProtocol()).isEqualTo(config.getTlsConfiguration().getProtocol());

        var urls = statHelper.getUrlsForDestination("foo");
        assertThat(urls).containsOnly(
                httpUrlAsHttpsWithPort(STATS_URL_1, jolokiaPort),
                httpUrlAsHttpsWithPort(STATS_URL_2, jolokiaPort),
                httpUrlAsHttpsWithPort(STATS_URL_3, jolokiaPort)
        );
    }

    private static String httpUrlAsHttpsWithPort(String httpUrl, int newPort) {
        checkArgument(httpUrl.startsWith("http://"));
        var components = KiwiUrls.extractAllFrom(httpUrl);
        return KiwiUrls.createHttpsUrl(components.getCanonicalName(), newPort, components.getPath().orElseThrow());
    }

    @Test
    void shouldConstructUsingInsecureClient() {
        var config = new ActiveMqConfig();
        config.setBrokerUri("failover:(ssl://host1:61617,ssl://host2:61617,ssl://host3:61617)?randomize=false");
        config.setHealthConfig(new ActiveMqHealthConfig());
        config.setJolokiaPort(8011);
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
