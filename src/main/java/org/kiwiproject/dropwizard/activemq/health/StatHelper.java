package org.kiwiproject.dropwizard.activemq.health;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.exception.ExceptionUtils.indexOfThrowable;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.base.KiwiStrings.f;
import static org.kiwiproject.collect.KiwiLists.first;
import static org.kiwiproject.collect.KiwiLists.isNotNullOrEmpty;
import static org.kiwiproject.collect.KiwiLists.isNullOrEmpty;
import static org.kiwiproject.collect.KiwiLists.newListStartingAtCircularOffset;
import static org.kiwiproject.jaxrs.KiwiEntities.safeReadEntity;

import com.google.common.annotations.VisibleForTesting;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.jaxrs.KiwiResponses;
import org.kiwiproject.jaxrs.exception.JaxrsNotFoundException;
import org.kiwiproject.json.JsonHelper;

import javax.net.ssl.HostnameVerifier;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Slf4j
class StatHelper {

    private static final Pattern BROKER_URI_SPLIT_PATTERN = Pattern.compile("\\s*,\\s*");
    private static final Pattern HOST_EXTRACTION_PATTERN = Pattern.compile(".*?//(.*?)[:?/].*");
    private static final String JOLOKIA_API_PATH = "/api/jolokia/";
    private static final String READ_DESTINATION_STATS =
            "read/org.apache.activemq:type=Broker,brokerName=*,destinationType=*,destinationName=";
    private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 5;
    private static final int DEFAULT_READ_TIMEOUT_SECONDS = 5;

    static final String DLQ_QUEUE_NAME = "ActiveMQ.DLQ";

    @VisibleForTesting
    final Client client;

    private final List<String> baseUrlQueue;
    private final int baseUrlCount;
    private final AtomicLong currentUrlIndex;
    private final JsonHelper jsonHelper;

    StatHelper(ActiveMqConfig config) {
        this(config.getBrokerUri(),
                getUriScheme(config),
                config.getJolokiaPort(),
                buildClient(config),
                JsonHelper.newDropwizardJsonHelper());
    }

    @VisibleForTesting
    StatHelper(String brokerUri, String uriScheme, int port, Client client, JsonHelper jsonHelper) {
        this.client = requireNotNull(client);
        this.currentUrlIndex = new AtomicLong();
        this.jsonHelper = requireNotNull(jsonHelper);

        checkArgumentNotBlank(brokerUri);
        checkArgumentNotBlank(uriScheme);
        this.baseUrlQueue = List.copyOf(buildJolokiaUrls(brokerUri, uriScheme, port));
        checkArgument(!this.baseUrlQueue.isEmpty(),
                f("Must be able to extract at least 1 base url from: {}", brokerUri));
        this.baseUrlCount = this.baseUrlQueue.size();
    }

    private static String getUriScheme(ActiveMqConfig config) {
        return config.isUseSecureRestConnections() ? "https" : "http";
    }

    private static Client buildClient(ActiveMqConfig config) {
        var jmxUser = config.getHealthConfig().getJmxUser();
        var jmxCred = config.getHealthConfig().getJmxCred();
        var basicAuth = HttpAuthenticationFeature.basic(jmxUser, jmxCred);

        var clientBuilder = ClientBuilder.newBuilder()
                .connectTimeout(DEFAULT_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .readTimeout(DEFAULT_READ_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .register(basicAuth);

        if (config.isUseSecureRestConnections()) {
            var sslContext = config.getTlsConfiguration().toSSLContext();
            var hostnameVerifier = createAppropriateHostnameVerifier(config.isVerifyRestConnectionHostnames());
            return clientBuilder
                    .sslContext(sslContext)
                    .hostnameVerifier(hostnameVerifier)
                    .build();
        }

        return clientBuilder.build();
    }

    private static HostnameVerifier createAppropriateHostnameVerifier(boolean verifyHostnames) {
        return verifyHostnames ? new DefaultHostnameVerifier() : new NoopHostnameVerifier();
    }

    @VisibleForTesting
    static List<String> buildJolokiaUrls(String brokerUri, String uriScheme, int port) {
        return Stream.of(BROKER_URI_SPLIT_PATTERN.split(brokerUri))
                .map(StatHelper::matchingHostOrNull)
                .filter(Objects::nonNull)
                .map(hostname -> buildJolokiaUrl(uriScheme, hostname, port))
                .toList();
    }

    private static String matchingHostOrNull(String url) {
        var matcher = HOST_EXTRACTION_PATTERN.matcher(url);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private static String buildJolokiaUrl(String scheme, String hostname, int port) {
        var validHostname = "embedded".equals(hostname) ? "localhost" : hostname;

        return f("{}://{}:{}{}", scheme, validHostname, port, JOLOKIA_API_PATH);
    }

    /**
     * @throws JaxrsNotFoundException if there is any problem getting the stats
     */
    JolokiaResponseValue getStatsSingleResultOrNull(String topicOrQueueName) {
        List<JolokiaResponseValue> stats = getStats(topicOrQueueName);
        if (isNullOrEmpty(stats)) {
            return null;
        }

        int numStats = stats.size();
        if (numStats > 1) {
            LOG.warn("Retrieved {} sets of stats for destination named: {}, returning only the FIRST entry",
                    numStats,
                    topicOrQueueName);
        }

        return first(stats);
    }

    /**
     * @throws JaxrsNotFoundException if there is any problem getting the stats
     */
    @VisibleForTesting
    List<JolokiaResponseValue> getStats(String topicOrQueueName) {
        List<String> urls = getUrlsForDestination(topicOrQueueName);
        return getStatsForDestination(urls);
    }

    @VisibleForTesting
    List<String> getUrlsForDestination(String topicOrQueueName) {
        var input = baseUrlQueue.stream()
                .map(baseUrl -> f("{}{}{}", baseUrl, READ_DESTINATION_STATS, topicOrQueueName))
                .toList();
        return newListStartingAtCircularOffset(input, currentUrlIndex.get());
    }

    /**
     * @throws JaxrsNotFoundException if there is any problem getting the stats
     */
    @VisibleForTesting
    List<JolokiaResponseValue> getStatsForDestination(List<String> activeMqUrls) {

        List<Response> successfulResponses = new ArrayList<>();
        var anyRequestWasSuccessful = activeMqUrls.stream()
                .anyMatch(url -> attemptClientGet(client, url, successfulResponses));
        LOG.debug("GET of stats to any of URLs {} succeeded? {}", activeMqUrls, anyRequestWasSuccessful);

        if (isNotNullOrEmpty(successfulResponses)) {
            var json = first(successfulResponses).readEntity(String.class);
            var jolokiaResponse = jsonHelper.toObject(json, JolokiaResponse.class);

            if (nonNull(jolokiaResponse.getValue())) {
                return new ArrayList<>(jolokiaResponse.getValue().values());
            } else {
                logWarningsIfNotDLQ(json, jolokiaResponse);
            }
        }

        throw new JaxrsNotFoundException("Unable to fetch stats from ActiveMQ urls: " + activeMqUrls);
    }

    /**
     * @implNote Does NOT close successful Responses. That is left to the caller.
     */
    @VisibleForTesting
    boolean attemptClientGet(Client client, String url, List<Response> successfulResponses) {
        LOG.debug("Fetching statistics from: {}", url);

        try {
            var response = doGet(client, url);
            LOG.debug("Received {} response from {}", response.getStatus(), url);

            if (KiwiResponses.successful(response)) {
                successfulResponses.add(response);
                return true;
            } else {
                LOG.warn("Got unsuccessful {} response from {} with entity: {}",
                        response.getStatus(),
                        url,
                        safeReadEntity(response).orElse("[no response entity]")
                );
                KiwiResponses.closeQuietly(response);
            }

        } catch (Exception e) {
            if (indexOfThrowable(e, ConnectException.class) >= 0) {
                LOG.trace("Failed to connect to: {}, probably not the primary broker", url, e);
            } else {
                LOG.warn("Encountered exception fetching response from: {}," +
                                " exception type: {}, exception message: {} (enable DEBUG for details)",
                        url, e.getClass(), e.getMessage());
                LOG.debug("Exception from: {}", url, e);
            }
        }

        incrementBaseUrlIndexIfCurrentUrlMatches(url);
        return false;
    }

    @VisibleForTesting
    Response doGet(Client client, String url) {
        return client.target(url).request(MediaType.APPLICATION_JSON_TYPE).get();
    }

    @VisibleForTesting
    void incrementBaseUrlIndexIfCurrentUrlMatches(String url) {
        currentUrlIndex.updateAndGet(currIndex -> {
            var urlPrefix = baseUrlQueue.get((int) currIndex % baseUrlCount);

            if (StringUtils.startsWith(url, urlPrefix)) {
                long nextIndex = currIndex + 1;
                LOG.trace("Incrementing current url index {} to: {}", currIndex, nextIndex);
                return nextIndex;
            } else {
                LOG.trace("Leaving current url index at: {} (url does not start with prefix)", currIndex);
                return currIndex;
            }
        });
    }

    private static void logWarningsIfNotDLQ(String json, JolokiaResponse jolokiaResponse) {
        // TODO See about changing this to use the "DLQ" boolean value (assuming it is always present)
        if (json.contains(DLQ_QUEUE_NAME)) {
            LOG.trace("DLQ was not found; this is not a problem (since it means we do NOT have dead messages)");
            return;
        }

        LOG.warn("Unable to retrieve value set from response (enable DEBUG for details)");
        if (isNotBlank(jolokiaResponse.getError())) {
            LOG.warn("Error returned: type={}, message={}",
                    jolokiaResponse.getErrorType(), jolokiaResponse.getError());
        }

        LOG.debug("Response JSON: {}", json);
    }

    @VisibleForTesting
    String getCurrentBaseUrl() {
        int currIndex = (int) currentUrlIndex.get();
        return baseUrlQueue.get(currIndex % baseUrlCount);
    }
}
