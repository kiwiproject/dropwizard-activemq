package org.kiwiproject.dropwizard.activemq.health;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.collect.KiwiLists.isNullOrEmpty;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.AccessLevel;

import org.kiwiproject.concurrent.Async;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig;
import org.kiwiproject.dropwizard.activemq.internal.DestinationIdentifier;
import org.kiwiproject.metrics.health.HealthCheckResults;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public abstract class StatsHealthCheck<C extends ActiveMqConfigured> extends HealthCheck {

    private static final String LINE_SEPARATOR = "\n<br />";

    protected final C activeMqConfigured;
    protected final ActiveMqConfig config;
    protected final String serviceName;

    private final ActiveMqHealthConfig healthConfig;
    private final List<String> ignoredDestinations;

    private final AtomicLong lastUpdateTimestamp;
    private final long healthCheckRefreshIntervalMillis;
    private Result lastResult;

    @VisibleForTesting
    @Setter(AccessLevel.PACKAGE)
    private StatHelper statHelper;

    StatsHealthCheck(C activeMqConfigured) {
        this(activeMqConfigured, new StatHelper(activeMqConfigured.getActiveMqConfig()));
    }

    StatsHealthCheck(C activeMqConfigured, StatHelper statHelper) {
        this.activeMqConfigured = requireNotNull(activeMqConfigured);
        var activeMqConfig = activeMqConfigured.getActiveMqConfig();
        this.config = requireNotNull(activeMqConfig);
        this.healthConfig = requireNotNull(activeMqConfig.getHealthConfig());
        this.ignoredDestinations = this.healthConfig.getIgnoredDestinations();
        this.serviceName = requireNotBlank(activeMqConfigured.getServiceName());
        this.statHelper = requireNotNull(statHelper);

        this.lastUpdateTimestamp = new AtomicLong();
        this.healthCheckRefreshIntervalMillis = healthConfig.getRefreshInterval().toMilliseconds();
    }

    @Override
    protected Result check() throws Exception {
        if (isNull(lastResult) || refreshIntervalElapsed()) {
            CompletableFuture<Result> future = Async.doAsync(this::performCheck);

            try {
                lastResult = future.get(10, TimeUnit.SECONDS); // TODO make this configurable?
                lastUpdateTimestamp.set(System.currentTimeMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while checking ActiveMQ statistics", e);
            } catch (Exception e) {
                LOG.error("Encountered exception checking ActiveMQ statistics", e);
                return HealthCheckResults.newUnhealthyResult("Failed to retrieve stats: %s", e.getMessage());
            }
        } else {
            LOG.debug("Skipping stat check, refresh interval ({} seconds) not yet reached",
                    healthCheckRefreshIntervalMillis);
        }

        return lastResult;
    }

    private boolean refreshIntervalElapsed() {
        long maxAllowedTimestampSinceLastRefresh = System.currentTimeMillis() - healthCheckRefreshIntervalMillis;
        return lastUpdateTimestamp.get() < maxAllowedTimestampSinceLastRefresh;
    }

    private Result performCheck() {
        Map<String, JolokiaResponseValue> destinationStats = new HashMap<>();

        getDestinationList()
                .parallelStream()
                .filter(dest -> isNullOrEmpty(ignoredDestinations) ||
                        !ignoredDestinations.contains(dest))
                .forEach(dest -> checkStatsForDestination(destinationStats, dest));

        var resultBuilder = organizeResults(destinationStats);

        return resultBuilder.build();
    }

    private void checkStatsForDestination(Map<String, JolokiaResponseValue> resultMap, String dest) {
        DestinationIdentifier.DestinationInfo info = DestinationIdentifier
                .evaluateDestinationName(dest, isProducer(), serviceName).orElse(null);

        String key = null;
        try {
            LOG.debug("Performing Stat Check for destination: {}", dest);
            if (nonNull(info)) {
                key = info.getName();
                JolokiaResponseValue result = statHelper.getStatsSingleResultOrNull(info.getName());
                if (nonNull(result)) {
                    // inject destination info, so that we have context of the result in later
                    // processinf
                    result.setDestinationInfo(info);
                }
                resultMap.put(key, result);
            } else {
                LOG.warn("Unable to evalulate a destination for: {} (no DestinationInfo)", dest);
                resultMap.put(key, null);
            }
        } catch (Exception e) {
            LOG.error("Encountered exception trying to gather stats for destination: {}", key, e);
            resultMap.put(key, null);
        }
    }

    private ResultBuilder organizeResults(Map<String, JolokiaResponseValue> results) {
        Map<String, Map<String, Object>> unhealthyResults = new HashMap<>();
        Map<String, Map<String, Object>> healthyResults = new HashMap<>();

        results.keySet().forEach(dest -> {
            JolokiaResponseValue stats = results.get(dest);
            Map<String, Object> resultMap = buildResultMap(dest, stats);

            if (nonNull(stats)) {
                if (isIgnored(stats) || !isExceedingConfiguredThresholds(stats)) {
                    healthyResults.put(dest, resultMap);
                } else {
                    unhealthyResults.put(dest, resultMap);
                }
            } else {
                if (responseNotRequired(dest)) {
                    healthyResults.put(dest, resultMap);
                } else {
                    unhealthyResults.put(dest, resultMap);
                }
            }

            runAdditionalChecks(stats, resultMap, healthyResults, unhealthyResults);
        });

        var resultBuilder = unhealthyResults.isEmpty() ? Result.builder().healthy() : Result.builder().unhealthy();

        if (unhealthyResults.isEmpty()) {
            resultBuilder.withMessage("All ActiveMQ connections are healthy");
        } else {
            resultBuilder.withMessage(concatenateUnhealthyMessages(unhealthyResults));
        }

        resultBuilder.withDetail("unhealthyResults", unhealthyResults);
        resultBuilder.withDetail("healthyResults", healthyResults);
        resultBuilder.withDetail("ignoredDestinations", ignoredDestinations);

        return resultBuilder;
    }

    private Map<String, Object> buildResultMap(Object est, JolokiaResponseValue stats) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'buildResultMap'");
    }

    protected boolean isIgnored(JolokiaResponseValue stats) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isIgnored'");
    }

    protected boolean responseNotRequired(String destination) {
        return !responseRequired(destination);
    }

    protected boolean responseRequired(String destination) {
        var isProducerAndDestIsVirtualTopic = isProducer() && destination.startsWith("VirtualTopic.");

        return !isProducerAndDestIsVirtualTopic;
    }

    /**
     * This is a no-op. Override to run additional checks.
     */
    protected void runAdditionalChecks(JolokiaResponseValue stats,
            Map<String, Object> resultMap,
            Map<String, Map<String, Object>> healthyResults,
            Map<String, Map<String, Object>> unhealthyResults) {

        // no-op
    }

    private static String concatenateUnhealthyMessages(Map<String, Map<String, Object>> unhealthyResults) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'concatenateUnhealthyMessages'");
    }

    protected ActiveMqHealthConfig getHealthConfig() {
        return config.getHealthConfig();
    }

    // Abstract methods

    /**
     * TODO
     *
     * @return
     */
    protected abstract List<String> getDestinationList();

    /**
     * TODO
     *
     * @return
     */
    protected abstract boolean isProducer();

    /**
     * TODO
     *
     * @return
     */
    protected abstract boolean isExceedingConfiguredThresholds(JolokiaResponseValue stats);

}
