package org.kiwiproject.dropwizard.activemq.health;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResult;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;

import lombok.extern.slf4j.Slf4j;

import org.kiwiproject.metrics.health.HealthStatus;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;

@Slf4j
public class BrokerHealthCheck extends HealthCheck {

    private static final String DEFAULT_NAME = "ActiveMQ Producer/Consumer";
    private static final long DEFAULT_JMS_CHECK_WAIT_TIME = 5;
    private static final TimeUnit DEFAULT_JMS_CHECK_WAIT_TIME_UNIT = TimeUnit.SECONDS;

    private final String name;
    private final long jmsCheckWaitTime;
    private final TimeUnit jmsCheckWaitTimeUnit;
    private final ConnectionFactory factory;
    private final String serviceName;

    public BrokerHealthCheck(String name, ConnectionFactory factory, String serviceName) {
        this(name, factory, DEFAULT_JMS_CHECK_WAIT_TIME, DEFAULT_JMS_CHECK_WAIT_TIME_UNIT, serviceName);
    }

    @VisibleForTesting
    BrokerHealthCheck(String name,
                      ConnectionFactory factory,
                      long jmsCheckWaitTime,
                      TimeUnit jmsCheckWaitTimeUnit,
                      String serviceName) {

        this.name = requireNotBlank(name);
        this.factory = requireNotNull(factory);
        this.jmsCheckWaitTime = jmsCheckWaitTime;
        this.jmsCheckWaitTimeUnit = requireNotNull(jmsCheckWaitTimeUnit);
        this.serviceName = requireNotBlank(serviceName);
    }

    public static String createHealthCheckNameWithPrefix(String prefix) {
        if (isBlank(prefix)) {
            return defaultHealthCheckName();
        }

        return prefix + " " + defaultHealthCheckName();
    }

    public static String defaultHealthCheckName() {
        return DEFAULT_NAME;
    }

    @Override
    protected Result check() {
        // This health check creates a temporary queue (see ConsumerAndProducerProvider). In ActiveMQ, creating
        // a temporary queue makes a blocking call. When AMQ is down, the call will block until it is alive
        // again. The following code uses a CompletionService to perform the check asynchronously, and waits up
        // to a maxiumum timeout before reporting as unhealthy.

        var brokerUrl = getBrokerUrl(factory).orElse("[unknown-broker-URL]");
        LOG.trace("Start health check {} for broker: {}", name, brokerUrl);

        var executor = Executors.newSingleThreadExecutor();
        var completionService = new ExecutorCompletionService<Result>(executor);
        try {
            LOG.trace("Submit task to check health in {} for broker: {}", name, brokerUrl);
            completionService.submit(() -> doProduceConsumeCheck(brokerUrl));

            try {
                LOG.trace("Wait up to {} {} for health check result in {} for broker: {}",
                    jmsCheckWaitTime, jmsCheckWaitTimeUnit, name, brokerUrl);
                var resultFuture = completionService.poll(jmsCheckWaitTime, jmsCheckWaitTimeUnit);
                return getResult(resultFuture, brokerUrl);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("InterruptedException waiting for {} health check for broker: {}", name, brokerUrl, e);
                return newUnhealthyResult(HealthStatus.CRITICAL, e,
                        "Interrupted while waiting. Broker: " + brokerUrl);
            } catch (ExecutionException e) {
                LOG.error("Unexpected ExecutionException performing {} health check for broker: {}", name, brokerUrl, e);
                return newUnhealthyResult(HealthStatus.CRITICAL, e,
                        "There is some problem with the broker (we don't know what happened):" + brokerUrl);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @VisibleForTesting
    static Optional<String> getBrokerUrl(ConnectionFactory factory) {
        return null;  // TODO
    }

    @VisibleForTesting
    Result doProduceConsumeCheck(String brokerUrl) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'doProduceConsumeCheck'");
    }

    private static Result getResult(Future<Result> resultFuture, String brokerUrl)
            throws InterruptedException, ExecutionException {

        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getResult'");
    }

}
