package org.kiwiproject.dropwizard.activemq.health;

import static java.util.Objects.isNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResultBuilder;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResultBuilder;

import com.codahale.metrics.health.HealthCheck;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.jaxrs.exception.JaxrsNotFoundException;

/**
 * Health check that reports unhealthy if there are ANY messages in the DLQ (dead letter queue).
 * <p>
 * This check is NOT automatically registered by DropwizardActiveMq, so that if any messages get into
 * the DLQ, you don't see every service using this library start reporting at the same time.
 * <p>
 * You must explicitly register this health check, which gives you control over which service
 * will report on an unhealthy DLQ.
 * <p>
 * Note that you can also change the name of the DLQ if you are not using the ActiveMQ
 * default name. This can be done in
 * {@link org.kiwiproject.dropwizard.activemq.config.ActiveMqHealthConfig ActiveMqHealthConfig}
 * by setting the {@code dlqName} property.
 */
@Slf4j
public class DeadLetterQueueHealthCheck extends HealthCheck {

    private final StatHelper statHelper;
    private final String dlqName;

    public DeadLetterQueueHealthCheck(ActiveMqConfig config) {
        this(config, new StatHelper(config));
    }

    @VisibleForTesting
    DeadLetterQueueHealthCheck(ActiveMqConfig config, StatHelper statHelper) {
        this.statHelper = requireNotNull(statHelper);
        this.dlqName = requireNotBlank(config.getHealthConfig().getDlqName());
    }

    /**
     * @return the name of the ActiveMQ dead-letter queue
     */
    public String getQueueName() {
        return dlqName;
    }

    @Override
    protected Result check() {
        JolokiaResponseValue result;

        try {
            result = statHelper.getStatsSingleResultOrNull(dlqName);
        } catch (JaxrsNotFoundException e) {
            LOG.trace("Got a JaxrsNotFoundException trying to find the DLQ using getStatsSingleResultOrNull."
                    + " We will assume it does not exist, which is OK (b/c it means there are no messages in the DLQ)");

            return buildHealthyResult("Dead-letter queue does not exist");
        }

        if (isNull(result)) {
            return buildHealthyResult("No stats available for DLQ");
        }

        var queueSize = result.getQueueSize();
        if (isNull(queueSize)) {
            return buildUnhealthyResult("No QueueSize in response from ActiveMQ");
        }

        if (queueSize > 0) {
            return buildUnhealthyResult("Dead-letter queue contains messages. Current count: " + queueSize);
        }

        return buildHealthyResult("Dead-letter queue is empty");
    }

    private Result buildHealthyResult(String message) {
        return buildResult(true, message);
    }

    private Result buildUnhealthyResult(String message) {
        return buildResult(false, message);
    }

    private Result buildResult(boolean healthy, String message) {
        var builder = healthy ? newHealthyResultBuilder() : newUnhealthyResultBuilder();
        return builder
                .withMessage(message)
                .withDetail("dlqName", dlqName)
                .build();
    }
}
