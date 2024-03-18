package org.kiwiproject.dropwizard.activemq.health;

import static java.util.Objects.isNull;
import static org.kiwiproject.base.KiwiPreconditions.requireNotNull;
import static org.kiwiproject.metrics.health.HealthCheckResults.newHealthyResult;
import static org.kiwiproject.metrics.health.HealthCheckResults.newUnhealthyResult;

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
 */
@Slf4j
public class DeadLetterQueueHealthCheck extends HealthCheck {

    private final StatHelper statHelper;

    public DeadLetterQueueHealthCheck(ActiveMqConfig config) {
        this(new StatHelper(config));
    }

    @VisibleForTesting
    DeadLetterQueueHealthCheck(StatHelper statHelper) {
        this.statHelper = requireNotNull(statHelper);
    }

    /**
     * @return the name of the ActiveMQ dead-letter queue
     */
    public String getQueueName() {
        return StatHelper.DLQ_QUEUE_NAME;
    }

    @Override
    protected Result check() throws Exception {
        JolokiaResponseValue result;

        try {
            result = statHelper.getStatsSingleResultOrNull(StatHelper.DLQ_QUEUE_NAME);
        } catch (JaxrsNotFoundException e) {
            LOG.trace("Got a JaxrsNotFoundException trying to find the DLQ using getStatsSingleResultOrNull."
                    + " We will assume it does not exist, which is OK (b/c it means there are no messages in the DLQ)");

            return newHealthyResult("Dead-letter queue does not exist");
        }

        if (isNull(result)) {
            return newHealthyResult("No stats available for DLQ");
        }

        var queueSize = result.getQueueSize();
        if (isNull(queueSize)) {
            return newUnhealthyResult("No QueueSize in response from ActiveMQ");
        }

        if (queueSize > 0) {
            return newUnhealthyResult("Dead-letter queue contains messages. Current count: " + queueSize);
        }

        return newHealthyResult("Dead-letter queue is empty");
    }
}
