package org.kiwiproject.dropwizard.activemq.health;

import com.google.common.annotations.VisibleForTesting;
import org.kiwiproject.base.KiwiEnvironment;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

import java.util.List;

/**
 * Health check for consumers. This is automatically registered by DropwizardActiveMq unless explicitly disabled.
 *
 * @see org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig#setEnableStatsHealthChecks(boolean)
 */
public class ConsumerStatsHealthCheck<C extends ActiveMqConfigured> extends StatsHealthCheck<C> {

    public static final String DEFAULT_NAME = "ActiveMQ Consumer Stats";

    public ConsumerStatsHealthCheck(C activeMqConfigured) {
        super(activeMqConfigured);
    }

    @VisibleForTesting
    ConsumerStatsHealthCheck(C activeMqConfigured, StatHelper statHelper) {
        super(activeMqConfigured, statHelper);
    }

    @VisibleForTesting
    ConsumerStatsHealthCheck(C activeMqConfigured, StatHelper statHelper, KiwiEnvironment kiwiEnvironment) {
        super(activeMqConfigured, statHelper, kiwiEnvironment);
    }

    @Override
    protected List<String> getDestinationList() {
        return config.getConsumers();
    }

    @Override
    protected boolean isProducer() {
        return false;
    }

    @Override
    protected boolean isExceedingConfiguredThresholds(JolokiaResponseValue stats) {
        return isExceedingQueueThreshold(stats) || isBelowConsumerThreshold(stats);
    }

    private boolean isExceedingQueueThreshold(JolokiaResponseValue stats) {
        return stats.getQueueSize() >= getHealthConfig().getMaxPendingThreshold();
    }

    private boolean isBelowConsumerThreshold(JolokiaResponseValue stats) {
        return stats.getConsumerCount() <= getHealthConfig().getMinConsumerThreshold();
    }
}
