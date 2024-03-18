package org.kiwiproject.dropwizard.activemq.health;

import com.google.common.annotations.VisibleForTesting;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

import java.util.List;

/**
 * Health check for producers. This is automatically registered by DropwizardActiveMq unless explicitly disabled.
 *
 * @see org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig#setEnableStatsHealthChecks(boolean)
 */
public class ProducerStatsHealthCheck<C extends ActiveMqConfigured> extends StatsHealthCheck<C> {

    public static final String DEFAULT_NAME = "ActiveMQ Producer Stats";

    public ProducerStatsHealthCheck(C activeMqConfigured) {
        super(activeMqConfigured);
    }

    @VisibleForTesting
    ProducerStatsHealthCheck(C activeMqConfigured, StatHelper statHelper) {
        super(activeMqConfigured, statHelper);
    }

    @Override
    protected List<String> getDestinationList() {
        return config.getProducers();
    }

    @Override
    protected boolean isProducer() {
        return true;
    }

    @Override
    protected boolean isExceedingConfiguredThresholds(JolokiaResponseValue stats) {
        return isExceedingQueueThreshold(stats) || isBelowConsumerThreshold(stats);
    }

    private boolean isExceedingQueueThreshold(JolokiaResponseValue stats) {
        return stats.getQueueSize() >= getHealthConfig().getMaxPendingThreshold();
    }

    private boolean isBelowConsumerThreshold(JolokiaResponseValue stats) {
        return !stats.getName().startsWith("VirtualTopic")
                && stats.getConsumerCount() <= getHealthConfig().getMinConsumerThreshold();
    }
}
