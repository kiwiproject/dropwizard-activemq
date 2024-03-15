package org.kiwiproject.dropwizard.activemq.health;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

import java.util.List;

public class ProducerStatsHealthCheck<C extends ActiveMqConfigured> extends StatsHealthCheck<C> {

    public static final String DEFAULT_NAME = "ActiveMQ Producer Stats";

    public ProducerStatsHealthCheck(C activeMqConfigured) {
        super(activeMqConfigured);
        //TODO Auto-generated constructor stub
    }

    @Override
    protected List<String> getDestinationList() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDestinationList'");
    }

    @Override
    protected boolean isProducer() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isProducer'");
    }

    @Override
    protected boolean isExceedingConfiguredThresholds(JolokiaResponseValue stats) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'isExceedingConfiguredThresholds'");
    }
}
