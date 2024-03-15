package org.kiwiproject.dropwizard.activemq.health;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

public class ConsumerStatsHealthCheck<C extends ActiveMqConfigured> extends StatsHealthCheck<C> {

    public static final String DEFAULT_NAME = "ActiveMQ Consumer Stats";

    public ConsumerStatsHealthCheck(C activeMqConfigured) {
        super(activeMqConfigured);
        //TODO Auto-generated constructor stub
    }
}
