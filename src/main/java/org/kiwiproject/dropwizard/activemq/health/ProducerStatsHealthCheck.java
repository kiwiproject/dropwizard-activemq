package org.kiwiproject.dropwizard.activemq.health;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

public class ProducerStatsHealthCheck<C extends ActiveMqConfigured> extends StatsHealthCheck<C> {

    public static final String DEFAULT_NAME = "ActiveMQ Producer Stats";

    public ProducerStatsHealthCheck(C activeMqConfigured) {
        super(activeMqConfigured);
        //TODO Auto-generated constructor stub
    }
}
