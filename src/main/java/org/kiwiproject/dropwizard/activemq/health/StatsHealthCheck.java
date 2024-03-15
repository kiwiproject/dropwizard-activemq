package org.kiwiproject.dropwizard.activemq.health;

import com.codahale.metrics.health.HealthCheck;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

class StatsHealthCheck<C extends ActiveMqConfigured> extends HealthCheck {

    StatsHealthCheck(C activeMqConfigured) {
        this(activeMqConfigured, new StatHelper(activeMqConfigured.getActiveMqConfig()));
    }

    StatsHealthCheck(C activeMqConfigured, StatHelper statHelper) {
        // TODO
    }

    @Override
    protected Result check() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'check'");
    }

}
