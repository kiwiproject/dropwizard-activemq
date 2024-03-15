package org.kiwiproject.dropwizard.activemq.health;

import static org.apache.commons.lang3.StringUtils.isBlank;

import com.codahale.metrics.health.HealthCheck;

import javax.jms.ConnectionFactory;

public class BrokerHealthCheck extends HealthCheck {

    public static final String DEFAULT_NAME = "ActiveMQ Producer/Consumer";

    public BrokerHealthCheck(String name, ConnectionFactory factory, String serviceName) {
        // TODO
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
    protected Result check() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'check'");
    }

}
