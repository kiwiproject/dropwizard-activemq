package org.kiwiproject.dropwizard.activemq.health;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;

class StatHelper {

    static final String DLQ_QUEUE_NAME = "ActiveMQ.DLQ";

    StatHelper(ActiveMqConfig config) {
        // TODO
    }

    JolokiaResponseValue getStatsSingleResultOrNull(String name) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getStatsSingleResultOrNull'");
    }

}
