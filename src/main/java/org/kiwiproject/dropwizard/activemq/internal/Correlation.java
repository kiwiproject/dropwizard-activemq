package org.kiwiproject.dropwizard.activemq.internal;

import lombok.experimental.UtilityClass;

@UtilityClass
class Correlation {

    static final ThreadLocal<String> CORRELATION_ID = new ThreadLocal<>();
}
