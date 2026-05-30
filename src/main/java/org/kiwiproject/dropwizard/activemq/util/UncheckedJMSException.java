package org.kiwiproject.dropwizard.activemq.util;

import jakarta.jms.JMSException;

/**
 * Unchecked wrapper around {@link jakarta.jms.JMSException}, allowing JMS exceptions to
 * propagate without requiring checked exception handling at every call site.
 */
public class UncheckedJMSException extends RuntimeException {

    public UncheckedJMSException(JMSException jmsEx) {
        super(jmsEx);
    }

    /**
     * Returns the cause of this exception.
     *
     * @return the {@link JMSException} which is the cause of this exception
     */
    @Override
    public synchronized JMSException getCause() {
        return (JMSException) super.getCause();
    }
}
