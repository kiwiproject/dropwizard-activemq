package org.kiwiproject.dropwizard.activemq.util;

import javax.jms.JMSException;

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
