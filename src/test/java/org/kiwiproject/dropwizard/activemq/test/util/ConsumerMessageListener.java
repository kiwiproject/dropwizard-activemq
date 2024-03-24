package org.kiwiproject.dropwizard.activemq.test.util;

import static com.google.common.base.Preconditions.checkState;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConsumerMessageListener implements MessageListener {

    @Getter
    private final String consumerName;

    private final AtomicInteger count;

    public ConsumerMessageListener(String consumerName) {
        this.consumerName = requireNotBlank(consumerName);
        this.count = new AtomicInteger();
    }

    @Override
    public void onMessage(Message message) {
        checkState(message instanceof TextMessage, "only TextMessage is supported");
        var textMessage = (TextMessage) message;

        var currentCount = count.incrementAndGet();

        try {
            LOG.info("Consumer: '{}' received message: '{}', total count: {}",
                    consumerName, textMessage.getText(), currentCount);
        } catch (Exception e) {
            LOG.error("Caught exception in onMessage", e);
            throw new RuntimeException("An unexpected JMSException occured");
        }
    }

    public int getCount() {
        return count.get();
    }
}
