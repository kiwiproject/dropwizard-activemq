package org.kiwiproject.dropwizard.activemq.test.util;

import static com.google.common.base.Preconditions.checkState;
import static org.kiwiproject.base.KiwiPreconditions.requireNotBlank;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ConsumerMessageListener implements MessageListener {

    @Getter
    private final String consumerName;

    private final AtomicInteger count;
    private final List<String> messages;

    public ConsumerMessageListener(String consumerName) {
        this.consumerName = requireNotBlank(consumerName);
        this.count = new AtomicInteger();
        this.messages = new ArrayList<>();
    }

    @Override
    public void onMessage(Message message) {
        checkState(message instanceof TextMessage, "only TextMessage is supported");
        var textMessage = (TextMessage) message;

        var currentCount = count.incrementAndGet();

        try {
            var text = textMessage.getText();
            messages.add(text);

            LOG.info("Consumer: '{}' received message: '{}', total count: {}",
                    consumerName, text, currentCount);
        } catch (Exception e) {
            LOG.error("Caught exception in onMessage", e);
            throw new RuntimeException("An unexpected JMSException occurred");
        }
    }

    public int getCount() {
        return count.get();
    }

    public List<String> getMessages() {
        return List.copyOf(messages);
    }
}
