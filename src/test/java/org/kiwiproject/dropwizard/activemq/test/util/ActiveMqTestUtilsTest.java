package org.kiwiproject.dropwizard.activemq.test.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.CustomDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;

@DisplayName("ActiveMqTestUtils")
class ActiveMqTestUtilsTest {

    @Nested
    class NameOf {

        @Test
        void shouldReturnNameOfQueue() {
            var destination = new ActiveMQQueue("test-queue");

            assertThat(ActiveMqTestUtils.nameOf(destination)).isEqualTo("test-queue");
        }

        @Test
        void shouldReturnNameOfTopic() {
            var destination = new ActiveMQTopic("test-topic");

            assertThat(ActiveMqTestUtils.nameOf(destination)).isEqualTo("test-topic");
        }

        @Test
        void shouldThrowIllegalArgument_WhenIsNotQueueOrTopic() {
            var destination = new MyCustomDestination();
            var className = destination.getClass().getName();

            assertThatIllegalArgumentException()
                    .isThrownBy(() -> ActiveMqTestUtils.nameOf(destination))
                    .withMessage("Unsupported Destination type: %s", className);
        }
    }

    static class MyCustomDestination implements CustomDestination {

        @Override
        public MessageConsumer createConsumer(ActiveMQSession session, String messageSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageConsumer createConsumer(ActiveMQSession session,
                                              String messageSelector,
                                              boolean noLocal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicSubscriber createSubscriber(ActiveMQSession session,
                                                String messageSelector,
                                                boolean noLocal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicSubscriber createDurableSubscriber(ActiveMQSession session,
                                                       String name,
                                                       String messageSelector,
                                                       boolean noLocal) {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueueReceiver createReceiver(ActiveMQSession session, String messageSelector) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MessageProducer createProducer(ActiveMQSession session) throws JMSException {
            throw new UnsupportedOperationException();
        }

        @Override
        public TopicPublisher createPublisher(ActiveMQSession session) throws JMSException {
            throw new UnsupportedOperationException();
        }

        @Override
        public QueueSender createSender(ActiveMQSession session) throws JMSException {
            throw new UnsupportedOperationException();
        }
    }

}
