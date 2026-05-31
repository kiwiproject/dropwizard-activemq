package org.kiwiproject.dropwizard.activemq.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.kiwiproject.test.util.Fixtures;

import java.io.IOException;
import java.util.List;

@DisplayName("JolokiaResponse")
class JolokiaResponseTest {

    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Nested
    class Deserialization {

        @Test
        void shouldDeserialize_WithoutAttribute() throws IOException {
            var json = Fixtures.fixture("jolokia-response-amq5.json");
            var response = MAPPER.readValue(json, JolokiaResponse.class);
            var values = List.copyOf(response.getValue().values());

            assertAll(
                    () -> assertThat(response.getRequest())
                            .containsEntry("mbean", "org.apache.activemq:brokerName=*,destinationName=test,destinationType=*,type=Broker")
                            .containsEntry("type", "read")
                            .doesNotContainKey("attribute"),
                    () -> assertThat(values).satisfiesExactly(v -> {
                        assertThat(v.getQueueSize()).isEqualTo(3L);
                        assertThat(v.getConsumerCount()).isEqualTo(2L);
                    }),
                    () -> assertThat(response.getTimestamp()).isEqualTo(1710783708L),
                    () -> assertThat(response.getStatus()).isEqualTo(200L)
            );
        }

        @Test
        void shouldDeserialize_WithAttributeAsArray() throws IOException {
            var json = Fixtures.fixture("jolokia-response-amq6.json");
            var response = MAPPER.readValue(json, JolokiaResponse.class);
            var values = List.copyOf(response.getValue().values());

            assertAll(
                    () -> assertThat(response.getRequest())
                            .containsEntry("mbean", "org.apache.activemq:brokerName=*,destinationName=test,destinationType=*,type=Broker")
                            .containsEntry("type", "read")
                            .containsKey("attribute"),
                    () -> assertThat(response.getRequest().get("attribute"))
                            .asInstanceOf(InstanceOfAssertFactories.LIST)
                            .contains("QueueSize", "ConsumerCount", "Name"),
                    () -> assertThat(values).satisfiesExactly(v -> {
                        assertThat(v.getQueueSize()).isEqualTo(3L);
                        assertThat(v.getConsumerCount()).isEqualTo(2L);
                    }),
                    () -> assertThat(response.getTimestamp()).isEqualTo(1710783708L),
                    () -> assertThat(response.getStatus()).isEqualTo(200L)
            );
        }
    }
}
