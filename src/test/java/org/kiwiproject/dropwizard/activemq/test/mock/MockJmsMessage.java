package org.kiwiproject.dropwizard.activemq.test.mock;

import com.google.errorprone.annotations.Immutable;
import lombok.Getter;

import java.util.Map;

@Getter
@Immutable
public class MockJmsMessage {

    final String payload;
    final Map<String, Object> headers;

    private MockJmsMessage(String payload, Map<String, Object> headers) {
        this.payload = payload;
        this.headers = headers;
    }

    public static MockJmsMessage of(String payload, Map<String, Object> headers) {
        return new MockJmsMessage(payload, headers);
    }
}
