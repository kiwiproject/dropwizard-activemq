package org.kiwiproject.dropwizard.activemq.testing;

import com.google.errorprone.annotations.Immutable;
import lombok.Getter;

import java.util.Map;

@Getter
@Immutable
class FakeJmsMessage {

    final String payload;
    final Map<String, Object> headers;

    private FakeJmsMessage(String payload, Map<String, Object> headers) {
        this.payload = payload;
        this.headers = headers;
    }

    static FakeJmsMessage of(String payload, Map<String, Object> headers) {
        return new FakeJmsMessage(payload, headers);
    }
}
