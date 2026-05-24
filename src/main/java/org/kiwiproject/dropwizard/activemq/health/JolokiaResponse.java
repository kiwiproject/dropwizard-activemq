package org.kiwiproject.dropwizard.activemq.health;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.kiwiproject.json.FlexibleJsonModel;

import java.util.Map;

/**
 * Model for the JSON response returned by the Jolokia HTTP API, used by
 * {@link DeadLetterQueueHealthCheck} to query ActiveMQ queue state.
 */
@Getter
@Setter
public class JolokiaResponse extends FlexibleJsonModel {

    private Map<String, String> request;
    private Map<String, JolokiaResponseValue> value;
    private Long timestamp;
    private Long status;

    @JsonProperty("error_type")
    private String errorType;

    private String error;
}
