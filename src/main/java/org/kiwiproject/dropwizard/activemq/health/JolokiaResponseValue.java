package org.kiwiproject.dropwizard.activemq.health;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import org.kiwiproject.dropwizard.activemq.internal.DestinationIdentifier;
import org.kiwiproject.json.FlexibleJsonModel;

import java.util.Map;

/**
 * Represent the "important" bits of the JMX information provided.
 * <p>
 * There are far more fields than this, so we should add to these
 * properties if we find use for others in the future.
 */
@Getter
@Setter
public class JolokiaResponseValue extends FlexibleJsonModel {

    private DestinationIdentifier.DestinationInfo destinationInfo;  // additional value holder

    @JsonProperty("Name")
    private String name;

    @JsonProperty("QueueSize")
    private Long queueSize;

    @JsonProperty("EnqueueCount")
    private Long enqueueCount;

    @JsonProperty("DequeueCount")
    private Long dequeueCount;

    @JsonProperty("BlockedSends")
    private Long blockedSends;

    @JsonProperty("ConsumerCount")
    private Long consumerCount;

    @JsonProperty("ProducerCount")
    private Long producerCount;

    @JsonIgnore  // this is here to exclude the data from the serialized JSON (it gets verbose)
    @JsonProperty("MessageGroups")
    private Map<String, Object> messageGroups;
}
