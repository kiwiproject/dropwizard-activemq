package org.kiwiproject.dropwizard.activemq.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

@DisplayName("DynamicDestinations")
class DynamicDestinationsTest {

    @Test
    void shouldBuildDynamicDestinations_WhenGivenEmptyLists() {
        assertThat(DynamicDestinations.buildDynamicDestination(List.of(), List.of())).isEmpty();
    }

    @Test
    void shouldBuildDynamicDestinations_WhenGivenOnlyTopics() {
        assertThat(DynamicDestinations.buildDynamicDestination(List.of("topicA", "topicB"), List.of()))
                .isEqualTo("*:topicA,topicB");
    }

    @Test
    void shouldBuildDynamicDestinations_WhenGivenOnlyQueues() {
        assertThat(DynamicDestinations.buildDynamicDestination(List.of(), List.of("queueA", "queueB")))
                .isEqualTo("*:queue://queueA,queue://queueB");
    }

    @Test
    void shouldBuildDynamicDestinations_WhenGivenOnlyTopicsAndQueues() {
        var topics = List.of("topicA", "topicB");
        var queues = List.of("queueA", "queueB");
        assertThat(DynamicDestinations.buildDynamicDestination(topics, queues))
                .isEqualTo("*:topicA,topicB,queue://queueA,queue://queueB");
    }
}
