package org.kiwiproject.dropwizard.activemq;

import static org.kiwiproject.dropwizard.activemq.test.util.TestObjectFactory.uniqueServiceName;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfig;
import org.kiwiproject.dropwizard.activemq.config.ActiveMqConfigured;

import io.dropwizard.core.Configuration;

@Getter
@Setter
@Builder
public class TestAppConfig extends Configuration implements ActiveMqConfigured {

    private ActiveMqConfig activeMqConfig;

    @Builder.Default
    private String serviceName = uniqueServiceName();

    @Builder.Default
    private boolean elucidationEnabled = true;
}
