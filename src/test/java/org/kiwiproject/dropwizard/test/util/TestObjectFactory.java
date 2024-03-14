package org.kiwiproject.dropwizard.test.util;

import lombok.experimental.UtilityClass;

import org.kiwiproject.config.TlsContextConfiguration;

@UtilityClass
public class TestObjectFactory {

    public static String uniqueServiceName() {
        return "test-service-" + System.currentTimeMillis();
    }

    public static String keyStorePath() {
        // TODO real impl (and create the test keystore)
        return "test-keystore.jks";
    }

    public static String trustStorePath() {
        // TODO real impl (and create the test truststore)
        return "test-truststore.jks";
    }

    public static TlsContextConfiguration newTlsContextConfiguration() {
        return newTlsContextConfiguration(HostnameVerification.YES);
    }

    public static TlsContextConfiguration newTlsContextConfiguration(HostnameVerification hostnameVerification) {
        return TlsContextConfiguration.builder()
                .protocol("TLSv1.2")
                .keyStorePath(keyStorePath())
                .keyStorePassword("password")
                .trustStorePath(trustStorePath())
                .trustStorePassword("password")
                .verifyHostname(hostnameVerification.verifyHostname)
                .build();
    }
}
