package org.kiwiproject.dropwizard.activemq.test.util;

import lombok.experimental.UtilityClass;

import org.kiwiproject.config.TlsContextConfiguration;
import org.kiwiproject.io.KiwiPaths;

@UtilityClass
public class TestObjectFactory {

    public static String uniqueServiceName() {
        return "test-service-" + System.currentTimeMillis();
    }

    /*
     * Used the following command to generate the keystore:
     *
     * keytool -genkeypair -alias testkey -keyalg RSA -keysize 2048 -keystore test-keystore.jks -dname "CN=Test, OU=Test, O=Test, L=Test, S=Test, C=US" -storepass password -keypass password
     *
     * To create the sample trust store, first exported the above keystore:
     *
     * keytool -export -alias testkey -keystore test-keystore.jks -file testkey.cer -storepass password
     *
     * Finally, created the trust store using:
     *
     * keytool -import -alias testkey -file testkey.cer -keystore test-truststore.jks -storepass password -noprompt
     *
     * Voila!
     */

    public static String keyStorePath() {
        return KiwiPaths.pathFromResourceName("test-keystore.jks").toFile().getAbsolutePath();
    }

    public static String trustStorePath() {
        return KiwiPaths.pathFromResourceName("test-truststore.jks").toFile().getAbsolutePath();
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
