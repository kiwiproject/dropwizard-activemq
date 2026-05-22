package org.kiwiproject.dropwizard.activemq.test.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import lombok.experimental.UtilityClass;
import org.kiwiproject.config.TlsContextConfiguration;
import org.kiwiproject.security.KeyStoreType;
import org.kiwiproject.security.SSLContextProtocol;
import org.kiwiproject.test.security.CertificateTestHelpers;
import org.kiwiproject.test.security.TestKeyStores;

@UtilityClass
public class TestObjectFactory {

    private static final String PKCS12 = KeyStoreType.PKCS12.getValue();
    private static final TestKeyStores TEST_KEY_STORES;

    static {
        try {
            var certDir = Files.createTempDirectory("dropwizard-activemq-test-certs");
            TEST_KEY_STORES = CertificateTestHelpers.createKeyAndTrustStores(
                    certDir,
                    PKCS12,
                    PKCS12);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to generate test keystores", e);
        }
    }

    public static String uniqueServiceName() {
        return "test-service-" + System.currentTimeMillis();
    }

    public static String keyStorePath() {
        return TEST_KEY_STORES.requiredKeyStorePathAsString();
    }

    public static String trustStorePath() {
        return TEST_KEY_STORES.requiredTrustStorePathAsString();
    }

    public static TlsContextConfiguration newTlsContextConfiguration() {
        return newTlsContextConfiguration(HostnameVerification.YES);
    }

    public static TlsContextConfiguration newTlsContextConfiguration(HostnameVerification hostnameVerification) {
        return TlsContextConfiguration.builder()
                .protocol(SSLContextProtocol.TLS_1_3.value)
                .keyStorePath(keyStorePath())
                .keyStorePassword(TEST_KEY_STORES.keyStorePassword())
                .keyStoreType(PKCS12)
                .trustStorePath(trustStorePath())
                .trustStorePassword(TEST_KEY_STORES.trustStorePassword())
                .trustStoreType(PKCS12)
                .verifyHostname(hostnameVerification.verifyHostname)
                .build();
    }

    /**
     * When using this in a test, make sure to annotate the test method
     * or class with {@link org.junitpioneer.jupiter.RestoreSystemProperties}.
     */
    public static void setTlsConfigSystemProperties() {
        System.setProperty("kiwi.tls.keyStorePath", keyStorePath());
        System.setProperty("kiwi.tls.keyStorePassword", TEST_KEY_STORES.keyStorePassword());
        System.setProperty("kiwi.tls.keyStoreType", PKCS12);
        System.setProperty("kiwi.tls.trustStorePath", trustStorePath());
        System.setProperty("kiwi.tls.trustStorePassword", TEST_KEY_STORES.trustStorePassword());
        System.setProperty("kiwi.tls.trustStoreType", PKCS12);
    }
}
