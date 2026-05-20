package org.kiwiproject.dropwizard.activemq.test.util;

import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;

public enum HostnameVerification {

    YES(true, DefaultHostnameVerifier.class),
    NO(false, NoopHostnameVerifier.class);

    public final boolean verifyHostname;
    public final Class<?> hostnameVerifierClass;

    HostnameVerification(boolean verifyHostname, Class<?> hostnameVerifierClass) {
        this.verifyHostname = verifyHostname;
        this.hostnameVerifierClass = hostnameVerifierClass;
    }
}
