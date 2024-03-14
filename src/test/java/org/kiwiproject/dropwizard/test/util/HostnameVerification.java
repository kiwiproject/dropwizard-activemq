package org.kiwiproject.dropwizard.test.util;

import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;

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
