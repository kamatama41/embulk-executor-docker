package org.embulk.executor.remoteserver;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;

class TLSConfig {
    private P12File keyStore = null;
    private P12File trustStore = null;
    private boolean enableClientAuth = false;

    TLSConfig() {
    }

    TLSConfig keyStore(P12File keyStore) {
        this.keyStore = keyStore;
        return this;
    }

    TLSConfig trustStore(P12File trustStore) {
        this.trustStore = trustStore;
        return this;
    }

    TLSConfig enableClientAuth(boolean enableClientAuth) {
        this.enableClientAuth = enableClientAuth;
        return this;
    }

    SSLContext getSSLContext() {
        try {
            KeyManager[] keyManagers = null;
            if (keyStore != null) {
                KeyStore ks = load(keyStore);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyStore.getPassword().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            TrustManager[] trustManagers = null;
            if (trustStore != null) {
                KeyStore ts = load(trustStore);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
                trustManagers = tmf.getTrustManagers();
            }

            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, null);
            return context;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    boolean isEnableClientAuth() {
        return enableClientAuth;
    }

    private static KeyStore load(P12File file) {
        try (InputStream keyStoreIS = new FileInputStream(file.getPath())) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(keyStoreIS, file.getPassword().toCharArray());
            return ks;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
