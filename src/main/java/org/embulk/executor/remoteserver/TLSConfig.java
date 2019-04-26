package org.embulk.executor.remoteserver;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;

class TLSConfig {
    private P12File keyStore = null;
    private String caCertPath = null;
    private boolean enableClientAuth = false;

    TLSConfig() {
    }

    void setKeyStore(P12File keyStore) {
        this.keyStore = keyStore;
    }

    void setEnableClientAuth(boolean enableClientAuth) {
        this.enableClientAuth = enableClientAuth;
    }

    void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }

    SSLContext getSSLContext() {
        try {
            KeyManager[] keyManagers = null;
            if (keyStore != null) {
                KeyStore ks = loadKeyStore(keyStore);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keyStore.getPassword().toCharArray());
                keyManagers = kmf.getKeyManagers();
            }

            TrustManager[] trustManagers = null;
            if (caCertPath != null) {
                KeyStore ts = loadTrustStore(caCertPath);
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

    private static KeyStore loadKeyStore(P12File file) {
        try (InputStream keyStoreIS = new FileInputStream(file.getPath())) {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(keyStoreIS, file.getPassword().toCharArray());
            return ks;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static KeyStore loadTrustStore(String path) throws Exception {
        try (FileInputStream inputStream = new FileInputStream(path)) {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(null, null);
            ks.setCertificateEntry("ca_cert", cf.generateCertificate(inputStream));
            return ks;
        }
    }
}
