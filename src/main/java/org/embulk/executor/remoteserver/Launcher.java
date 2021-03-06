package org.embulk.executor.remoteserver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class Launcher {
    public static void main(String[] args) throws IOException {
        Map<String, String> envVars = System.getenv();
        String host = envVars.getOrDefault("BIND_ADDRESS", "0.0.0.0");
        int port = Integer.parseInt(envVars.getOrDefault("PORT", "30001"));
        int numOfWorkers = Integer.parseInt(envVars.getOrDefault("NUM_OF_WORKERS", "5"));
        TLSConfig tlsConfig = createTLSConfig(envVars);
        configureLogLevel(envVars);

        EmbulkServer.start(host, port, numOfWorkers, tlsConfig);
    }

    private static TLSConfig createTLSConfig(Map<String, String> envVars) {
        if (!"true".equals(envVars.get("USE_TLS"))) {
            return null;
        }

        TLSConfig tlsConfig = new TLSConfig();
        String keyP12Path = envVars.get("CERT_P12_PATH");
        String keyP12Password = envVars.get("CERT_P12_PASSWORD");
        if (keyP12Path != null && keyP12Password != null) {
            tlsConfig.setKeyStore(new P12File(keyP12Path, keyP12Password));
        }

        String caCertPath = envVars.get("CA_CERT_PATH");
        if (caCertPath != null) {
            tlsConfig.setCaCertPath(caCertPath);
        }

        tlsConfig.setEnableClientAuth("true".equals(envVars.get("REQUIRE_TLS_CLIENT_AUTH")));
        return tlsConfig;
    }

    private static void configureLogLevel(Map<String, String> envVars) {
        Level logLevel = Level.toLevel(envVars.getOrDefault("LOG_LEVEL", "info"));
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(logLevel);
    }
}
