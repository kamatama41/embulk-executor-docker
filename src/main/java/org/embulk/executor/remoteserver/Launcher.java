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
        int numOfWorkers = Integer.parseInt(envVars.getOrDefault("NUM_OF_WORKERS", "1"));
        Level logLevel = Level.toLevel(envVars.getOrDefault("LOG_LEVEL", "info"));
        configureLogLevel(logLevel);
        EmbulkServer.start(host, port, numOfWorkers);
    }

    private static void configureLogLevel(Level logLevel) {
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(logLevel);
    }
}
