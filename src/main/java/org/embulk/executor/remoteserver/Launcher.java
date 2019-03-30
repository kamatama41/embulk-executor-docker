package org.embulk.executor.remoteserver;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Launcher {
    public static void main(String[] args) throws IOException {
        String host = "0.0.0.0";
        int port = 30001;
        int numOfWorkers = 1;
        Level logLevel = Level.INFO;
        if (args.length == 4) {
            host = args[0];
            port = Integer.parseInt(args[1]);
            numOfWorkers = Integer.parseInt(args[2]);
            logLevel = Level.toLevel(args[3]);
        }
        configureLogLevel(logLevel);
        EmbulkServer.start(host, port, numOfWorkers);
    }

    private static void configureLogLevel(Level logLevel) {
        Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(logLevel);
    }
}
