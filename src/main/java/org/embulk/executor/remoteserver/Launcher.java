package org.embulk.executor.remoteserver;

import java.io.IOException;

public class Launcher {
    public static void main(String[] args) throws IOException {
        String host = "0.0.0.0";
        int port = 30001;
        int numOfWorkers = 1;
        if (args.length == 3) {
            host = args[0];
            port = Integer.parseInt(args[1]);
            numOfWorkers = Integer.parseInt(args[2]);
        }
        EmbulkServer.start(host, port, numOfWorkers);
    }
}
