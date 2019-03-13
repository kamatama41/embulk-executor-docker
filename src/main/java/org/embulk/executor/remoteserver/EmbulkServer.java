package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.SocketServer;

import java.io.IOException;

public class EmbulkServer implements AutoCloseable {
    private SocketServer server;

    EmbulkServer(SocketServer  server) {
        this.server = server;
    }

    static EmbulkServer start(String host, int port, int numOfWorkers) throws IOException {
        SocketServer server = new SocketServer();
        SessionManager sessionManager = new SessionManager();
        server.setHost(host);
        server.setPort(port);
        server.setNumOfWorkers(numOfWorkers);
        server.registerSyncCommand(new InitializeSessionCommand(sessionManager));
        server.registerSyncCommand(new RemoveSessionCommand(sessionManager));
        server.registerCommand(new StartTaskCommand(sessionManager));
        server.start();
        return new EmbulkServer(server);
    }

    @Override
    public void close() throws IOException {
        server.stop();
    }
}
