package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.SocketServer;

import java.io.IOException;

public class EmbulkServer extends SocketServer implements AutoCloseable {

    EmbulkServer() throws IOException {}

    @Override
    public synchronized void start() throws IOException {
        registerSyncCommand(new CreateSessionCommand());
        registerCommand(new StartTaskCommand());
        super.start();
    }

    @Override
    public void close() throws IOException {
        this.stop();
    }
}
