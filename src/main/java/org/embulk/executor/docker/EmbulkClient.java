package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.SocketClient;
import org.embulk.config.ModelManager;

import java.io.IOException;

class EmbulkClient extends SocketClient implements AutoCloseable {
    private final SessionManager sessionManager;

    EmbulkClient(ModelManager modelManager) {
        this.sessionManager = new SessionManager(modelManager);
    }

    @Override
    public synchronized void open() throws IOException {
        registerSyncCommand(new CreateSessionCommand());
        registerCommand(new NotifyTaskStateCommand(sessionManager));
        super.open();
    }

    SessionManager getSessionManager() {
        return sessionManager;
    }
}
