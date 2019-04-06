package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Connection;
import com.github.kamatama41.nsocket.SyncCommand;

public class RemoveSessionCommand implements SyncCommand<String, Void> {
    static final String ID = "remove_session";
    private final ServerSessionRegistry sessionRegistry;

    RemoveSessionCommand(ServerSessionRegistry sessionRegistry) {
        this.sessionRegistry = sessionRegistry;
    }

    @Override
    public Void apply(String sessionId, Connection connection) {
        sessionRegistry.remove(sessionId);
        return null;
    }

    @Override
    public String getId() {
        return ID;
    }
}
