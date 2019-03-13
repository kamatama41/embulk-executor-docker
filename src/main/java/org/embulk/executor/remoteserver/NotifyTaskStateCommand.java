package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Command;
import com.github.kamatama41.nsocket.Connection;

class NotifyTaskStateCommand implements Command<UpdateTaskStateData> {
    static final String ID = "notify_task_state";
    private final SessionState sessionState;

    NotifyTaskStateCommand(SessionState sessionState) {
        this.sessionState = sessionState;
    }

    @Override
    public void execute(UpdateTaskStateData data, Connection connection) throws Exception {
        sessionState.update(data);
    }

    @Override
    public String getId() {
        return ID;
    }
}
