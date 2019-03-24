package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.Command;
import com.github.kamatama41.nsocket.Connection;

class NotifyTaskStateCommand implements Command<UpdateTaskStateData> {
    static final String ID = "notify_task_state";
    private final SessionManager sessionManager;

    NotifyTaskStateCommand(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
    }

    @Override
    public void execute(UpdateTaskStateData data, Connection connection) throws Exception {
        SessionManager.State state = sessionManager.getState(data.getSessionId());
        state.update(data);
    }

    @Override
    public String getId() {
        return ID;
    }
}
