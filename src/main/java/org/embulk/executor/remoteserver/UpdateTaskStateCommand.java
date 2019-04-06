package org.embulk.executor.remoteserver;

import com.github.kamatama41.nsocket.Command;
import com.github.kamatama41.nsocket.Connection;

class UpdateTaskStateCommand implements Command<UpdateTaskStateData> {
    static final String ID = "update_task_state";
    private final ClientSession session;

    UpdateTaskStateCommand(ClientSession session) {
        this.session = session;
    }

    @Override
    public void execute(UpdateTaskStateData data, Connection connection) throws Exception {
        session.update(data);
    }

    @Override
    public String getId() {
        return ID;
    }
}
