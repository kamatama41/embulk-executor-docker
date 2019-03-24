package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.Command;
import com.github.kamatama41.nsocket.Connection;

public class StartTaskCommand implements Command<Integer> {
    static final String ID = "start_task";

    @Override
    public void execute(Integer taskIndex, Connection connection) throws Exception {
        if (connection.attachment() == null) {
            throw new IllegalStateException("Session is not created.");
        }
        Session session = (Session) connection.attachment();
        session.runTask(taskIndex, connection);
    }

    @Override
    public String getId() {
        return ID;
    }
}
