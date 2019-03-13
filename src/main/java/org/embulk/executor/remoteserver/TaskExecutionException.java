package org.embulk.executor.remoteserver;

class TaskExecutionException extends Exception {
    TaskExecutionException(String message) {
        super(message);
    }

    TaskExecutionException(Throwable cause) {
        super(cause);
    }
}
