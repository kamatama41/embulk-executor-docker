package org.embulk.executor.remoteserver;

public enum TaskState {
    STARTED, INPUT_COMMITTED, OUTPUT_COMMITTED, FINISHED, FAILED
}
