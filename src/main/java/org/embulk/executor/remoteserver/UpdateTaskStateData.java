package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class UpdateTaskStateData {
    private String sessionId;
    private int taskIndex;
    private TaskState taskState;
    private String taskReport;
    private String errorMessage;

    @JsonCreator
    UpdateTaskStateData(
            @JsonProperty("sessionId") String sessionId,
            @JsonProperty("taskIndex") int taskIndex,
            @JsonProperty("taskState") TaskState taskState) {
        this.sessionId = sessionId;
        this.taskIndex = taskIndex;
        this.taskState = taskState;
    }

    @JsonProperty
    String getSessionId() {
        return sessionId;
    }

    @JsonProperty
    int getTaskIndex() {
        return taskIndex;
    }

    @JsonProperty
    TaskState getTaskState() {
        return taskState;
    }

    @JsonProperty
    String getTaskReport() {
        return taskReport;
    }

    void setTaskReport(String taskReport) {
        this.taskReport = taskReport;
    }

    @JsonProperty
    String getErrorMessage() {
        return errorMessage;
    }

    void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
