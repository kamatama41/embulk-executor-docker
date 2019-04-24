package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

class P12File {
    private final String path;
    private final String password;

    @JsonCreator
    P12File(@JsonProperty("path") String path, @JsonProperty("password") String password) {
        if (path == null || password == null) {
            throw new NullPointerException("Path and password must not be null");
        }
        this.path = path;
        this.password = password;
    }

    @JsonProperty
    String getPath() {
        return path;
    }

    @JsonProperty
    String getPassword() {
        return password;
    }
}
