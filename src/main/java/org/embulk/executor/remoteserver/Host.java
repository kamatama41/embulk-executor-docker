package org.embulk.executor.remoteserver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetSocketAddress;

class Host {
    private String name;
    private int port;

    @JsonCreator
    Host(@JsonProperty("name") String name,
         @JsonProperty("port") int port) {
        this.name = name;
        this.port = port;
    }

    @JsonProperty
    String getName() {
        return name;
    }

    @JsonProperty
    int getPort() {
        return port;
    }

    InetSocketAddress toAddress() {
        return new InetSocketAddress(name, port);
    }
}
