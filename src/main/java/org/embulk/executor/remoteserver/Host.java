package org.embulk.executor.remoteserver;

import java.net.InetSocketAddress;

class Host {
    private String name;
    private int port;

    Host(String name, int port) {
        this.name = name;
        this.port = port;
    }

    String getName() {
        return name;
    }

    int getPort() {
        return port;
    }

    InetSocketAddress toAddress() {
        return new InetSocketAddress(name, port);
    }

    static Host of(String host) {
        String[] split = host.split(":");
        if (split.length > 2) {
            throw new IllegalArgumentException("Host must be the format 'hostname(:port)' but " + host);
        }
        if (split.length == 1) {
            return new Host(split[0], 30001);
        } else {
            return new Host(split[0], Integer.parseInt(split[1]));
        }
    }
}
