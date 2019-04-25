# Remote server executor plugin for Embulk

Embulk executor plugin to run Embulk tasks on remote servers. 

## Overview

* **Plugin type**: executor

### Notes
- It's still very experimental version, so might change its spec without notification. 
- It might have some issues or bugs. I would appreciate it if you use this and give me reports/feedback!

## Configuration

- **hosts**: List of remote servers (`hostname` or `hostname:port`, default port is `30001`). If not specified, the executor runs as the local mode, which starts an Embulk server on its own process (array of string)
- **timeout_seconds**: Timeout seconds of the whole execution (integer, default: `3600`)
- **use_tls**: Enable to connect server over TLS (boolean, default: `false`)
- **cert_p12_file**: Information of a P12 file used as your client certificate. It would be needed when client authentication is enabled on Embulk server.
  - **path**: Path of the file (string, required)
  - **password**: Password of the file (string, required)
- **ca_p12_file**: Information of a P12 file used as CA certificate. It would be needed when Embulk server uses a certificate in which unknown CA signed.
  - **path**: Path of the file (string, required)
  - **password**: Password of the file (string, required)

## Example

```yaml
exec:
  type: remoteserver
  hosts:
    - embulk-server1.local
    - embulk-server2.local:30002
  timeout_seconds: 86400
```

## Embulk server
The server receives requests from client (Embulk) and run Embulk tasks, then returns results to client. It communicates with clients via `TCP 30001 port`. 

### Running Embulk server as a Docker container
The image is hosted by [DockerHub](https://hub.docker.com/r/kamatama41/embulk-executor-remoteserver).  
You can try running Embulk server by the following command. 

```sh
$ docker run --rm -p 30001:30001 kamatama41/embulk-executor-remoteserver
```

### Configure Embulk server
There are some environment variables to configure the server

- `BIND_ADDRESS`: Bind address of the server (default: `0.0.0.0`)
- `PORT`: Port number to listen (default: `30001`)
- `USE_TLS`: Try to connect to client via TLS if `true` (default: `false`)
- `REQUIRE_TLS_CLIENT_AUTH`: Require client authentication if `true` (default: `false`) 
- `CERT_P12_PATH`, `CERT_P12_PASSWORD`: Path and password of the P12 file for server certificate. Ignored unless both is set.
- `CA_P12_PATH`, `CA_P12_PASSWORD`: Path and password of the P12 file for CA certificate. Ignored unless both is set.

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
