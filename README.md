# Remote server executor plugin for Embulk

Embulk executor plugin to run Embulk tasks on remote servers. 

## Overview

* **Plugin type**: executor

### Notes
- It's still very experimental version, so might change its spec without notification. 
- It might have performance issues or bugs. I would appreciate it if you use this and give me reports/feedback!

## Configuration

- **hosts**: List of remote servers (`hostname` or `hostname:port`, default port is `30001`). If not specified, the executor runs as local mode, which start Embulk server on its own process (array of string)
- **timeout_seconds**: Timeout seconds of the whole execution (integer, default: `3600`)

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
The server recieves requests from client (Embulk) and run Embulk tasks, then returns results to client. It communicates with clients via `TCP 30001 port`. 

### Running Embulk server as a Docker container
The image is hosted by [DockerHub](https://hub.docker.com/r/kamatama41/embulk-executor-remoteserver).  
You can try running Embulk server by the following command. 

```sh
$ docker run --rm -p 30001:30001 kamatama41/embulk-executor-remoteserver
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
