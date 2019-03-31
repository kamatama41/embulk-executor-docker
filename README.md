# Remote server executor plugin for Embulk

Embulk executor plugin to run plugins on remote server.

## Overview

* **Plugin type**: executor

### Notes
- It's still very experimental version, so might change its spec without notification. 
- It might have performance issues or bugs. I would appreciate it if you use this and give me reports/feedback!

## Configuration

- **hosts**: List of remote servers. If not specified, the executor runs as local mode, which start Embulk server on its own process (array of string)
- **timeout_seconds**: Timeout seconds of the whole execution (integer, default: `3600`)

## Example

```yaml
exec:
  type: remoteserver
  hosts:
    - {name:remote-server1.com, port:30001}
    - {name:remote-server2.com, port:30001}
  timeout_seconds: 86400
```

## Running Embulk server as Docker container
Its image is hosted by [DockerHub](https://cloud.docker.com/repository/docker/kamatama41/embulk-executor-remoteserver)
You can try running Embulk server by the following command. 

```sh
$ docker run --rm -p 30001:30001 kamatama41/embulk-executor-remoteserver
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
