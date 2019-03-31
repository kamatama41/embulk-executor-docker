# Remote server executor plugin for Embulk

Embulk executor plugin to run plugins on remote server.

## Overview

* **Plugin type**: executor

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


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
