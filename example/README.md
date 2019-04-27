## How to run

Run `embulk bundle`

```sh
$ embulk bundle --path vendor/bundle
```

Run `docker-compose up`

```sh
$ docker-compose up
Creating example_remote1_1 ... done
Attaching to example_remote1_1
remote1_1  | 15:11:59.045 [main] INFO  c.g.kamatama41.nsocket.SocketServer - Starting server..
```

In another session, run `embulk run`

```sh
$ embulk run config.yml -b .
```

So that 2 output files would be generated in `work` directory

```sh
$ cat work/output_*
id,name,time
1,Scott,2019-04-06 15:12:42.029000 +0000
id,name,time
2,Tiger,2019-04-06 15:12:42.029000 +0000
```
