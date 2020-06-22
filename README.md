# dgraph-ws-client

This is a dGraph gRPC client wrapped with WebSocket protocol written in Rust.

# Motivation

We have previously implemented [dgraph-js-native](https://github.com/xanthous-tech/dgraph-js-native) but was not getting enough performance boost due to [not being able to invoke JS method in Rust thread in Neon](https://github.com/neon-bindings/neon/issues/197). The poll-based approach was good but was simply wasting cycles and hurt the performance. I think the only other way to get better performance is to use WebSocket to wrap the gRPC client, and have this live alongside of dGraph Alpha's so the JS side can just use WebSockets to communicate.

# Start
edit `docker-compose.yml` 
```yml
version: "3"

services:
  dgraph-ws-client:
    build: .
    ports:
      - "9000:9000"
    environment:
      DGRAPH_ALPHAS: http://localhost:9080 # replace localhost with your host IP
      RUST_LOG: info
      LISTEN_ADDRESS: 0.0.0.0:9000
      CONNECTION_CHECK_INTERVAL: 5000
      CONNECTION_CHECK_RETRY: 3
```
and run
```shell
$ docker-compose up -d
```

# Sponsor

[Treelab](https://www.treelab.com.cn)

# License

[MIT](./LICENSE)
