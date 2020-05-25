# dgraph-ws-client

This is a dGraph gRPC client wrapped with WebSocket protocol written in Rust.

# Motivation

We have previously implemented [dgraph-js-native](https://github.com/xanthous-tech/dgraph-js-native) but was not getting enough performance boost due to [not being able to invoke JS method in Rust thread in Neon](https://github.com/neon-bindings/neon/issues/197). The poll-based approach was good but was simply wasting cycles and hurt the performance. I think the only other way to get better performance is to use WebSocket to wrap the gRPC client, and have this live alongside of dGraph Alpha's so the JS side can just use WebSockets to communicate.

# Sponsor

[Treelab](https://www.treelab.com.cn)

# License

[MIT](./LICENSE)
