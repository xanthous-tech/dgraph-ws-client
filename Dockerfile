# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM registry.cn-zhangjiakou.aliyuncs.com/caminer/rust_1:1.47.0 as cargo-build

#RUN apt-get update

#RUN apt-get install libssl-dev pkg-config -y
RUN rustup component add rustfmt --toolchain 1.47.0-x86_64-unknown-linux-gnu
#WORKDIR /usr/src/dgraph-ws-client

COPY . .

RUN cargo build --release

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM registry.cn-zhangjiakou.aliyuncs.com/caminer/debian:buster-slim

ENV RUST_LOG=info
ENV DGRAPH_ALPHAS=http://localhost:9080
ENV LISTEN_ADDRESS=0.0.0.0:9000
ENV CONNECTION_CHECK_INTERVAL=5000
ENV CONNECTION_CHECK_RETRY=3

EXPOSE 9000

RUN apt-get update && apt-get install libssl1.1 -y

WORKDIR /opt/dgraph-ws-client

COPY --from=cargo-build /usr/src/dgraph-ws-client/target/release/dgraph-ws-client .

CMD ["./dgraph-ws-client"]
