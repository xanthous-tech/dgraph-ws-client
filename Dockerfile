# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:1.43.1 as cargo-build

RUN apt-get update

RUN apt-get install libssl-dev pkg-config -y

WORKDIR /usr/src/dgraph-ws-client

COPY . .

RUN cargo build --release

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM debian:buster-slim

ENV RUST_LOG=info
ENV DGRAPH_ALPHAS=http://localhost:9080

EXPOSE 9001
EXPOSE 9002
EXPOSE 9003

RUN apt-get update && apt-get install libssl1.1 -y

WORKDIR /opt/dgraph-ws-client

COPY --from=cargo-build /usr/src/dgraph-ws-client/target/release/dgraph-ws-client .

CMD ["./dgraph-ws-client"]
