# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:1.43.1 as cargo-build

RUN apt-get update

RUN apt-get install musl-tools libssl-dev -y

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/dgraph-ws-client

COPY Cargo.toml Cargo.toml

RUN mkdir src/

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

RUN rm -f target/x86_64-unknown-linux-musl/release/deps/dgraph-ws-client*

COPY . .

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM alpine:latest

RUN addgroup -g 1000 dgraphws

RUN adduser -D -s /bin/sh -u 1000 -G dgraphws dgraphws

WORKDIR /home/dgraphws/bin/

COPY --from=cargo-build /usr/src/dgraph-ws-client/target/x86_64-unknown-linux-musl/release/dgraph-ws-client .

RUN chown dgraphws:dgraphws dgraph-ws-client

USER dgraphws

CMD ["./dgraph-ws-client"]
