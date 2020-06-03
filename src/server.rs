use std::sync::Arc;
use std::convert::Infallible;
use std::net::SocketAddr;

use log::{info, error};
use hyper::{Body, Request, Response, Server};
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use dgraph_tonic::Client;

use crate::channels::{
  create_read_only_txn_channel,
  create_best_effort_txn_channel,
  create_mutated_txn_channel,
};

async fn shutdown_signal() {
  // Wait for the CTRL+C signal
  tokio::signal::ctrl_c()
    .await
    .expect("failed to install CTRL+C signal handler");
}

pub async fn build(addr: SocketAddr, client_arc: Arc<Client>) {
  let make_svc = make_service_fn(|socket: &AddrStream| {
    let remote_addr = socket.remote_addr();
    async move {
      Ok::<_, Infallible>(service_fn(move |_: Request<Body>| async move {
        Ok::<_, Infallible>(Response::new(Body::from(format!("Hello, {}", remote_addr))))
      }))
    }
  });

  let server = Server::bind(&addr)
    .serve(make_svc)
    .with_graceful_shutdown(shutdown_signal())
    ;

  if let Err(e) = server.await {
    error!("server error: {}", e);
  }

  // let handle1 = tokio::spawn(create_read_only_txn_channel("0.0.0.0:9001", client_arc.clone()));
  // let handle2 = tokio::spawn(create_best_effort_txn_channel("0.0.0.0:9002", client_arc.clone()));
  // let handle3 = tokio::spawn(create_mutated_txn_channel("0.0.0.0:9003", client_arc.clone()));

  // let _responses = tokio::try_join!(handle1, handle2, handle3);
}
