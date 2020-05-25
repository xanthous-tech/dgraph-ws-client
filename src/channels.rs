use std::sync::Arc;

use log::{info, debug};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use dgraph_tonic::Client;

use crate::connections::{
  accept_query_txn_connection,
  accept_mutate_txn_connection,
};

pub async fn create_read_only_txn_channel(addr: &str, client: Arc<Client>) {
  // Create the event loop and TCP listener we'll accept connections on.
  let try_socket = TcpListener::bind(addr).await;
  let mut listener = try_socket.expect("Failed to bind");
  info!("Read-Only Txn Channel Listening on: {}", addr);

  while let Ok((stream, _)) = listener.accept().await {
    debug!("creating new read-only txn");
    let txn = Arc::new(Mutex::new(Some(client.new_read_only_txn())));
    tokio::spawn(accept_query_txn_connection(stream, txn));
  }
}

pub async fn create_best_effort_txn_channel(addr: &str, client: Arc<Client>) {
  // Create the event loop and TCP listener we'll accept connections on.
  let try_socket = TcpListener::bind(addr).await;
  let mut listener = try_socket.expect("Failed to bind");
  info!("Best-Effort Txn Channel Listening on: {}", addr);

  while let Ok((stream, _)) = listener.accept().await {
    debug!("creating new best-effort txn");
    let txn = Arc::new(Mutex::new(Some(client.new_best_effort_txn())));
    tokio::spawn(accept_query_txn_connection(stream, txn));
  }
}

pub async fn create_mutated_txn_channel(addr: &str, client: Arc<Client>) {
  // Create the event loop and TCP listener we'll accept connections on.
  let try_socket = TcpListener::bind(addr).await;
  let mut listener = try_socket.expect("Failed to bind");
  info!("Mutated Txn Channel Listening on: {}", addr);

  while let Ok((stream, _)) = listener.accept().await {
    debug!("creating new mutated txn");
    let txn = Arc::new(Mutex::new(Some(client.new_mutated_txn())));
    tokio::spawn(accept_mutate_txn_connection(stream, txn));
  }
}
