use std::sync::Arc;

use dgraph_tonic::Client;
use hyper::upgrade::Upgraded;
use log::debug;
use tokio::sync::Mutex;

use crate::connections::{accept_mutate_txn_connection, accept_query_txn_connection};

pub async fn create_read_only_txn_channel(upgraded: Upgraded, client: Arc<Client>) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_read_only_txn())));
    debug!("creating new read only txn");
    accept_query_txn_connection(stream, txn_arc_mutex).await
}

pub async fn create_best_effort_txn_channel(upgraded: Upgraded, client: Arc<Client>) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_best_effort_txn())));
    debug!("creating new best effort txn");
    accept_query_txn_connection(stream, txn_arc_mutex).await
}

pub async fn create_mutated_txn_channel(upgraded: Upgraded, client: Arc<Client>) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_mutated_txn())));
    debug!("creating new mutated txn");
    accept_mutate_txn_connection(stream, txn_arc_mutex).await
}
