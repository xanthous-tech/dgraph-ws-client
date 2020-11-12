use tungstenite::protocol::WebSocketConfig;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use dgraph_tonic::Client;
use futures::future::{select, FutureExt, TryFutureExt};
use futures::StreamExt;
use hyper::upgrade::Upgraded;
use log::debug;
use redis::{AsyncCommands, RedisError, aio::MultiplexedConnection};
use tokio::sync::{oneshot, Mutex};

use crate::connections::{
    accept_mutate_txn_connection, accept_query_txn_connection, auto_close_connection,
};

fn get_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        max_send_queue: None,
        max_message_size: Some(2048 << 20),  // 2GB
        max_frame_size: Some(2048 << 20),    // 2GB
    }
}

pub async fn create_read_only_txn_channel(upgraded: Upgraded, client: Arc<Client>, disable_auto_close: bool) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        Some(get_websocket_config()),
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_read_only_txn())));

    let (sender, receiver) = stream.split();
    let sender_arc_mutex = Arc::new(Mutex::new(Some(sender)));
    let query_count = Arc::new(AtomicU32::new(0));

    let (shutdown_hook, shutdown) = oneshot::channel::<()>();
    let shutdown_hook_arc_mutex = Arc::new(Mutex::new(Some(shutdown_hook)));

    if !disable_auto_close {
        tokio::spawn(select(
            auto_close_connection(sender_arc_mutex.clone(), query_count.clone()).boxed(),
            shutdown.map_err(drop),
        ));
    }

    debug!("creating new read only txn");
    accept_query_txn_connection(
        sender_arc_mutex,
        receiver,
        txn_arc_mutex,
        shutdown_hook_arc_mutex.clone(),
        query_count.clone(),
    )
    .await
}

pub async fn create_best_effort_txn_channel(upgraded: Upgraded, client: Arc<Client>, disable_auto_close: bool) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        Some(get_websocket_config()),
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_best_effort_txn())));

    let (sender, receiver) = stream.split();
    let sender_arc_mutex = Arc::new(Mutex::new(Some(sender)));
    let query_count = Arc::new(AtomicU32::new(0));

    let (shutdown_hook, shutdown) = oneshot::channel::<()>();
    let shutdown_hook_arc_mutex = Arc::new(Mutex::new(Some(shutdown_hook)));

    if !disable_auto_close {
        tokio::spawn(select(
            auto_close_connection(sender_arc_mutex.clone(), query_count.clone()).boxed(),
            shutdown.map_err(drop),
        ));
    }

    debug!("creating new best effort txn");
    accept_query_txn_connection(
        sender_arc_mutex,
        receiver,
        txn_arc_mutex,
        shutdown_hook_arc_mutex.clone(),
        query_count.clone(),
    )
    .await
}

pub async fn create_mutated_txn_channel(upgraded: Upgraded, client: Arc<Client>, txn_id: String, mut redis_connection: MultiplexedConnection, disable_auto_close: bool) {
    let stream = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded,
        tungstenite::protocol::Role::Server,
        Some(get_websocket_config()),
    )
    .await;
    let txn_arc_mutex = Arc::new(Mutex::new(Some(client.new_mutated_txn())));

    let (sender, receiver) = stream.split();
    let sender_arc_mutex = Arc::new(Mutex::new(Some(sender)));
    let query_count = Arc::new(AtomicU32::new(0));

    let (shutdown_hook, shutdown) = oneshot::channel::<()>();
    let shutdown_hook_arc_mutex = Arc::new(Mutex::new(Some(shutdown_hook)));

    if !disable_auto_close {
        tokio::spawn(select(
            auto_close_connection(sender_arc_mutex.clone(), query_count.clone()).boxed(),
            shutdown.map_err(drop),
        ));
    }

    let _inc_txn_result: Result<Vec<u8>, RedisError> = redis_connection.xadd(
        "incoming_txns",
        "*",
        &[
            ("txnId", &txn_id.clone()),
        ],
    ).await;

    let _result: Result<Vec<u8>, RedisError> = redis_connection.xadd(
        format!("txn:{:}", txn_id.clone()),
        "*",
        &[("event", "txn_started")],
    ).await;

    debug!("creating new mutated txn");
    accept_mutate_txn_connection(
        sender_arc_mutex,
        receiver,
        txn_arc_mutex,
        shutdown_hook_arc_mutex.clone(),
        query_count.clone(),
        txn_id,
        redis_connection.clone(),
    )
    .await
}
