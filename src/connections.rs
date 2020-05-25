use std::sync::Arc;

use tungstenite::{Message};
use futures_util::{SinkExt, StreamExt};
use log::{info, debug, error};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use dgraph_tonic::{Query, Mutate};

use crate::models::{ResponsePayload};
use crate::txn::{
  discard_txn,
  process_query_txn_request,
  process_mutate_txn_request,
};

pub async fn accept_query_txn_connection<Q>(stream: TcpStream, txn_arc_mutex: Arc<Mutex<Option<Q>>>) where Q: Query {
  let addr = stream
    .peer_addr()
    .expect("connected streams should have a peer address");

  info!("Peer address {} requesting query txn", addr);

  let ws_stream = tokio_tungstenite::accept_async(stream)
    .await
    .expect("Error during the websocket handshake occurred");

  info!("New WebSocket connection: {}", addr);

  let (mut sender, mut receiver) = ws_stream.split();

  while let Some(message) = receiver.next().await {
    // TODO: better error message by capturing the receive error
    let _result = match message {
      Err(receive_error) => {
        error!("{:?}", receive_error);
        let payload = ResponsePayload {
          error: Some(format!("Message Receive Error: {:?}", receive_error)),
          message: None,
          json: None,
          uids_map: None,
        };

        sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await
      },
      Ok(m) => match m {
        Message::Ping(ping) => { sender.send(Message::Pong(ping)).await },
        Message::Pong(_) => Ok(()),
        Message::Close(_) => Ok(()),
        Message::Text(t) => {
          let response = process_query_txn_request(txn_arc_mutex.clone(), t).await;
          match response {
            Ok(payload) => sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await,
            Err(err) => {
              let payload = ResponsePayload {
                error: Some(format!("Txn Error: {:?}", err)),
                message: None,
                json: None,
                uids_map: None,
              };

              sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await
            }
          }
        },
        Message::Binary(b) => {
          // TODO: process binary data as needed
          sender.send(Message::Binary(b)).await
        },
      },
    };
  }
}

pub async fn accept_mutate_txn_connection<M>(stream: TcpStream, txn_arc_mutex: Arc<Mutex<Option<M>>>) where M: Mutate {
  let addr = stream
    .peer_addr()
    .expect("connected streams should have a peer address");
  info!("Peer address: {}", addr);

  let ws_stream = tokio_tungstenite::accept_async(stream)
    .await
    .expect("Error during the websocket handshake occurred");

  info!("New WebSocket connection: {}", addr);

  let (mut sender, mut receiver) = ws_stream.split();

  while let Some(message) = receiver.next().await {
    // TODO: better error message by capturing the receive error
    let _result = match message {
      Err(receive_error) => {
        error!("{:?}", receive_error);
        debug!("discarding txn on error");
        let response = discard_txn(txn_arc_mutex.clone()).await;
        match response {
          Ok(payload) => sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await,
          Err(err) => {
            let payload = ResponsePayload {
              error: Some(format!("Txn Error: {:?}", err)),
              message: None,
              json: None,
              uids_map: None,
            };

            sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await
          }
        }
      },
      Ok(m) => match m {
        Message::Ping(ping) => { sender.send(Message::Pong(ping)).await },
        Message::Pong(_) => { Ok(()) },
        Message::Close(_) => {
          debug!("discarding txn on close");
          let response = discard_txn(txn_arc_mutex.clone()).await;
          match response {
            Ok(payload) => sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await,
            Err(err) => {
              let payload = ResponsePayload {
                error: Some(format!("Txn Error: {:?}", err)),
                message: None,
                json: None,
                uids_map: None,
              };

              sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await
            }
          }
        },
        Message::Text(t) => {
          let response = process_mutate_txn_request(txn_arc_mutex.clone(), t).await;
          match response {
            Ok(payload) => sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await,
            Err(err) => {
              let payload = ResponsePayload {
                error: Some(format!("Txn Error: {:?}", err)),
                message: None,
                json: None,
                uids_map: None,
              };

              sender.send(Message::Text(serde_json::to_string(&payload).unwrap_or_default())).await
            }
          }
        },
        Message::Binary(b) => {
          // TODO: process binary data as needed
          sender.send(Message::Binary(b)).await
        },
      },
    };
  }
}
