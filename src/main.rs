#[macro_use]
extern crate anyhow;
#[macro_use]
extern crate serde_json;

use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};

use tungstenite::{Message};
use futures_util::{SinkExt, StreamExt};
use log::{info, debug, error};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use dgraph_tonic::{Client, Query, Mutate, Mutation};

#[derive(Serialize, Deserialize, Debug)]
struct QueryPayload {
  pub q: String,
  // #[serde(skip_serializing_if = "Option::is_none")]
  pub vars: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct MutationPayload {
  #[serde(rename = "setJson")]
  pub set_json: Option<Vec<u8>>,
  #[serde(rename = "deleteJson")]
  pub delete_json: Option<Vec<u8>>,
  #[serde(rename = "setNquads")]
  pub set_nquads: Option<Vec<u8>>,
  #[serde(rename = "delNquads")]
  pub del_nquads: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct RequestPayload {
  pub query: Option<QueryPayload>,
  pub mutate: Option<MutationPayload>,
  pub commit: Option<bool>,
}

async fn create_read_only_txn_channel(addr: &str, client: Arc<Client>) {
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

async fn create_best_effort_txn_channel(addr: &str, client: Arc<Client>) {
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

async fn create_mutated_txn_channel(addr: &str, client: Arc<Client>) {
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

#[tokio::main]
async fn main() {
  let _ = env_logger::try_init();

  let client = Arc::new(Client::new("http://localhost:9080").expect("dgraph client"));

  let handle1 = tokio::spawn(create_read_only_txn_channel("127.0.0.1:9001", client.clone()));
  let handle2 = tokio::spawn(create_best_effort_txn_channel("127.0.0.1:9002", client.clone()));
  let handle3 = tokio::spawn(create_mutated_txn_channel("127.0.0.1:9003", client.clone()));

  let _responses = tokio::try_join!(handle1, handle2, handle3);
}

async fn accept_query_txn_connection<Q>(stream: TcpStream, txn: Arc<Mutex<Option<Q>>>) where Q: Query {
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
    let _result = match message {
      Ok(m) => match m {
        Message::Text(t) => { sender.send(Message::Text(t)).await },
        Message::Binary(b) => { sender.send(Message::Binary(b)).await },
        Message::Ping(ping) => { sender.send(Message::Ping(ping)).await },
        Message::Pong(pong) => { sender.send(Message::Pong(pong)).await },
        Message::Close(Some(frame)) => { sender.send(Message::Close(Some(frame))).await },
        Message::Close(None) => { sender.send(Message::Close(None)).await },
      },
      Err(_) => {
        info!("{:?}", message);
        Ok(())
      },
    };
  }
}

async fn accept_mutate_txn_connection<M>(stream: TcpStream, txn_arc_mutex: Arc<Mutex<Option<M>>>) where M: Mutate {
  let addr = stream
    .peer_addr()
    .expect("connected streams should have a peer address");
  info!("Peer address: {}", addr);

  let ws_stream = tokio_tungstenite::accept_async(stream)
    .await
    .expect("Error during the websocket handshake occurred");

  info!("New WebSocket connection: {}", addr);

  let (mut sender, mut receiver) = ws_stream.split();

  let mut txn_guard = txn_arc_mutex.lock().await;

  while let Some(message) = receiver.next().await {
    let _result = match message {
      Err(receive_error) => {
        error!("{:?}", receive_error);
        debug!("discarding txn on error");
        let txn = txn_guard.take();
        match txn {
          Some(t) => {
            let result = t.discard().await;
            match result {
              Ok(_) => {
                debug!("txn discarded");
              },
              Err(e) => {
                error!("DgraphError {:?}", e);
              }
            }
          },
          None => {},
        };
        Ok(())
      },
      Ok(m) => match m {
        Message::Ping(ping) => { sender.send(Message::Pong(ping)).await },
        Message::Pong(_) => { Ok(()) },
        Message::Close(_) => {
          debug!("discarding txn on close");
          let txn = txn_guard.take();
          match txn {
            Some(t) => {
              let result = t.discard().await;
              match result {
                Ok(_) => {
                  debug!("txn discarded");
                  sender.send(Message::Text(format!("discarded"))).await
                },
                Err(e) => { sender.send(Message::Text(format!("DgraphError {:?}", e))).await }
              }
            },
            None => { sender.send(Message::Text(format!("Txn is empty"))).await },
          }
        },
        Message::Text(t) => {
          let parsed: Result<RequestPayload, _> = serde_json::from_str(t.as_str());
          match parsed {
            Err(e) => { sender.send(Message::Text(format!("parse request error {:?}", e))).await },
            Ok(request) => {
              debug!("incoming request {:?}", request);
              match request.query {
                Some(query) => {
                  let txn = txn_guard.as_mut();
                  let vars = match query.vars {
                    Some(v) => v,
                    None => HashMap::new(),
                  };
                  match txn {
                    Some(t) => {
                      let response = t.query_with_vars(query.q, vars).await;
                      match response {
                        Ok(r) => {
                          sender.send(Message::Text(String::from_utf8(r.json).unwrap_or_default())).await
                        },
                        Err(e) => {
                          sender.send(Message::Text(format!("Dgraph Error {:?}", e).to_string())).await
                        }
                      }
                    },
                    None => {
                      sender.send(Message::Text(format!("Empty Txn").to_string())).await
                    }
                  }
                },
                None => {
                  match request.mutate {
                    Some(mutate) => {
                      let mut mutation = Mutation::new();

                      mutation.commit_now = match request.commit {
                        Some(c) => c,
                        None => false,
                      };

                      mutation.set_json = match mutate.set_json {
                        Some(v) => v,
                        None => Default::default(),
                      };

                      mutation.delete_json = match mutate.delete_json {
                        Some(v) => v,
                        None => Default::default(),
                      };

                      mutation.set_nquads = match mutate.set_nquads {
                        Some(v) => v,
                        None => Default::default(),
                      };

                      mutation.del_nquads = match mutate.del_nquads {
                        Some(v) => v,
                        None => Default::default(),
                      };

                      if mutation.commit_now {
                        let txn = txn_guard.take();

                        match txn {
                          Some(t) => {
                            let response = t.mutate_and_commit_now(mutation).await;
                            match response {
                              Ok(r) => {
                                sender.send(Message::Text(String::from_utf8(r.json).unwrap_or_default())).await
                              },
                              Err(e) => {
                                sender.send(Message::Text(format!("Dgraph Error {:?}", e).to_string())).await
                              }
                            }
                          },
                          None => {
                            sender.send(Message::Text(format!("Empty Txn").to_string())).await
                          }
                        }
                      } else {
                        let txn = txn_guard.as_mut();

                        match txn {
                          Some(t) => {
                            let response = t.mutate(mutation).await;
                            match response {
                              Ok(r) => {
                                sender.send(Message::Text(String::from_utf8(r.json).unwrap_or_default())).await
                              },
                              Err(e) => {
                                sender.send(Message::Text(format!("Dgraph Error {:?}", e).to_string())).await
                              }
                            }
                          },
                          None => {
                            sender.send(Message::Text(format!("Empty Txn"))).await
                          }
                        }
                      }
                    },
                    None => {
                      match request.commit {
                        Some(c) => {
                          let txn = txn_guard.take();

                          match txn {
                            Some(t) => {
                              let response = t.commit().await;
                              match response {
                                Ok(r) => {
                                  sender.send(Message::Text(format!("committed"))).await
                                },
                                Err(e) => {
                                  sender.send(Message::Text(format!("Dgraph Error {:?}", e))).await
                                }
                              }
                            },
                            None => {
                              sender.send(Message::Text(format!("Empty Txn"))).await
                            }
                          }
                        },
                        None => {
                          sender.send(Message::Text(t)).await
                        },
                      }
                    },
                  }
                },
              }
            },
          }
        },
        Message::Binary(b) => { sender.send(Message::Binary(b)).await },
      },
    };
  }
}
