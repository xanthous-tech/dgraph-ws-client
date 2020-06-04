use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dgraph_tonic::{Mutate, Query};
use futures::future::{select, FutureExt, TryFutureExt};
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use hyper::upgrade::Upgraded;
use log::{debug, error, info};
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::WebSocketStream;
use tungstenite::{Error, Message};

use crate::models::ResponsePayload;
use crate::txn::{discard_txn, process_mutate_txn_request, process_query_txn_request};

pub async fn accept_query_txn_connection<Q>(
    stream: WebSocketStream<Upgraded>,
    txn_arc_mutex: Arc<Mutex<Option<Q>>>,
) where
    Q: Query,
{
    let (sender, mut receiver) = stream.split();

    let sender_arc_mutex = Arc::new(Mutex::new(Some(sender)));

    let counter = Arc::new(AtomicU32::new(0));
    let prev_count = Arc::new(AtomicU32::new(0));

    let (shutdown_hook, shutdown) = oneshot::channel::<()>();
    let shutdown_hook_arc_mutex = Arc::new(Mutex::new(Some(shutdown_hook)));
    tokio::spawn(select(
        auto_close_connection(
            sender_arc_mutex.clone(),
            counter.clone(),
            prev_count.clone(),
        )
        .boxed(),
        shutdown.map_err(drop),
    ));

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

                send_message(
                    sender_arc_mutex.clone(),
                    Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                )
                .await
            }
            Ok(m) => match m {
                Message::Ping(ping) => {
                    send_message(sender_arc_mutex.clone(), Message::Pong(ping)).await
                }
                Message::Pong(_) => Ok(()),
                Message::Close(_) => {
                    kill_task(shutdown_hook_arc_mutex.clone()).await;
                    break;
                }
                Message::Text(t) => {
                    increment_counter(counter.clone());
                    let response = process_query_txn_request(txn_arc_mutex.clone(), t).await;
                    match response {
                        Ok(payload) => {
                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                        Err(err) => {
                            let payload = ResponsePayload {
                                error: Some(format!("Txn Error: {:?}", err)),
                                message: None,
                                json: None,
                                uids_map: None,
                            };

                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                    }
                }
                Message::Binary(b) => {
                    // TODO: process binary data as needed
                    send_message(sender_arc_mutex.clone(), Message::Binary(b)).await
                }
            },
        };
    }
}

pub async fn accept_mutate_txn_connection<M>(
    stream: WebSocketStream<Upgraded>,
    txn_arc_mutex: Arc<Mutex<Option<M>>>,
) where
    M: Mutate,
{
    let (sender, mut receiver) = stream.split();

    let sender_arc_mutex = Arc::new(Mutex::new(Some(sender)));

    let counter = Arc::new(AtomicU32::new(0));
    let prev_count = Arc::new(AtomicU32::new(0));

    let (shutdown_hook, shutdown) = oneshot::channel::<()>();
    let shutdown_hook_arc_mutex = Arc::new(Mutex::new(Some(shutdown_hook)));
    tokio::spawn(select(
        auto_close_connection(
            sender_arc_mutex.clone(),
            counter.clone(),
            prev_count.clone(),
        )
        .boxed(),
        shutdown.map_err(drop),
    ));

    while let Some(message) = receiver.next().await {
        // TODO: better error message by capturing the receive error
        let _result = match message {
            Err(receive_error) => {
                error!("{:?}", receive_error);
                debug!("discarding txn on error");
                let response = discard_txn(txn_arc_mutex.clone()).await;
                match response {
                    Ok(payload) => {
                        send_message(
                            sender_arc_mutex.clone(),
                            Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                        )
                        .await
                    }
                    Err(err) => {
                        let payload = ResponsePayload {
                            error: Some(format!("Txn Error: {:?}", err)),
                            message: None,
                            json: None,
                            uids_map: None,
                        };

                        send_message(
                            sender_arc_mutex.clone(),
                            Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                        )
                        .await
                    }
                }
            }
            Ok(m) => match m {
                Message::Ping(ping) => {
                    send_message(sender_arc_mutex.clone(), Message::Pong(ping)).await
                }
                Message::Pong(_) => Ok(()),
                Message::Close(_) => {
                    debug!("discarding txn on close");
                    let response = discard_txn(txn_arc_mutex.clone()).await;
                    let _ = match response {
                        Ok(payload) => {
                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                        Err(err) => {
                            let payload = ResponsePayload {
                                error: Some(format!("Txn Error: {:?}", err)),
                                message: None,
                                json: None,
                                uids_map: None,
                            };

                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                    };

                    kill_task(shutdown_hook_arc_mutex.clone()).await;
                    break;
                }
                Message::Text(t) => {
                    increment_counter(counter.clone());
                    let response = process_mutate_txn_request(txn_arc_mutex.clone(), t).await;
                    match response {
                        Ok(payload) => {
                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                        Err(err) => {
                            let payload = ResponsePayload {
                                error: Some(format!("Txn Error: {:?}", err)),
                                message: None,
                                json: None,
                                uids_map: None,
                            };

                            send_message(
                                sender_arc_mutex.clone(),
                                Message::Text(serde_json::to_string(&payload).unwrap_or_default()),
                            )
                            .await
                        }
                    }
                }
                Message::Binary(b) => {
                    // TODO: process binary data as needed
                    send_message(sender_arc_mutex.clone(), Message::Binary(b)).await
                }
            },
        };
    }
}

async fn send_message(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    msg: Message,
) -> Result<(), Error> {
    let mut sender_guard = sender_arc_mutex.lock().await;
    let sender = sender_guard.as_mut();
    match sender {
        Some(s) => s.send(msg).await,
        None => Ok(()),
    }
}

async fn auto_close_connection(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    counter: Arc<AtomicU32>,
    prev_count: Arc<AtomicU32>,
) -> () {
    let mut interval = tokio::time::interval(Duration::from_millis(10000));

    let same_count = Arc::new(AtomicU32::new(0));

    while let Some(_) = interval.next().await {
        let count = counter.load(Ordering::Relaxed);
        let prev = prev_count.load(Ordering::Relaxed);
        if count != prev {
            debug!("txn is alive. Count - {}, Prev - {}", count, prev);
            prev_count.store(count, Ordering::Relaxed);
        } else {
            let sc = same_count.fetch_add(1, Ordering::Relaxed);
            debug!("txn is not alive. Same Count - {}", sc);
            if sc >= 6 {
                info!("closing connection");
                let result = send_message(sender_arc_mutex.clone(), Message::Close(None)).await;
                match result {
                    Ok(_) => (),
                    Err(e) => error!("Error sending close message {:?}", e),
                };
                break;
            }
        }
    }
}

async fn kill_task(shutdown_hook_arc_mutex: Arc<Mutex<Option<oneshot::Sender<()>>>>) {
    debug!("killing auto disconnect task");

    let mut shutdown_hook_guard = shutdown_hook_arc_mutex.lock().await;

    let shutdown_hook = shutdown_hook_guard.take();

    let _ = match shutdown_hook {
        Some(s) => s.send(()),
        None => Ok(()),
    };

    ()
}

fn increment_counter(counter: Arc<AtomicU32>) {
    counter.fetch_add(1, Ordering::Relaxed);
}
