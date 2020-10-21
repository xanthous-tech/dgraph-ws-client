use std::env;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dgraph_tonic::{ClientError, DgraphError, Mutate, Query};
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use hyper::upgrade::Upgraded;
use log::{debug, error};
use serde_json::json;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::WebSocketStream;
use tungstenite::{Error, Message};

use crate::dgraph::TxnContextExport;
use crate::models::{RequestPayload, ResponsePayload};
use crate::txn::{discard_txn, process_mutate_txn_request, process_query_txn_request};

pub async fn accept_query_txn_connection<Q>(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    mut receiver: SplitStream<WebSocketStream<Upgraded>>,
    txn_arc_mutex: Arc<Mutex<Option<Q>>>,
    shutdown_hook_arc_mutex: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    query_count: Arc<AtomicU32>,
) where
    Q: Query + 'static,
{
    while let Some(message) = receiver.next().await {
        let sam = sender_arc_mutex.clone();
        let tam = txn_arc_mutex.clone();
        let sham = shutdown_hook_arc_mutex.clone();
        let qc = query_count.clone();

        tokio::spawn(async move { process_query_message(sam, tam, sham, qc, message).await });
    }
}

pub async fn accept_mutate_txn_connection<M>(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    mut receiver: SplitStream<WebSocketStream<Upgraded>>,
    txn_arc_mutex: Arc<Mutex<Option<M>>>,
    shutdown_hook_arc_mutex: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    query_count: Arc<AtomicU32>,
    txn_id: String,
) where
    M: Mutate + TxnContextExport + 'static,
{
    while let Some(message) = receiver.next().await {
        let id = txn_id.clone();
        let sam = sender_arc_mutex.clone();
        let tam = txn_arc_mutex.clone();
        let sham = shutdown_hook_arc_mutex.clone();
        let qc = query_count.clone();

        tokio::spawn(async move {
            process_mutate_message(sam, tam, sham, qc, message, id.clone()).await
        });
    }
}

async fn process_query_message<Q>(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    txn_arc_mutex: Arc<Mutex<Option<Q>>>,
    shutdown_hook_arc_mutex: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    query_count: Arc<AtomicU32>,
    message: Result<Message, Error>,
) where
    Q: Query,
{
    // TODO: better error message by capturing the receive error
    let _result = match message {
        Err(receive_error) => {
            error!("{:?}", receive_error);
            let payload = ResponsePayload {
                id: None,
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
                debug!("received ping {:?}", ping);
                Ok(())
            }
            Message::Pong(_) => {
                debug!("received pong");
                Ok(())
            }
            Message::Close(c) => {
                kill_task(shutdown_hook_arc_mutex.clone()).await;
                send_message(sender_arc_mutex.clone(), Message::Close(c)).await
            }
            Message::Text(t) => {
                let parsed: Result<RequestPayload, _> = serde_json::from_str(t.as_str());
                match parsed {
                    Err(e) => {
                        let payload = ResponsePayload {
                            id: None,
                            error: Some(format!("Parse Error: {:?}", e)),
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
                    Ok(request) => {
                        increment_counter(query_count.clone());
                        let response =
                            process_query_txn_request(txn_arc_mutex.clone(), request.clone()).await;
                        decrement_counter(query_count.clone());
                        match response {
                            Ok(payload) => {
                                send_message(
                                    sender_arc_mutex.clone(),
                                    Message::Text(
                                        serde_json::to_string(&payload).unwrap_or_default(),
                                    ),
                                )
                                .await
                            }
                            Err(err) => {
                                let mut error_msg = Some(
                                    json!({
                                        "message": format!("Txn Error: {:?}", &err),
                                    })
                                    .to_string(),
                                );
                                if err.is::<DgraphError>() {
                                    let dgraph_err: DgraphError =
                                        err.downcast::<DgraphError>().unwrap();
                                    match dgraph_err {
                                        DgraphError::GrpcError(failure) => {
                                            if failure.is::<ClientError>() {
                                                let client_err: ClientError =
                                                    failure.downcast::<ClientError>().unwrap();
                                                match client_err {
                                                    ClientError::CannotAlter(status)
                                                    | ClientError::CannotCheckVersion(status)
                                                    | ClientError::CannotCommitOrAbort(status)
                                                    | ClientError::CannotDoRequest(status)
                                                    | ClientError::CannotLogin(status)
                                                    | ClientError::CannotMutate(status)
                                                    | ClientError::CannotQuery(status)
                                                    | ClientError::CannotRefreshLogin(status) => {
                                                        error_msg.replace(json!({
                                                            "status": status.code().description(),
                                                            "code": status.code() as i32,
                                                            "message": format!("{:}", status.message()),
                                                        }).to_string());
                                                    }
                                                    _ => {}
                                                };
                                            }
                                        }
                                        _ => {}
                                    };
                                }
                                let payload = ResponsePayload {
                                    id: request.id,
                                    error: error_msg,
                                    message: None,
                                    json: None,
                                    uids_map: None,
                                };

                                send_message(
                                    sender_arc_mutex.clone(),
                                    Message::Text(
                                        serde_json::to_string(&payload).unwrap_or_default(),
                                    ),
                                )
                                .await
                            }
                        }
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

async fn process_mutate_message<M>(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    txn_arc_mutex: Arc<Mutex<Option<M>>>,
    shutdown_hook_arc_mutex: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    query_count: Arc<AtomicU32>,
    message: Result<Message, Error>,
    txn_id: String,
) where
    M: Mutate + TxnContextExport,
{
    // TODO: better error message by capturing the receive error
    let _result = match message {
        Err(receive_error) => {
            error!("{:?}", receive_error);
            debug!("discarding txn on error");
            let response = discard_txn(None, txn_arc_mutex.clone()).await;
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
                        id: None,
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
                debug!("received ping {:?}", ping);
                Ok(())
            }
            Message::Pong(_) => {
                debug!("received pong");
                Ok(())
            }
            Message::Close(c) => {
                debug!("discarding txn on close");
                let response = discard_txn(None, txn_arc_mutex.clone()).await;
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
                            id: None,
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
                send_message(sender_arc_mutex.clone(), Message::Close(c)).await
            }
            Message::Text(t) => {
                let parsed: Result<RequestPayload, _> = serde_json::from_str(t.as_str());
                match parsed {
                    Err(e) => {
                        let payload = ResponsePayload {
                            id: None,
                            error: Some(format!("Parse Error: {:?}", e)),
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
                    Ok(request) => {
                        increment_counter(query_count.clone());
                        let response =
                            process_mutate_txn_request(txn_arc_mutex.clone(), request.clone())
                                .await;
                        decrement_counter(query_count.clone());
                        match response {
                            Ok(payload) => {
                                send_message(
                                    sender_arc_mutex.clone(),
                                    Message::Text(
                                        serde_json::to_string(&payload).unwrap_or_default(),
                                    ),
                                )
                                .await
                            }
                            Err(err) => {
                                let mut error_msg = Some(
                                    json!({
                                        "message": format!("Txn Error: {:?}", &err),
                                    })
                                    .to_string(),
                                );
                                if err.is::<DgraphError>() {
                                    let dgraph_err: DgraphError =
                                        err.downcast::<DgraphError>().unwrap();
                                    match dgraph_err {
                                        DgraphError::GrpcError(failure) => {
                                            if failure.is::<ClientError>() {
                                                let client_err: ClientError =
                                                    failure.downcast::<ClientError>().unwrap();
                                                match client_err {
                                                    ClientError::CannotAlter(status)
                                                    | ClientError::CannotCheckVersion(status)
                                                    | ClientError::CannotCommitOrAbort(status)
                                                    | ClientError::CannotDoRequest(status)
                                                    | ClientError::CannotLogin(status)
                                                    | ClientError::CannotMutate(status)
                                                    | ClientError::CannotQuery(status)
                                                    | ClientError::CannotRefreshLogin(status) => {
                                                        error_msg.replace(json!({
                                                            "status": status.code().description(),
                                                            "code": status.code() as i32,
                                                            "message": format!("{:}", status.message()),
                                                        }).to_string());
                                                    }
                                                    _ => {}
                                                };
                                            }
                                        }
                                        _ => {}
                                    };
                                }
                                let payload = ResponsePayload {
                                    id: request.id,
                                    error: error_msg,
                                    message: None,
                                    json: None,
                                    uids_map: None,
                                };

                                send_message(
                                    sender_arc_mutex.clone(),
                                    Message::Text(
                                        serde_json::to_string(&payload).unwrap_or_default(),
                                    ),
                                )
                                .await
                            }
                        }
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

pub async fn auto_close_connection(
    sender_arc_mutex: Arc<Mutex<Option<SplitSink<WebSocketStream<Upgraded>, Message>>>>,
    query_count: Arc<AtomicU32>,
) -> () {
    let timeout = match env::var("CONNECTION_CHECK_INTERVAL") {
        Ok(val) => val.parse::<u64>().unwrap_or(5000),
        Err(_) => 5000,
    };
    let retry_count = match env::var("CONNECTION_CHECK_RETRY") {
        Ok(val) => val.parse::<u32>().unwrap_or(3),
        Err(_) => 3,
    };
    let mut interval = tokio::time::interval(Duration::from_millis(timeout));

    let retry = Arc::new(AtomicU32::new(0));

    while let Some(_) = interval.next().await {
        let count = query_count.load(Ordering::Relaxed);
        if count > 0 {
            debug!("txn is alive. query count - {}", count);
            retry.store(0, Ordering::Relaxed);
        } else {
            let r = retry.fetch_add(1, Ordering::Relaxed);
            debug!("txn is not alive. retry - {}", r);
            if r >= retry_count {
                debug!("closing connection");
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
    debug!("incrementing counter");
    counter.fetch_add(1, Ordering::Relaxed);
}

fn decrement_counter(counter: Arc<AtomicU32>) {
    debug!("decrementing counter");
    counter.fetch_sub(1, Ordering::Relaxed);
}
