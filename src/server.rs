use std::sync::Arc;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::collections::HashMap;

use log::{info, error};
use futures::future::TryFutureExt;
use hyper::{Body, Request, Response, Server, Method, StatusCode, HeaderMap};
use hyper::service::{make_service_fn, service_fn};
use hyper::server::conn::AddrStream;
use hyper::header::{UPGRADE, HeaderValue, CONNECTION};
use dgraph_tonic::Client;
use ring::digest::{digest, SHA1_FOR_LEGACY_USE_ONLY};

use crate::channels::{
  create_read_only_txn_channel,
  create_best_effort_txn_channel,
  create_mutated_txn_channel,
};

async fn shutdown_signal() {
  // Wait for the CTRL+C signal
  tokio::signal::ctrl_c()
    .await
    .and_then(|_| {
      info!("captured signal, stopping server");
      Ok(())
    })
    .expect("failed to install CTRL+C signal handler");
}

pub async fn build(addr: SocketAddr, client_arc: Arc<Client>) {
  let client_one = client_arc.clone();
  let make_svc = make_service_fn(|_socket: &AddrStream| {
    let client_two = client_one.clone();
    async move {
      let client_three = client_two.clone();
      Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
        let client_four = client_three.clone();
        async move {
          let mut res = Response::new(Body::empty());

          match (req.method(), req.uri().path()) {
            (&Method::GET, "/") => {
              *res.body_mut() = Body::from("OK");
            },
            (&Method::GET, "/txn") => {
              if !req.headers().contains_key(UPGRADE)
                || !req.headers().contains_key(CONNECTION)
                || !req.headers().contains_key("Sec-WebSocket-Key")
                || !(req.headers()[UPGRADE].as_bytes() == "websocket".as_bytes()) {
                *res.status_mut() = StatusCode::BAD_REQUEST;
                *res.body_mut() = Body::from("Invalid WebSocket Upgrade Request");
                return Ok(res);
              }

              let sec_websocket_key = String::from_utf8(req.headers()["Sec-WebSocket-Key"].as_bytes().to_vec()).unwrap_or_default();
              let websocket_key = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".to_string();
              let combined = format!("{}{}", sec_websocket_key, websocket_key);
              let sec_websocket_accept = base64::encode(digest(&SHA1_FOR_LEGACY_USE_ONLY, combined.as_bytes()).as_ref());

              *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
              res.headers_mut()
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
              res.headers_mut()
                .insert(CONNECTION, req.headers()[CONNECTION].clone());
              res.headers_mut()
                .insert("Sec-WebSocket-Accept", HeaderValue::from_bytes(sec_websocket_accept.as_bytes()).unwrap_or_else(|_| HeaderValue::from_static("")));

              tokio::task::spawn(async move {
                let query = req.uri().query().unwrap_or_default().to_string();
                let query_params = querystring::querify(query.as_str());
                let query_map: HashMap<&str, &str> = query_params.iter().cloned().collect();
                match req.into_body().on_upgrade().await {
                  Ok(upgraded) => {
                    info!("WebSocket Upgraded {:?}", query_map.clone());
                    let read_only = query_map.get("read_only").unwrap_or(&"");
                    let best_effort = query_map.get("best_effort").unwrap_or(&"");
                    if *read_only == "true" {
                      if *best_effort == "true" {
                        create_best_effort_txn_channel(upgraded, client_four.clone()).await;
                      } else {
                        create_read_only_txn_channel(upgraded, client_four.clone()).await;
                      }
                    } else {
                      info!("creating mutated txn");
                      create_mutated_txn_channel(upgraded, client_four.clone()).await;
                    }
                  }
                  Err(e) => error!("upgrade error: {}", e),
                }
              });
            },
            _ => {
              *res.status_mut() = StatusCode::NOT_FOUND;
              *res.body_mut() = Body::from("Not Found");
            },
          };

          Ok::<_, hyper::Error>(res)
        }
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