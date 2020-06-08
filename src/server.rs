use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use dgraph_tonic::{Client};
use hyper::header::{HeaderValue, CONNECTION, UPGRADE};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use log::{debug, error, info};
use ring::digest::{digest, SHA1_FOR_LEGACY_USE_ONLY};

use crate::channels::{
    create_best_effort_txn_channel, create_mutated_txn_channel, create_read_only_txn_channel,
};
use crate::models::AlterPayload;

type GenericError = Box<dyn std::error::Error + Send + Sync>;

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

pub async fn build(addr: SocketAddr, dgraph_client: Arc<Client>) {
    let make_svc = make_service_fn(|_| {
        let dgraph_client = dgraph_client.clone();
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                ws_client(req, dgraph_client.clone())
            }))
        }
    });

    let server = Server::bind(&addr)
        .serve(make_svc)
        .with_graceful_shutdown(shutdown_signal());

    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}

async fn ws_client(req: Request<Body>, dgraph_client: Arc<Client>) -> Result<Response<Body>, GenericError> {
    let dgraph_client = dgraph_client.clone();
    let mut res = Response::new(Body::empty());

    match (req.method(), req.uri().path()) {
        (&Method::GET, "/") => {
            *res.body_mut() = Body::from("OK")
        }

        (&Method::POST, "/alter") => {
            return do_alter(req, dgraph_client).await
        }

        (&Method::GET, "/txn") => {
            if !req.headers().contains_key(UPGRADE)
                || !req.headers().contains_key(CONNECTION)
                || !req.headers().contains_key("Sec-WebSocket-Key")
                || !(req.headers()[UPGRADE].as_bytes() == "websocket".as_bytes())
            {
                *res.status_mut() = StatusCode::BAD_REQUEST;
                *res.body_mut() = Body::from("Invalid WebSocket Upgrade Request");
                return Ok(res);
            }

            // websocket handshake
            let sec_websocket_key = String::from_utf8(
                req.headers()["Sec-WebSocket-Key"].as_bytes().to_vec(),
            )
                .unwrap_or_default();
            let websocket_key = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".to_string();
            let combined = format!("{}{}", sec_websocket_key, websocket_key);
            let sec_websocket_accept = base64::encode(
                digest(&SHA1_FOR_LEGACY_USE_ONLY, combined.as_bytes()).as_ref(),
            );

            *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            res.headers_mut()
                .insert(UPGRADE, HeaderValue::from_static("websocket"));
            res.headers_mut()
                .insert(CONNECTION, req.headers()[CONNECTION].clone());
            res.headers_mut().insert(
                "Sec-WebSocket-Accept",
                HeaderValue::from_bytes(sec_websocket_accept.as_bytes())
                    .unwrap_or_else(|_| HeaderValue::from_static("")),
            );

            // websocket upgrade
            tokio::task::spawn(do_upgrade_websocket(req, dgraph_client));
        }
        _ => {
            *res.status_mut() = StatusCode::NOT_FOUND;
            *res.body_mut() = Body::from("Not Found");
        }
    };

    Ok(res)
}

async fn do_upgrade_websocket(req: Request<Body>, dgraph_client: Arc<Client>) {
    let query = req.uri().query().unwrap_or_default().to_string();
    let query_params = querystring::querify(query.as_str());
    let query_map: HashMap<&str, &str> = query_params.iter().cloned().collect();

    match req.into_body().on_upgrade().await {
        Err(e) => error!("upgrade error: {}", e),
        Ok(upgraded) => {
            debug!("WebSocket Upgraded {:?}", query_map.clone());
            let read_only = query_map.get("read_only").unwrap_or(&"");
            let best_effort = query_map.get("best_effort").unwrap_or(&"");
            if *read_only == "true" {
                if *best_effort == "true" {
                    create_best_effort_txn_channel(
                        upgraded,
                        dgraph_client.clone(),
                    ).await;
                } else {
                    create_read_only_txn_channel(
                        upgraded,
                        dgraph_client.clone(),
                    ).await;
                }
            } else {
                create_mutated_txn_channel(
                    upgraded,
                    dgraph_client.clone(),
                ).await;
            }
        }
    }
}

async fn do_alter(req: Request<Body>, dgraph_client: Arc<Client>) -> Result<Response<Body>, GenericError> {
    let req_body = hyper::body::to_bytes(req).await?;
    let alter_body: AlterPayload = serde_json::from_slice(req_body.to_vec().as_slice())?;

    let _result = dgraph_client.alter(alter_body.into_operation()).await?;

    let response = Response::new(Body::empty());
    Ok(response)
}
