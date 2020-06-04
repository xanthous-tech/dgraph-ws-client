#[macro_use]
extern crate anyhow;

extern crate serde_json;

mod server;
mod models;
mod channels;
mod connections;
mod txn;

use std::env;
use std::sync::Arc;
use std::net::SocketAddr;

use log::info;
use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let _ = env_logger::try_init();

  let addresses = match env::var("DGRAPH_ALPHAS") {
    Ok(val) => val.clone(),
    Err(_) => "http://localhost:9080".to_string(),
  };

  let address_vec = addresses.split(",").collect::<Vec<&str>>();

  info!("creating client against dGraph servers {:?}", address_vec);

  let client_arc = Arc::new(Client::new(address_vec).expect("dgraph client"));

  let addr_str = match env::var("LISTEN_ADDRESS") {
    Ok(val) => val.clone(),
    Err(_) => "0.0.0.0:9000".to_string(),
  };
  let addr: SocketAddr = addr_str.parse().unwrap_or(SocketAddr::from(([0, 0, 0, 0], 9000)));

  info!("server listening at {:}", addr);

  server::build(addr, client_arc.clone()).await
}
