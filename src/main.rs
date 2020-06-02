#[macro_use]
extern crate anyhow;

extern crate serde_json;

mod models;
mod channels;
mod connections;
mod txn;

use std::env;
use std::sync::Arc;

use log::info;
use dgraph_tonic::Client;

use channels::{
  create_read_only_txn_channel,
  create_best_effort_txn_channel,
  create_mutated_txn_channel,
};

#[tokio::main]
async fn main() {
  let _ = env_logger::try_init();

  let addresses = match env::var("DGRAPH_ALPHAS") {
    Ok(val) => val.clone(),
    Err(_) => "http://localhost:9080".to_string(),
  };

  let address_vec = addresses.split(",").collect::<Vec<&str>>();

  info!("creating client against dGraph servers {:?}", address_vec);

  let client = Arc::new(Client::new(address_vec).expect("dgraph client"));

  let handle1 = tokio::spawn(create_read_only_txn_channel("127.0.0.1:9001", client.clone()));
  let handle2 = tokio::spawn(create_best_effort_txn_channel("127.0.0.1:9002", client.clone()));
  let handle3 = tokio::spawn(create_mutated_txn_channel("127.0.0.1:9003", client.clone()));

  let _responses = tokio::try_join!(handle1, handle2, handle3);
}
