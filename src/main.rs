#[macro_use]
extern crate anyhow;

extern crate serde_json;

mod models;
mod channels;
mod connections;
mod txn;

use channels::{
  create_read_only_txn_channel,
  create_best_effort_txn_channel,
  create_mutated_txn_channel,
};

use std::sync::Arc;

use dgraph_tonic::Client;

#[tokio::main]
async fn main() {
  let _ = env_logger::try_init();

  let client = Arc::new(Client::new("http://localhost:9080").expect("dgraph client"));

  let handle1 = tokio::spawn(create_read_only_txn_channel("127.0.0.1:9001", client.clone()));
  let handle2 = tokio::spawn(create_best_effort_txn_channel("127.0.0.1:9002", client.clone()));
  let handle3 = tokio::spawn(create_mutated_txn_channel("127.0.0.1:9003", client.clone()));

  let _responses = tokio::try_join!(handle1, handle2, handle3);
}
