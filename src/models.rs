use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryPayload {
  pub q: String,
  pub vars: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MutationPayload {
  #[serde(rename = "setJson")]
  pub set_json: Option<Vec<u8>>,
  #[serde(rename = "deleteJson")]
  pub delete_json: Option<Vec<u8>>,
  #[serde(rename = "setNquads")]
  pub set_nquads: Option<Vec<u8>>,
  #[serde(rename = "delNquads")]
  pub del_nquads: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestPayload {
  pub query: Option<QueryPayload>,
  pub mutate: Option<MutationPayload>,
  pub commit: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResponsePayload {
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub message: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub json: Option<Value>,
  #[serde(rename = "uidsMap", skip_serializing_if = "Option::is_none")]
  pub uids_map: Option<HashMap<String, String>>,
}
