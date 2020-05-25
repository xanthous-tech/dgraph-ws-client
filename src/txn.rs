use std::collections::HashMap;
use std::sync::Arc;
use std::str::from_utf8;

use log::{debug, error};
use anyhow::Result;
use tokio::sync::Mutex;

use dgraph_tonic::{Query, Mutate, Mutation};

use crate::models::{RequestPayload, ResponsePayload, QueryPayload, MutationPayload};

pub async fn discard_txn<M>(txn_arc_mutex: Arc<Mutex<Option<M>>>) -> Result<ResponsePayload> where M: Mutate {
  let mut txn_guard = txn_arc_mutex.lock().await;
  let txn = txn_guard.take();

  match txn {
    Some(t) => {
      let result = t.discard().await;
      match result {
        Ok(_) => {
          debug!("txn discarded");
          Ok(ResponsePayload {
            error: None,
            message: Some("txn discarded".to_string()),
            json: None,
            uids_map: None,
          })
        },
        Err(e) => {
          error!("DgraphError {:?}", e);
          Err(anyhow!("DgraphError {:?}", e))
        }
      }
    },
    None => {
      debug!("txn is empty");
      Ok(ResponsePayload {
        error: None,
        message: Some("txn is empty".to_string()),
        json: None,
        uids_map: None,
      })
    },
  }
}

pub async fn process_query_txn_request<Q>(txn_arc_mutex: Arc<Mutex<Option<Q>>>, request_string: String) -> Result<ResponsePayload> where Q: Query {
  let parsed: Result<RequestPayload, _> = serde_json::from_str(request_string.as_str());

  match parsed {
    Err(e) => {
      error!("parse request error {:?}", e);
      Err(anyhow!("parse request error {:?}", e))
    },
    Ok(request) => {
      debug!("incoming request {:?}", request);
      match request.mutate {
        Some(_) => {
          error!("invalid request {:?}", request);
          Err(anyhow!("invalid request {:?}", request))
        },
        None => match request.commit {
          Some(_) => {
            error!("invalid request {:?}", request);
            Err(anyhow!("invalid request {:?}", request))
          },
          None => match request.query {
            None => {
              error!("invalid request {:?}", request);
              Err(anyhow!("invalid request {:?}", request))
            },
            Some(query) => {
              query_with_vars(txn_arc_mutex.clone(), query.clone()).await
            },
          },
        },
      }
    },
  }
}

pub async fn process_mutate_txn_request<M>(txn_arc_mutex: Arc<Mutex<Option<M>>>, request_string: String) -> Result<ResponsePayload> where M: Mutate {
  let parsed: Result<RequestPayload, _> = serde_json::from_str(request_string.as_str());

  match parsed {
    Err(e) => {
      error!("parse request error {:?}", e);
      Err(anyhow!("parse request error {:?}", e))
    },
    Ok(request) => {
      debug!("incoming request {:?}", request);
      match request.query {
        Some(query) => {
          match request.mutate {
            Some(mu) => {
              let mutation = generate_mutation(mu, request.commit);
              upsert(txn_arc_mutex.clone(), query.clone(), mutation).await
            },
            None => query_with_vars(txn_arc_mutex.clone(), query.clone()).await,
          }
        },
        None => {
          match request.mutate {
            Some(mu) => {
              let mutation = generate_mutation(mu, request.commit);
              mutate(txn_arc_mutex.clone(), mutation).await
            },
            None => {
              match request.commit {
                Some(c) => if c {
                  commit_txn(txn_arc_mutex).await
                } else {
                  error!("invalid request {:?}", request);
                  Err(anyhow!("invalid request {:?}", request))
                },
                None => {
                  error!("invalid request {:?}", request);
                  Err(anyhow!("invalid request {:?}", request))
                },
              }
            },
          }
        },
      }
    },
  }
}

async fn commit_txn<M>(txn_arc_mutex: Arc<Mutex<Option<M>>>) -> Result<ResponsePayload> where M: Mutate {
  let mut txn_guard = txn_arc_mutex.lock().await;
  let txn = txn_guard.take();

  match txn {
    Some(t) => {
      let result = t.commit().await;
      match result {
        Ok(_) => {
          debug!("txn committed");
          Ok(ResponsePayload {
            error: None,
            message: Some("txn committed".to_string()),
            json: None,
            uids_map: None,
          })
        },
        Err(e) => {
          error!("DgraphError {:?}", e);
          Err(anyhow!("DgraphError {:?}", e))
        }
      }
    },
    None => {
      debug!("txn is empty");
      Ok(ResponsePayload {
        error: None,
        message: Some("txn is empty".to_string()),
        json: None,
        uids_map: None,
      })
    },
  }
}

async fn query_with_vars<Q>(txn_arc_mutex: Arc<Mutex<Option<Q>>>, query: QueryPayload) -> Result<ResponsePayload> where Q: Query {
  let mut txn_guard = txn_arc_mutex.lock().await;
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
          Ok(ResponsePayload {
            error: None,
            message: None,
            json: Some(serde_json::from_str(from_utf8(r.json.as_slice()).unwrap_or_default()).unwrap_or_default()),
            uids_map: Some(r.uids),
          })
        },
        Err(e) => {
          error!("DgraphError {:?}", e);
          Err(anyhow!("DgraphError {:?}", e))
        }
      }
    },
    None => {
      // empty txn should be error on normal operations
      error!("txn is empty");
      Err(anyhow!("txn is empty"))
    }
  }
}

async fn upsert<M>(txn_arc_mutex: Arc<Mutex<Option<M>>>, query: QueryPayload, mutation: Mutation) -> Result<ResponsePayload> where M: Mutate {
  let mut txn_guard = txn_arc_mutex.lock().await;

  let vars = match query.vars {
    Some(v) => v,
    None => HashMap::new(),
  };

  if mutation.commit_now {
    let txn = txn_guard.take();

    match txn {
      Some(t) => {
        let response = t.upsert_with_vars_and_commit_now(query.q, vars, mutation).await;
        match response {
          Ok(r) => {
            Ok(ResponsePayload {
              error: None,
              message: None,
              json: Some(serde_json::from_str(from_utf8(r.json.as_slice()).unwrap_or_default()).unwrap_or_default()),
              uids_map: Some(r.uids),
            })
          },
          Err(e) => {
            error!("DgraphError {:?}", e);
            Err(anyhow!("DgraphError {:?}", e))
          }
        }
      },
      None => {
        // empty txn should be error on normal operations
        error!("txn is empty");
        Err(anyhow!("txn is empty"))
      }
    }
  } else {
    let txn = txn_guard.as_mut();

    match txn {
      Some(t) => {
        let response = t.upsert_with_vars(query.q, vars, mutation).await;
        match response {
          Ok(r) => {
            Ok(ResponsePayload {
              error: None,
              message: None,
              json: Some(serde_json::from_str(from_utf8(r.json.as_slice()).unwrap_or_default()).unwrap_or_default()),
              uids_map: Some(r.uids),
            })
          },
          Err(e) => {
            error!("DgraphError {:?}", e);
            Err(anyhow!("DgraphError {:?}", e))
          }
        }
      },
      None => {
        // empty txn should be error on normal operations
        error!("txn is empty");
        Err(anyhow!("txn is empty"))
      }
    }
  }
}

async fn mutate<M>(txn_arc_mutex: Arc<Mutex<Option<M>>>, mutation: Mutation) -> Result<ResponsePayload> where M: Mutate {
  let mut txn_guard = txn_arc_mutex.lock().await;

  if mutation.commit_now {
    let txn = txn_guard.take();

    match txn {
      Some(t) => {
        let response = t.mutate_and_commit_now(mutation).await;
        match response {
          Ok(r) => {
            Ok(ResponsePayload {
              error: None,
              message: None,
              json: Some(serde_json::from_str(from_utf8(r.json.as_slice()).unwrap_or_default()).unwrap_or_default()),
              uids_map: Some(r.uids),
            })
          },
          Err(e) => {
            error!("DgraphError {:?}", e);
            Err(anyhow!("DgraphError {:?}", e))
          }
        }
      },
      None => {
        // empty txn should be error on normal operations
        error!("txn is empty");
        Err(anyhow!("txn is empty"))
      }
    }
  } else {
    let txn = txn_guard.as_mut();

    match txn {
      Some(t) => {
        let response = t.mutate(mutation).await;
        match response {
          Ok(r) => {
            Ok(ResponsePayload {
              error: None,
              message: None,
              json: Some(serde_json::from_str(from_utf8(r.json.as_slice()).unwrap_or_default()).unwrap_or_default()),
              uids_map: Some(r.uids),
            })
          },
          Err(e) => {
            error!("DgraphError {:?}", e);
            Err(anyhow!("DgraphError {:?}", e))
          }
        }
      },
      None => {
        // empty txn should be error on normal operations
        error!("txn is empty");
        Err(anyhow!("txn is empty"))
      }
    }
  }
}

fn generate_mutation(mutate: MutationPayload, commit_now: Option<bool>) -> Mutation {
  let mut mutation = Mutation::new();

  mutation.commit_now = match commit_now {
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

  mutation
}
