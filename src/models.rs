use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryPayload {
    pub q: String,
    pub vars: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MutationPayload {
    pub set_json: Option<String>,
    pub delete_json: Option<String>,
    pub set_nquads: Option<String>,
    pub del_nquads: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestPayload {
    pub id: Option<String>,
    pub query: Option<QueryPayload>,
    pub mutate: Option<MutationPayload>,
    pub commit: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResponsePayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json: Option<Value>,
    #[serde(rename = "uidsMap", skip_serializing_if = "Option::is_none")]
    pub uids_map: Option<HashMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AlterPayload {
    pub schema: String,
    pub drop_attr: String,
    pub drop_all: bool,
    pub drop_value: String,
    pub drop_op: alter_payload::DropOp,
}

impl AlterPayload {
    pub fn into_operation(self) -> dgraph_tonic::Operation {
        dgraph_tonic::Operation {
            schema: self.schema,
            drop_all: self.drop_all,
            drop_attr: self.drop_attr,
            drop_value: self.drop_value,
            drop_op: alter_payload::get_operation_drop_op_val(self.drop_op),
        }
    }
}

pub mod alter_payload {
    use serde::{Deserialize, Serialize};

    // Did not model this as an i32 enum so wire version is
    //  readable.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum DropOp {
        None, // 0
        All,  // 1
        Data, // 2
        Attr, // 3
        Type, // 4
    }

    pub fn get_operation_drop_op_val (original: DropOp) -> i32 {
        match original {
            DropOp::None => 0,
            DropOp::All  => 1,
            DropOp::Data => 2,
            DropOp::Attr => 3,
            DropOp::Type => 4,
        }
    }
}
