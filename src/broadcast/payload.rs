use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    // This is a unique identifier from that nodes point of view.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<usize>,
}

impl Metadata {
    pub fn new(msg_id: Option<usize>, in_reply_to: Option<usize>) -> Self {
        Self {
            msg_id,
            in_reply_to,
        }
    }
}

// Consists of the incoming messages for generator the test
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum IncomingPayload {
    Init(Init),
    Topology(Topology),
    #[serde(rename = "read")]
    ReadPayload(ReadStruct),
    Broadcast(Broadcast),
    BroadcastOk(BroadcastOk),
}

// Consists of the outgoing messages for the echo test
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum OutgoingPayload {
    InitOk(InitOk),
    TopologyOk(TopologyOk),
    #[serde(rename = "read_ok")]
    ReadPayloadOk(ReadOk),
    BroadcastOk(BroadcastOk),
    Broadcast(Broadcast),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,

    #[serde(flatten)]
    pub metadata: Metadata,
}

impl Init {
    pub fn new(node_id: String, node_ids: Vec<String>, metadata: Metadata) -> Self {
        Self {
            node_id,
            node_ids,
            metadata,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitOk {
    #[serde(flatten)]
    pub metadata: Metadata,
}

impl InitOk {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topology {
    pub topology: HashMap<String, Vec<String>>,
    #[serde(flatten)]
    pub metadata: Metadata,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyOk {
    #[serde(flatten)]
    pub metadata: Metadata,
}

impl TopologyOk {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadStruct {
    #[serde(flatten)]
    pub metadata: Metadata,
}

impl ReadStruct {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadOk {
    #[serde(flatten)]
    pub metadata: Metadata,
    pub messages: Vec<usize>,
}

impl ReadOk {
    pub fn new(metadata: Metadata, messages: Vec<usize>) -> Self {
        Self { metadata, messages }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broadcast {
    #[serde(flatten)]
    pub metadata: Metadata,
    pub message: usize,
}

impl Broadcast {
    pub fn new(metadata: Metadata, message: usize) -> Self {
        Self { metadata, message }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BroadcastOk {
    #[serde(flatten)]
    pub metadata: Metadata,
}

impl BroadcastOk {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}
