use serde::{Deserialize, Serialize};

// Consists of the incoming messages for generator the test
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum IncomingPayload {
    Init(Init),
    Generate(Generate),
}

// Consists of the outgoing messages for the echo test
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum OutgoingPayload {
    InitOk(InitOk),
    GenerateOk(GenerateOk),
}

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
pub struct GenerateOk {
    #[serde(flatten)]
    pub metadata: Metadata,
    #[serde(rename = "id")]
    pub generated_id: String,
}

impl GenerateOk {
    pub fn new(metadata: Metadata, generated_id: String) -> Self {
        Self {
            metadata,
            generated_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Generate {
    #[serde(flatten)]
    pub metadata: Metadata,
}

impl Generate {
    pub fn new(metadata: Metadata) -> Self {
        Self { metadata }
    }
}
