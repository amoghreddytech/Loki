use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    #[serde(rename = "src")]
    pub source_node: String,
    #[serde(rename = "dest")]
    pub destination_node: String,
    #[serde(rename = "body")]
    pub payload: T,
}

impl<T> Envelope<T> {
    pub fn new(source_node: String, destination_node: String, payload: T) -> Self {
        Self {
            source_node,
            destination_node,
            payload,
        }
    }
}

#[async_trait]
pub trait HandleMessage<T> {
    async fn handle_message(&mut self, message: T) -> Result<Option<Value>>;
}
