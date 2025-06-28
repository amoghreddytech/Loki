use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;

use crate::message::{Envelope, HandleMessage};

use super::payload::{EchoOk, IncomingPayload, InitOk, Metadata, OutgoingPayload};

#[derive(Debug, Clone)]
pub struct EchoNode {
    pub msg_id: usize,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

impl EchoNode {
    pub fn new() -> Self {
        Self {
            msg_id: 1,
            node_id: String::new(),
            node_ids: vec![],
        }
    }

    fn get_msg_id(&mut self) -> usize {
        let id = self.msg_id;
        self.msg_id += 1;
        id
    }
}

#[async_trait]
impl HandleMessage<Envelope<IncomingPayload>> for EchoNode {
    async fn handle_message(
        &mut self,
        message: Envelope<IncomingPayload>,
    ) -> Result<Option<Value>> {
        let msg_src = message.source_node;
        let msg_dest = message.destination_node;
        let msg_id = self.get_msg_id();

        let response = match message.payload {
            IncomingPayload::Init(init) => {
                self.node_id = init.node_id;
                self.node_ids = init.node_ids;

                let metadata = Metadata::new(Some(msg_id), init.metadata.msg_id);
                let payload = OutgoingPayload::InitOk(InitOk::new(metadata)); // the destination of the srouce node is the new source node.
                // We reply with and init ok message to the node that sent it to us.
                Envelope::new(msg_dest, msg_src, payload)
            }
            IncomingPayload::Echo(echo) => {
                let metadata = Metadata::new(Some(msg_id), echo.metadata.msg_id);
                let echo_message = echo.echo;
                let payload = OutgoingPayload::EchoOk(EchoOk::new(echo_message, metadata));
                Envelope::new(msg_dest, msg_src, payload)
            }
        };

        let json_reponse = serde_json::to_value(response)?;
        Ok(Some(json_reponse))
    }
}
