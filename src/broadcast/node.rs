use anyhow::Result;
use async_trait::async_trait;

use super::payload::{ReadOk, TopologyOk};
use crate::message::{Envelope, HandleMessage};
use std::collections::HashSet;

use super::payload::{IncomingPayload, InitOk, Metadata, OutgoingPayload};
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Debug, Clone)]
pub struct BroadCastNode {
    msg_id: usize,
    pub neighbours: Vec<String>,
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub seen: HashSet<usize>,
    // values here are just things that can
    // be converted to json representation.
    pub tranmitter: UnboundedSender<Value>,
}

impl BroadCastNode {
    pub fn new(tranmitter: UnboundedSender<Value>) -> Self {
        Self {
            node_id: String::new(),
            node_ids: vec![],
            msg_id: 1,
            neighbours: Vec::new(),
            seen: HashSet::new(),
            tranmitter,
        }
    }

    pub fn get_msg_id(&mut self) -> usize {
        let id = self.msg_id;
        self.msg_id += 1;
        id
    }
}

#[async_trait]
impl HandleMessage<Envelope<IncomingPayload>> for BroadCastNode {
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
            IncomingPayload::Topology(mut topology) => {
                // If we want to implemet our own topologies and stuff
                // I'll have to do something with all the node_ids
                // let all_node_ids = self.node_ids.clone();

                // We inject the neighbours
                if let Some(neighbours) = topology.topology.remove(&self.node_id) {
                    self.neighbours = neighbours;
                    eprintln!(
                        "the neighbours for node {} = {:?}",
                        self.node_id, self.neighbours
                    );
                }

                let metadata = Metadata::new(Some(msg_id), topology.metadata.msg_id);

                let payload = OutgoingPayload::TopologyOk(TopologyOk::new(metadata));

                Envelope::new(msg_dest, msg_src, payload)
            }

            IncomingPayload::ReadPayload(data) => {
                let metadata = Metadata::new(Some(msg_id), data.metadata.msg_id);
                let messages: Vec<usize> = self.seen.clone().into_iter().collect::<Vec<usize>>();
                let payload = OutgoingPayload::ReadPayloadOk(ReadOk::new(metadata, messages));
                Envelope::new(msg_dest, msg_src, payload)
            }
        };

        let json_reponse = serde_json::to_value(response)?;
        Ok(Some(json_reponse))
    }
}
