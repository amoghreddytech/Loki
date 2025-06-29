use anyhow::Result;
use async_trait::async_trait;

use super::payload::{
    Broadcast, BroadcastOk, IncomingPayload, InitOk, Metadata, OutgoingPayload, ReadOk, TopologyOk,
};
use crate::message::{Envelope, HandleMessage};
use std::collections::HashSet;

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
    pub transmitter: UnboundedSender<Value>,
}

impl BroadCastNode {
    pub fn new(transmitter: UnboundedSender<Value>) -> Self {
        Self {
            node_id: String::new(),
            node_ids: vec![],
            msg_id: 1,
            neighbours: Vec::new(),
            seen: HashSet::new(),
            transmitter,
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

        let response: Option<Envelope<OutgoingPayload>> = match message.payload {
            IncomingPayload::Init(init) => {
                self.node_id = init.node_id;
                self.node_ids = init.node_ids;

                let metadata = Metadata::new(Some(msg_id), init.metadata.msg_id);
                let payload = OutgoingPayload::InitOk(InitOk::new(metadata)); // the destination of the srouce node is the new source node.
                // We reply with and init ok message to the node that sent it to us.
                Some(Envelope::new(msg_dest, msg_src, payload))
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

                Some(Envelope::new(msg_dest, msg_src, payload))
            }

            IncomingPayload::ReadPayload(data) => {
                let metadata = Metadata::new(Some(msg_id), data.metadata.msg_id);
                let messages: Vec<usize> = self.seen.clone().into_iter().collect::<Vec<usize>>();
                let payload = OutgoingPayload::ReadPayloadOk(ReadOk::new(metadata, messages));
                Some(Envelope::new(msg_dest, msg_src, payload))
            }

            IncomingPayload::Broadcast(broadcast) => {
                // first check if it's already present in the hashmap
                let is_new = self.seen.insert(broadcast.message);
                let neighbours_to_send: Vec<String> = self
                    .neighbours
                    .iter()
                    .filter(|&n| n != &msg_src)
                    .cloned()
                    .collect();

                let msg_ids: Vec<usize> = neighbours_to_send
                    .iter()
                    .map(|_| self.get_msg_id())
                    .collect();

                if is_new {
                    for (neighbour, msg_id) in
                        neighbours_to_send.into_iter().zip(msg_ids.into_iter())
                    {
                        let forward_metadata = Metadata::new(Some(msg_id), None);

                        let message = broadcast.message;

                        let forward_payload =
                            OutgoingPayload::Broadcast(Broadcast::new(forward_metadata, message));

                        let forward_message =
                            Envelope::new(self.node_id.clone(), neighbour.clone(), forward_payload);

                        if let Err(e) = self
                            .transmitter
                            .send(serde_json::to_value(forward_message)?)
                        {
                            eprintln!("Failed to foward broadcast {:?}", e)
                        }
                    }
                }

                // I need to reply with a broadcast Ok message
                let metadata = Metadata::new(Some(msg_id), broadcast.metadata.msg_id);
                let payload = OutgoingPayload::BroadcastOk(BroadcastOk::new(metadata));

                Some(Envelope::new(self.node_id.clone(), msg_src, payload))
            }
            IncomingPayload::BroadcastOk(_) => None,
        };

        match response {
            Some(r) => {
                let json_reponse = serde_json::to_value(r)?;
                Ok(Some(json_reponse))
            }
            None => Ok(None),
        }
    }
}
