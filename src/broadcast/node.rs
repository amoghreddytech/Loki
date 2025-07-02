use anyhow::Result;
use async_trait::async_trait;

use super::payload::{
    Broadcast, BroadcastOk, Gossip, GossipOk, IncomingPayload, InitOk, Metadata, OutgoingPayload,
    ReadOk, TopologyOk,
};
use crate::message::{Envelope, HandleMessage};
use std::collections::{HashMap, HashSet};

use serde_json::Value;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use std::sync::Arc;

#[derive(Debug)]
pub struct BroadCastNode {
    msg_id: Arc<Mutex<usize>>,
    pub neighbours: Vec<String>,
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub seen: HashSet<usize>,
    // values here are just things that can
    // be converted to json representation.
    pub transmitter: UnboundedSender<Value>,
    // I need a node_map_of_seen_values
    // say my node_id ix 1 -> this map will be
    // {"n2", [33,54], n3: [42,45], n4 : [65,76]}
    pub gossip_map: Arc<Mutex<HashMap<String, HashSet<usize>>>>,

    // This hashmap needs to hold the msg_id or the broadcast value and the nodes that it needs to send it to
    pub pending_acks: Arc<Mutex<HashMap<usize, HashSet<String>>>>,
}

impl BroadCastNode {
    pub fn new(transmitter: UnboundedSender<Value>) -> Self {
        Self {
            node_id: String::new(),
            node_ids: vec![],
            msg_id: Arc::new(Mutex::new(1)),
            neighbours: Vec::new(),
            seen: HashSet::new(),
            transmitter,
            gossip_map: Arc::new(Mutex::new(HashMap::new())),
            // I need to know foe each broadcast which neighbours have not replied
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn add_messages_to_gossip_map(&self, messages: HashSet<usize>, neighbour: String) {
        let mut map = self.gossip_map.lock().await;
        let known_sent_nodes: &mut HashSet<usize> =
            map.entry(neighbour).or_insert_with(HashSet::new);
        known_sent_nodes.extend(messages);
    }

    async fn get_msg_id(counter: &Arc<Mutex<usize>>) -> usize {
        let mut guard = counter.lock().await;
        let id = *guard;
        *guard = guard.saturating_add(1);
        id
    }

    async fn gossip(&self, msg_src: String) {
        let neighbours = self.neighbours.clone();
        for neighbour in neighbours {
            // This is the case of sending it back to source and
            // The case of if the neigbour is the maybe error here.
            //

            if neighbour == self.node_id {
                panic!("how can my neighbour be myself so then we can figure it out")
            }

            if neighbour == msg_src {
                continue;
            }

            // These are the nodes that I know I've sent this node.
            let known_sent_nodes: HashSet<usize> = {
                let map = self.gossip_map.lock().await;
                map.get(&neighbour).cloned().unwrap_or_default()
            };

            // my messages
            let unsent_messages: HashSet<usize> =
                self.seen.difference(&known_sent_nodes).cloned().collect();

            if unsent_messages.is_empty() {
                continue;
            }

            let counter = self.msg_id.clone();
            // Send the message;
            let new_msg_id: usize = Self::get_msg_id(&counter).await;
            let metadata = Metadata::new(Some(new_msg_id), None);
            // Create gossip messeges to the neighbours
            let payload = OutgoingPayload::Gossip(Gossip::new(unsent_messages, metadata));
            let envelope = Envelope::new(self.node_id.clone(), neighbour.clone(), payload);
            let gossip_json = match serde_json::to_value(envelope) {
                Ok(val) => val,
                Err(_) => panic!("Gossip json failed to serialize"), // Stop retries on serialization failure
            };
            if let Err(e) = self.transmitter.send(gossip_json.clone()) {
                eprintln!(
                    "Failed to send to {} from {} with errro {e}",
                    self.node_id, neighbour
                );
            }
        }
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

        // This is the outer envelope msg id so if we have a broacast this will go in
        // to the broacast okay or whatever.
        let msg_id = Self::get_msg_id(&self.msg_id.clone()).await;

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

            // Let's think about what we do when the system
            // sends one message.
            // First we update what we have seen.
            IncomingPayload::Broadcast(broadcast) => {
                // I inserted a broadcast method
                let new = self.seen.insert(broadcast.message);
                // This is the bit I have to deal and do with
                // actually broadcasting
                // Let's solve for a single node case and no partitions
                if new {}
                // I will send the broadcastOK for this method
                let meta = Metadata::new(Some(msg_id), broadcast.metadata.msg_id);
                let b_ok_payload = OutgoingPayload::BroadcastOk(BroadcastOk::new(meta));
                let b_ok_envelope = Envelope::new(msg_dest, msg_src, b_ok_payload);
                Some(b_ok_envelope)
            }
            IncomingPayload::Gossip(gossip) => {
                // What do you do when you get a gossip message
                self.seen.extend(gossip.message.iter());

                // The message_id is the parent mesg_id
                // And you reply to the node that sent you the gossip
                let metadata = Metadata::new(Some(msg_id), gossip.metadata.msg_id);
                let payload = OutgoingPayload::GossipOk(GossipOk::new(metadata, gossip.message));
                let response = Some(Envelope::new(msg_dest, msg_src, payload));
                // I need to gossip to my nighbours
                for neighbour in &self.neighbours {
                    let known_by_me = {
                        let mut map = self.gossip_map.lock().await;
                        map.entry(neighbour.clone())
                            .or_insert_with(HashSet::new)
                            .clone()
                    };

                    let difference: HashSet<usize> = self
                        .seen
                        .difference(&known_by_me)
                        .cloned()
                        .collect::<HashSet<usize>>();

                    let transmitter = self.transmitter.clone();
                    let sending_node = self.node_id.clone();
                    let destination_node = neighbour.clone();
                    let counter: Arc<Mutex<usize>> = self.msg_id.clone();
                    if !difference.is_empty() {
                        tokio::spawn(async move {
                            let new_msg_id = Self::get_msg_id(&counter).await;

                            let metadata = Metadata::new(Some(new_msg_id), None);
                            let payload =
                                OutgoingPayload::Gossip(Gossip::new(difference, metadata));
                            let envelope =
                                Some(Envelope::new(sending_node, destination_node, payload));

                            let gossip_response = match serde_json::to_value(envelope) {
                                Ok(val) => val,
                                Err(_) => panic!("Gossip json failed to serialize"), // Stop retries on serialization failure
                            };

                            if let Err(_) = transmitter.send(gossip_response) {}
                        });
                    }
                }

                response
            }

            // Crap I need to do when I get a gossip is Ok message
            IncomingPayload::GossipOk(gossip_ok) => None,
            IncomingPayload::BroadcastOk(ok) => {
                if let Some(in_reply_to) = ok.metadata.in_reply_to {
                    let mut pending_awks = self.pending_acks.lock().await;
                    // get me the hashset representing the in_reply_to_values
                    if let Some(awks_on_node) = pending_awks.get_mut(&in_reply_to) {
                        awks_on_node.remove(&msg_src);
                        if awks_on_node.is_empty() {
                            pending_awks.remove(&in_reply_to);
                        }
                    }
                }

                None
            }
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

pub fn gossip_retry(
    from: String,
    to: String,
    payload_message: HashSet<usize>,
    transmitter: UnboundedSender<Value>,
    msg_id: usize,
) {
    tokio::spawn(async move {
        let metadata = Metadata::new(Some(msg_id), None);
        let gossip_payload = OutgoingPayload::Gossip(Gossip::new(payload_message, metadata));
        let gossip_envelope = Envelope::new(from.clone(), to.clone(), gossip_payload);
        let gossip_json = match serde_json::to_value(gossip_envelope) {
            Ok(val) => val,
            Err(_) => return, // Stop retries on serialization failure
        };
        if let Err(e) = transmitter.send(gossip_json.clone()) {
            eprintln!("Failed to send to {} from {} with errro {e}", to, from);
        }
    });
}
