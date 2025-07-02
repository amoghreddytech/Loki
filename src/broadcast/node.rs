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
    pub node_id: String,
    pub node_ids: Vec<String>,
    pub neighbours: Vec<String>,
    pub seen: HashSet<usize>,
    // values here are just things that can
    // be converted to json representation.
    pub transmitter: UnboundedSender<Value>,
    msg_id: Arc<Mutex<usize>>,
    // I need a node_map_of_seen_values
    // say my node_id ix 1 -> this map will be
    // {"n2", [33,54], n3: [42,45], n4 : [65,76]}
    // pub gossip_map: Arc<Mutex<HashMap<String, HashSet<usize>>>>,

    // This hashmap needs to hold the msg_id or the broadcast value and the nodes that it needs to send it to
    pub pending_acks: Arc<Mutex<HashMap<String, HashSet<String>>>>,
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
            // gossip_map: Arc::new(Mutex::new(HashMap::new())),
            // I need to know foe each broadcast which neighbours have not replied
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn get_msg_id(counter: &Arc<Mutex<usize>>) -> usize {
        let mut guard = counter.lock().await;
        let id = *guard;
        *guard = guard.saturating_add(1);
        id
    }

    async fn smarter_broadcast(&self, counter: Arc<Mutex<usize>>, message: usize) {
        // Alaredy added the message value to self.seen.
        // I want to check acknolegements
        // I need some uniue_key
        // node_id that is send the thing and the message and a String
        // These two can be unique I can insert and remove in a hasset
        // or whatever.

        // SO the first time I get a broadcast message on a node I need to
        // Insert into the pending acknolegement array thing.
        // I need to create a loop or a time based something
        // where as long as there are things in the pending acknolodegment
        // Then it keeps sending after a time period.
        let key = format!("{}, {}", self.node_id, message);
        // This gets the pending acknologments for the key
        let pending_acks = {
            let mut pending_acks = self.pending_acks.lock().await;
            match pending_acks.get(&key) {
                // If it's some great we take that
                Some(set) => set.clone(),
                // If it's none we a create a hashset with
                None => {
                    let neighbours = self.neighbours.clone();
                    let set = HashSet::from_iter(neighbours.clone());
                    pending_acks.insert(key.clone(), set.clone());
                    set
                }
            }
        };

        // for the pending acks I am now going to send it to them

        for neighbour in pending_acks {
            if neighbour == self.node_id {
                // This can never be there
                // let's panic
                panic!("I can't be the neighbour of myself, check smarter broadcast");
            }

            let new_message_id = Self::get_msg_id(&counter).await;
            let meta = Metadata::new(Some(new_message_id), None);
            let b_payload = OutgoingPayload::Broadcast(Broadcast::new(meta, message));
            let b_envelope = Envelope::new(self.node_id.clone(), neighbour.clone(), b_payload);
            let json_value = match serde_json::to_value(&b_envelope) {
                Ok(val) => val,
                Err(e) => {
                    eprintln!(
                        "could not convert {:?} to a serde json value with errro {e}",
                        &b_envelope
                    );
                    panic!("")
                }
            };
            if let Err(e) = self.transmitter.send(json_value) {
                eprintln!(
                    "Could not send from broadcast to nighbours, printing is weird cause of borrow rules but do it now :---D \n {e} "
                );
            }
        }
        // I have transmitted my acks. I'm done broadcasting.
    }

    // I'll pass in arcs and stuff later
    // If I'm moving it to different threds
    // Now I'll just push this out into a
    // seperate function so I don't remake this
    // work again.
    async fn dumb_broadcast(&self, counter: Arc<Mutex<usize>>, message: usize) {
        for neighbour in &self.neighbours {
            // If the neighbour is the node that send the message dont broadcast back to it
            if neighbour == &self.node_id {
                continue;
            }

            let new_message_id = Self::get_msg_id(&counter).await;
            let meta = Metadata::new(Some(new_message_id), None);
            let b_payload = OutgoingPayload::Broadcast(Broadcast::new(meta, message));
            let b_envelope = Envelope::new(self.node_id.clone(), neighbour.clone(), b_payload);
            let json_value = match serde_json::to_value(&b_envelope) {
                Ok(val) => val,
                Err(e) => {
                    eprintln!(
                        "could not convert {:?} to a serde json value with errro {e}",
                        &b_envelope
                    );
                    panic!("")
                }
            };
            if let Err(e) = self.transmitter.send(json_value) {
                eprintln!(
                    "Could not send from broadcast to nighbours, printing is weird cause of borrow rules but do it now :---D \n {e} "
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
                // Let's solve for a multiple nodes case and no partitions
                let counter = self.msg_id.clone();
                if new {
                    // I have a new value I have to send it to everyone.
                    // The simlest way is to send continusly to all the neighbours and them
                    // to all the neighbours and this will work as long as
                    // The msg is new in the node
                    // Do I Broadcast this or do I gossip?
                    // I'll stick to broadcasting at the moment
                    // but we'll see
                    self.smarter_broadcast(counter, broadcast.message).await;
                }
                // I will send the broadcastOK for this method
                let meta = Metadata::new(Some(msg_id), broadcast.metadata.msg_id);
                let b_ok_payload =
                    OutgoingPayload::BroadcastOk(BroadcastOk::new(meta, broadcast.message));
                let b_ok_envelope = Envelope::new(msg_dest, msg_src, b_ok_payload);
                Some(b_ok_envelope)
            }
            IncomingPayload::Gossip(gossip) => {
                // What do you do when you get a gossip message
                None
            }

            // Crap I need to do when I get a gossip is Ok message
            IncomingPayload::GossipOk(gossip_ok) => None,
            IncomingPayload::BroadcastOk(ok) => {
                let key = format!("{}, {}", self.node_id.clone(), ok.messge);
                {
                    let mut pending_acks = self.pending_acks.lock().await;
                    if let Some(acks_set) = pending_acks.get_mut(&key) {
                        acks_set.remove(&msg_src);

                        if acks_set.is_empty() {
                            pending_acks.remove(&key);
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
