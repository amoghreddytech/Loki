// How do I do this gossip shit,
// Basically I just need a timer
// And I need to periodically keep sending our gossip messages
// and this will help reconsile everything else
// Let's come up with a way to continuously fire gossip messages.

use anyhow::Result;
use async_trait::async_trait;
use tokio::time::{Duration, sleep};

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
    // I'm looking at it from my nodes perspective
    // So I'm lookin at what my node thinks the other nodes have seen haven't seen.
    pub gossip_map: Arc<Mutex<HashMap<String, HashSet<usize>>>>,
    // This hashmap needs to hold the msg_id or the broadcast value and the nodes that it needs to send it to
    pub pending_acks: Arc<Mutex<HashMap<String, HashSet<String>>>>,
    //
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

    async fn get_msg_id(counter: &Arc<Mutex<usize>>) -> usize {
        let mut guard = counter.lock().await;
        let id = *guard;
        *guard = guard.saturating_add(1);
        id
    }

    pub async fn gossip(&self) {
        // This gets the map of what my node thinks that it has seen.
        let gossip_map = self.gossip_map.lock().await;
        let seen_copy = self.seen.clone();

        for neighbour in &self.node_ids {
            let neighbour_map = gossip_map.get(neighbour);
            let neighbour_map = match neighbour_map {
                Some(map) => map.clone(),
                None => HashSet::new(),
            };

            let from = self.node_id.clone();
            let to = neighbour.clone();

            let messges_to_send: HashSet<usize> =
                seen_copy.difference(&neighbour_map).cloned().collect();
            let new_message_id = Self::get_msg_id(&self.msg_id).await;
            let meta = Metadata::new(Some(new_message_id), None);
            let g_payload = OutgoingPayload::Gossip(Gossip::new(messges_to_send, meta));

            let g_envelope = Envelope::new(from, to, g_payload);

            let json_value = match serde_json::to_value(&g_envelope) {
                Ok(val) => val,
                Err(e) => {
                    eprintln!(
                        "could not convert {:?} to a serde json value with errro {e}",
                        &g_envelope
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

    async fn smarter_broadcast_with_retries(&self, counter: Arc<Mutex<usize>>, message: usize) {
        // I need some smart broadcast with like a retry mechanism
        // First ting I need to do is spawn a task that does the broadcast for me.

        let node_id = self.node_id.clone();
        let key = format!("{}, {}", node_id, message);
        let acks_guard = Arc::clone(&self.pending_acks);
        let all_neighbours = self.neighbours.clone();
        let transmitter = self.transmitter.clone();

        tokio::spawn(async move {
            let mut pending_acks = {
                let mut guard = acks_guard.lock().await;
                match guard.get(&key) {
                    // If it's some great we take that
                    Some(set) => set.clone(),
                    // If it's none we a create a hashset with
                    None => {
                        let neighbours = all_neighbours;
                        let set = HashSet::from_iter(neighbours.clone());
                        guard.insert(key.clone(), set.clone());
                        set
                    }
                }
            };

            while !pending_acks.is_empty() {
                for neighbour in pending_acks.iter() {
                    let new_message_id = Self::get_msg_id(&counter).await;
                    let meta = Metadata::new(Some(new_message_id), None);
                    let b_payload = OutgoingPayload::Broadcast(Broadcast::new(meta, message));

                    let b_envelope = Envelope::new(node_id.clone(), neighbour.clone(), b_payload);
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

                    if let Err(e) = transmitter.send(json_value) {
                        eprintln!(
                            "Could not send from broadcast to nighbours, printing is weird cause of borrow rules but do it now :---D \n {e} "
                        );
                    }
                }

                pending_acks = {
                    let guard = acks_guard.lock().await;
                    match guard.get(&key) {
                        // If it's some great we take that
                        Some(set) => set.clone(),
                        // If it's none we a create a hashset with
                        None => break,
                    }
                };

                sleep(Duration::from_secs(1)).await;
            }
        });

        // This gets the pending acknologments for the key

        // for the pending acks I am now going to send it to them

        // I have transmitted my acks. I'm done broadcasting.
    }

    // I'll pass in arcs and stuff later
    // If I'm moving it to different threds
    // Now I'll just push this out into a
    // seperate function so I don't remake this
    // work again.
    #[allow(unused)]
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
    #[allow(unused)]
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
                    self.smarter_broadcast_with_retries(counter, broadcast.message)
                        .await;
                }
                // I will send the broadcastOK for this method
                let meta = Metadata::new(Some(msg_id), broadcast.metadata.msg_id);
                let b_ok_payload =
                    OutgoingPayload::BroadcastOk(BroadcastOk::new(meta, broadcast.message));
                let b_ok_envelope = Envelope::new(msg_dest, msg_src, b_ok_payload);
                Some(b_ok_envelope)
            }
            IncomingPayload::Gossip(gossip) => {
                let recieved_messges = gossip.message;
                // If I reach this point then I can update my map
                self.seen.extend(recieved_messges.clone());
                {
                    let mut map = self.gossip_map.lock().await;
                    map.entry(msg_src.clone())
                        .or_insert_with(HashSet::new)
                        .extend(recieved_messges.clone());
                }

                {
                    let mut pending_acks = self.pending_acks.lock().await;
                    for message in recieved_messges {
                        let key = format!("{}, {}", self.node_id, message);
                        if let Some(neighbours_set) = pending_acks.get_mut(&key) {
                            neighbours_set.remove(&msg_src);
                        }
                    }
                }

                let meta = Metadata::new(Some(msg_id), gossip.metadata.msg_id);
                let g_ok_payload = OutgoingPayload::GossipOk(GossipOk::new(meta));
                let g_ok_envelope = Envelope::new(msg_dest, msg_src, g_ok_payload);
                Some(g_ok_envelope)
            }

            // Crap I need to do when I get a gossip is Ok message
            IncomingPayload::GossipOk(_) => None,
            IncomingPayload::BroadcastOk(ok) => {
                let key = format!("{}, {}", self.node_id.clone(), ok.messge);
                {
                    let mut pending_acks = self.pending_acks.lock().await;
                    if let Some(acks_set) = pending_acks.get_mut(&key) {
                        acks_set.remove(&msg_src);
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
