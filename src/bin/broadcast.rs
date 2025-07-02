use anyhow::Result;
use loki::broadcast::node::BroadCastNode;
use loki::broadcast::payload::IncomingPayload;
use loki::message::{Envelope, HandleMessage};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use tokio::sync::Mutex;
use tokio::sync::mpsc::unbounded_channel;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    let stdin = stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let (tx, mut rx) = unbounded_channel();
    let sender = tx.clone();
    let node = Arc::new(Mutex::new(BroadCastNode::new(sender)));

    {
        let node_clone = node.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(1000)).await;
                let locked = node_clone.lock().await;
                locked.gossip().await;
            }
        });
    }

    let mut stdout = stdout();
    tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            let response_json = response.to_string();
            if let Err(e) = stdout.write_all(response_json.as_bytes()).await {
                eprintln!("Failed to write to stdout: {:?}", e);
                break;
            }
            if let Err(e) = stdout.write_all(b"\n").await {
                eprintln!("Failed to print new line stdout: {:?}", e);
                break;
            };
            if let Err(e) = stdout.flush().await {
                eprintln!("Failed to flush to stdout: {:?}", e);
                break;
            };
        }
    });

    while let Some(line) = lines.next_line().await? {
        match serde_json::from_str::<Envelope<IncomingPayload>>(&line) {
            Ok(envelope) => {
                if let Some(response) = node.lock().await.handle_message(envelope).await? {
                    tx.send(response)?;
                }
            }
            Err(e) => {
                eprintln!(
                    "Failed to deserialize incoming message: {}\nLine: {}",
                    e, line
                );
            }
        }
    }

    Ok(())
}
