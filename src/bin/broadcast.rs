use anyhow::Result;
use loki::broadcast::node::BroadCastNode;
use loki::message::HandleMessage;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};
use tokio::sync::mpsc::unbounded_channel;

#[tokio::main]
async fn main() -> Result<()> {
    let stdin = stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let (tx, mut rx) = unbounded_channel();
    let sender = tx.clone();
    let mut node: BroadCastNode = BroadCastNode::new(sender);

    tokio::spawn(async move {
        while let Some(response) = rx.recv().await {
            let mut stdout = stdout();
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
        let envelope = serde_json::from_str(&line)?;
        if let Some(response) = node.handle_message(envelope).await? {
            tx.send(response)?;
        }
    }

    Ok(())
}
