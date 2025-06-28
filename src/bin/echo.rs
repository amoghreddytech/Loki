use anyhow::Result;
use loki::message::Envelope;
use loki::node::Node;
use loki::{echo::payload::IncomingPayload, message::HandleMessage};
use serde_json::from_str;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, stdin, stdout};

#[tokio::main]
async fn main() -> Result<()> {
    let stdin = stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    let mut stdout = stdout();
    let mut node: Node = Node::new();

    while let Some(line) = lines.next_line().await? {
        let envelope: Envelope<IncomingPayload> = from_str(&line)?;
        eprintln!("Got : {:?}", envelope);

        if let Some(response) = node.handle_message(envelope).await? {
            let response_json = response.to_string();
            eprintln!("responding_with : {:?}", response_json);

            stdout.write_all(response_json.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
    }

    Ok(())
}
