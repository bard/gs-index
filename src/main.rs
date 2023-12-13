#![allow(unused_imports, dead_code)]

use crate::event_handling::event_to_changeset;
use crate::event_source::event_stream_from_ndjson_stdin;
use clap::Parser;
use futures::pin_mut;
use futures::stream::StreamExt;
use std::io;
use std::pin::Pin;

mod event_handling;
mod event_source;
mod indexer;

// TODO allow choosing behavior on parse errors: do nothing, warn, panic.

#[derive(Parser, Debug)]
struct Args {
    /// Display warnings for parse errors
    #[arg(long, default_value_t = false)]
    show_warnings: bool,
}

const IPFS_GATEWAY: &str = "https://d16c97c2np8a2o.cloudfront.net/ipfs/";

async fn ipfs_getter(cid: String) -> String {
    let url = format!("{}{}", IPFS_GATEWAY, cid);
    reqwest::get(url).await.unwrap().text().await.unwrap()
}

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let args = Args::parse();

    let event_stream = event_stream_from_ndjson_stdin(0, args.show_warnings);
    pin_mut!(event_stream);

    while let Some((event, _index)) = event_stream.next().await {
        let change_set = event_to_changeset(&event, |cid: String| Box::pin(ipfs_getter(cid))).await;
        println!("{}", change_set.sql);
    }

    Ok(())
}
