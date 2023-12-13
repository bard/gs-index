use async_stream::stream;
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use serde_json::{from_str, to_string};
use std::fs::File;
use std::io::Write;
use std::io::{self, BufRead, BufReader};
use std::path::Path;

use crate::event_handling::{Event, EventPayload};

pub fn event_stream_from_vector(
    events: Vec<Event>,
    start: usize,
) -> impl Stream<Item = (Event, usize)> {
    let mut index = 0;
    stream! {
        for event in events {
            if index >= start {
                yield (event, index)
            }
            index += 1
        }
    }
}

pub fn event_stream_from_ndjson_file(
    file: File,
    start: usize,
    warn_on_unparseable_items: bool,
) -> impl Stream<Item = (Event, usize)> {
    event_stream_from_buf_reader(io::BufReader::new(file), start, warn_on_unparseable_items)
}

pub fn event_stream_from_ndjson_stdin(
    start: usize,
    warn_on_unparseable_items: bool,
) -> impl Stream<Item = (Event, usize)> {
    event_stream_from_buf_reader(
        io::BufReader::new(io::stdin()),
        start,
        warn_on_unparseable_items,
    )
}

pub fn event_stream_from_buf_reader<R: io::BufRead>(
    reader: R,
    start: usize,
    warn_on_unparseable_items: bool,
) -> impl Stream<Item = (Event, usize)> {
    let mut index = 0;
    stream! {
        for line in reader.lines() {
            if index >= start {
                let line_content = line.unwrap();
                let parse_result: Result<Event, _>  = serde_json::from_str(&line_content);
                match parse_result {
                    Ok(event) => {
                        yield (event, index);
                    }
                    Err(err) => {
                        // TODO propagate error instead of panicking. Might need to
                        // use try_stream!
                        if warn_on_unparseable_items {
                            eprintln!("Warning: skipping event due to parse error: {}. Data: {}", err, line_content);
                        }
                    }
                }
            }
            index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::event_handling::{EventPayload, MetaPtr};

    #[tokio::test]
    async fn test_event_stream_from_ndjson_file() {
        let mut tmp_file = NamedTempFile::new().unwrap();
        for event in get_sample_events() {
            writeln!(tmp_file, "{}", to_string(&event).unwrap()).unwrap();
        }
        let file_name = tmp_file.path();

        let file_event_source =
            event_stream_from_ndjson_file(File::open(file_name).unwrap(), 0, false);
        pin_mut!(file_event_source);

        let (event, index) = file_event_source.next().await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(event.block_number, 10);
    }

    #[tokio::test]
    async fn test_event_stream_from_vector() {
        let event_data = get_sample_events();
        let in_memory_event_source = event_stream_from_vector(event_data, 0);
        pin_mut!(in_memory_event_source);

        let (event, index) = in_memory_event_source.next().await.unwrap();
        assert_eq!(index, 0);
        assert_eq!(event.block_number, 10);

        let (event, index) = in_memory_event_source.next().await.unwrap();
        assert_eq!(index, 1);
        assert_eq!(event.block_number, 20);

        let (event, index) = in_memory_event_source.next().await.unwrap();
        assert_eq!(index, 2);
        assert_eq!(event.block_number, 30);
    }

    #[tokio::test]
    async fn test_events_stream_from_vector_starting_at_index() {
        let event_data = get_sample_events();
        let in_memory_event_source = event_stream_from_vector(event_data, 2);
        pin_mut!(in_memory_event_source);

        let (event, index) = in_memory_event_source.next().await.unwrap();
        assert_eq!(index, 2);
        assert_eq!(event.block_number, 30);
    }

    fn get_sample_events() -> Vec<Event> {
        vec![
            Event {
                chain_id: 1,
                address: "0x123".to_string(),
                block_number: 10,
                log_index: 0,
                data: EventPayload::ProjectCreated {
                    project_id: "proj-123".to_string(),
                },
            },
            Event {
                chain_id: 1,
                address: "0x123".to_string(),
                block_number: 20,
                log_index: 1,
                data: EventPayload::MetadataUpdated {
                    project_id: "proj-123".to_string(),
                    meta_ptr: MetaPtr {
                        pointer: "123".to_string(),
                    },
                },
            },
            Event {
                chain_id: 1,
                address: "0x123".to_string(),
                block_number: 30,
                log_index: 1,
                data: EventPayload::OwnerAdded {
                    project_id: "proj-123".to_string(),
                    owner: "0x123".to_string(),
                },
            },
        ]
    }
}
