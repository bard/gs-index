use std::pin::Pin;

use crate::event_handling::{
    event_to_changeset, events_to_change_sets_sequential, ChangeSet, Event, EventPayload,
    IpfsGetter, MetaPtr, DB_SCHEMA,
};
use async_stream::stream;
use futures::pin_mut;
use futures::stream::Stream;
use futures::stream::StreamExt;
use sea_query::{Expr, Iden, PostgresQueryBuilder, Query};
use tokio_postgres::{Client, Connection, Error, NoTls, Transaction};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_source::event_stream_from_vector;

    #[tokio::test]
    async fn test_project_created() {
        let ipfs_getter = |_cid: &str| -> String { "".to_string() };
        let events = vec![Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            payload: EventPayload::ProjectCreated {
                project_id: "proj-123".to_string(),
            },
        }];

        let db_dump = event_stream_to_db_dump(events, ipfs_getter).await.unwrap();

        insta::assert_yaml_snapshot!(db_dump);
    }

    #[tokio::test]
    async fn test_project_created_and_metadata_updated() {
        let ipfs_getter = |_cid: &str| -> String { "{ \"foo\": \"bar\" }".to_string() };
        let events = vec![
            Event {
                chain_id: 1,
                address: "0x123".to_string(),
                block_number: 4242,
                log_index: 1,
                payload: EventPayload::ProjectCreated {
                    project_id: "proj-123".to_string(),
                },
            },
            Event {
                chain_id: 1,
                address: "0x123".to_string(),
                block_number: 4242,
                log_index: 2,
                payload: EventPayload::MetadataUpdated {
                    project_id: "proj-123".to_string(),
                    meta_ptr: MetaPtr {
                        pointer: "123".to_string(),
                    },
                },
            },
        ];

        let db_dump = event_stream_to_db_dump(events, ipfs_getter).await.unwrap();

        insta::assert_yaml_snapshot!(db_dump);
    }

    fn dummy_ipfs_getter(_cid: &str) -> String {
        "".into()
    }

    async fn event_stream_to_db_dump(
        events: Vec<Event>,
        ipfs_getter: IpfsGetter,
    ) -> Result<String, Error> {
        let connection_string = "host=localhost user=postgres password=postgres";
        let (mut client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(connection);
        let transaction = client.transaction().await?;
        transaction.batch_execute(DB_SCHEMA).await?;

        let event_stream = event_stream_from_vector(events, 0);
        pin_mut!(event_stream);

        while let Some((event, _index)) = event_stream.next().await {
            let change_set = event_to_changeset(&event, ipfs_getter);
            transaction.simple_query(&change_set.sql).await?;
        }

        let rows = transaction
            .query("SELECT JSON_AGG(project) #>> '{}' FROM project;", &[])
            .await?;
        Ok(rows[0].get(0))
    }
}
