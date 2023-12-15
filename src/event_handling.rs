use std::pin::Pin;

use sea_query::{Expr, Iden, PostgresQueryBuilder, Query};
use serde::{Deserialize, Serialize};
use serde_json::{from_str, to_string};
use tokio_postgres::{Client, Error, NoTls, Transaction};

// TODO add round table
pub const DB_SCHEMA: &str = r#"
CREATE TABLE project (chain_id INTEGER NOT NULL, project_id VARCHAR NOT NULL, created_at_block BIGINT NOT NULL, metadata JSONB, PRIMARY KEY(chain_id, project_id));
CREATE TABLE round (chain_id INTEGER NOT NULL, round_address VARCHAR NOT NULL, created_at_block BIGINT NOT NULL);
"#;

#[derive(Iden)]
enum Project {
    Table,
    ChainId,
    ProjectId,
    CreatedAtBlock,
    Metadata,
    Owners,
}

#[derive(Iden)]
enum Round {
    Table,
    ChainId,
    RoundAddress,
    CreatedAtBlock,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    // TODO use stricter types
    pub chain_id: i32,
    pub address: String,
    pub block_number: i32,
    pub log_index: i32,
    pub data: EventPayload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
pub enum EventPayload {
    ProjectCreated {
        #[serde(rename = "projectID")]
        project_id: String,
    },
    MetadataUpdated {
        #[serde(rename = "projectID")]
        project_id: String,
        #[serde(rename = "metaPtr")]
        meta_ptr: MetaPtr,
    },
    OwnerAdded {
        #[serde(rename = "projectID")]
        project_id: String,
        owner: String,
    },
    OwnerRemoved {
        #[serde(rename = "projectID")]
        project_id: String,
        owner: String,
    },
    RoundCreated {
        #[serde(rename = "roundAddress")]
        round_address: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MetaPtr {
    pub pointer: String,
}

pub struct ChangeSet {
    pub sql: String,
}

pub async fn event_to_changeset(
    event: &Event,
    ipfs_getter: impl Fn(String) -> Pin<Box<dyn futures::Future<Output = String> + Send>>,
) -> ChangeSet {
    match &event.data {
        EventPayload::ProjectCreated { project_id } => ChangeSet {
            sql: Query::insert()
                .into_table(Project::Table)
                .columns([
                    Project::ChainId,
                    Project::ProjectId,
                    Project::CreatedAtBlock,
                ])
                .values_panic([
                    event.chain_id.into(),
                    project_id.into(),
                    event.block_number.into(),
                ])
                .to_string(PostgresQueryBuilder),
        },

        EventPayload::MetadataUpdated {
            meta_ptr,
            project_id,
        } => {
            let metadata = ipfs_getter(meta_ptr.pointer.clone()).await;
            ChangeSet {
                sql: Query::update()
                    .table(Project::Table)
                    .values([(Project::Metadata, metadata.into())])
                    .and_where(Expr::col(Project::ChainId).eq(event.chain_id))
                    .and_where(Expr::col(Project::ProjectId).eq(project_id))
                    .to_string(PostgresQueryBuilder),
            }
        }

        EventPayload::OwnerAdded { project_id, owner } => {
            ChangeSet {
                // TODO build SQL safely
                sql: format!(
                    r#"UPDATE "project" SET "owners" = ("owners" || '["{}"]') WHERE "chain_id" = {} AND "project_id" = '{}'"#,
                    owner, event.chain_id, project_id
                ),
            }
        }

        EventPayload::OwnerRemoved { project_id, owner } => {
            ChangeSet {
                // TODO build SQL safely
                sql: format!(
                    r#"UPDATE "project" SET "owners" = ("owners" - '{}') WHERE "chain_id" = {} AND "project_id" = '{}'"#,
                    owner, event.chain_id, project_id
                ),
            }
        }

        EventPayload::RoundCreated { round_address } => ChangeSet {
            sql: Query::insert()
                .into_table(Round::Table)
                .columns([Round::ChainId, Round::RoundAddress, Round::CreatedAtBlock])
                .values_panic([
                    event.chain_id.into(),
                    round_address.into(),
                    event.block_number.into(),
                ])
                .to_string(PostgresQueryBuilder),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_ipfs_getter(_url: String) -> Pin<Box<dyn futures::Future<Output = String> + Send>> {
        Box::pin(async move { r#"{ "foo": "bar" }"#.to_string() })
    }

    #[test]
    fn test_parse_event_json() {
        let event_data = r#"{"chainId":58008,"data":{"type":"ProjectCreated","projectID":"0x00","owner":"0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"},"address":"0x6294bed5B884Ae18bf737793Ef9415069Bf4bc11","signature":"ProjectCreated(uint256,address)","transactionHash":"0xdeae76e835f3d33f09c6e23b6ce5a831a6f8d314f4ac1823369f34b3bba0e0df","blockNumber":1070024,"logIndex":0}"#;
        let event: Event = from_str(&event_data).unwrap();
        assert_eq!(event.chain_id, 58008);
        assert!(matches!(event.data, EventPayload::ProjectCreated { .. }));
    }

    #[tokio::test]
    async fn test_handle_project_created() {
        let event = Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            data: EventPayload::ProjectCreated {
                project_id: "proj-123".to_string(),
            },
        };

        assert_eq!(
            event_to_changeset(&event, dummy_ipfs_getter).await.sql,
            r#"INSERT INTO "project" ("chain_id", "project_id", "created_at_block") VALUES (1, 'proj-123', 4242)"#
        );
    }

    #[tokio::test]
    async fn test_handle_metadata_updated() {
        let event = Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            data: EventPayload::MetadataUpdated {
                project_id: "proj-123".to_string(),
                meta_ptr: MetaPtr {
                    pointer: "123".to_string(),
                },
            },
        };

        assert_eq!(
            event_to_changeset(&event, dummy_ipfs_getter).await.sql,
            r#"UPDATE "project" SET "metadata" = E'{ \"foo\": \"bar\" }' WHERE "chain_id" = 1 AND "project_id" = 'proj-123'"#
        );
    }

    #[tokio::test]
    async fn test_handle_owner_added() {
        let event = Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            data: EventPayload::OwnerAdded {
                project_id: "proj-123".to_string(),
                owner: "0x123".to_string(),
            },
        };

        assert_eq!(
            event_to_changeset(&event, dummy_ipfs_getter).await.sql,
            r#"UPDATE "project" SET "owners" = ("owners" || '["0x123"]') WHERE "chain_id" = 1 AND "project_id" = 'proj-123'"#
        );
    }

    #[tokio::test]
    async fn test_handle_owner_removed() {
        let event = Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            data: EventPayload::OwnerRemoved {
                project_id: "proj-123".to_string(),
                owner: "0x123".to_string(),
            },
        };

        assert_eq!(
            event_to_changeset(&event, dummy_ipfs_getter).await.sql,
            r#"UPDATE "project" SET "owners" = ("owners" - '0x123') WHERE "chain_id" = 1 AND "project_id" = 'proj-123'"#
        );
    }

    #[tokio::test]
    async fn test_handle_contract_round_created() {
        let event = Event {
            chain_id: 1,
            address: "0x123".to_string(),
            block_number: 4242,
            log_index: 1,
            data: EventPayload::RoundCreated {
                round_address: "0x123".to_string(),
            },
        };

        assert_eq!(
            event_to_changeset(&event, dummy_ipfs_getter).await.sql,
            r#"INSERT INTO "round" ("chain_id", "round_address", "created_at_block") VALUES (1, '0x123', 4242)"#
        );
    }
}
