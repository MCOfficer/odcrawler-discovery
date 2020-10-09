use crate::db::Database;
use crate::{db, Opt};
use anyhow::Result;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::File;
use wither::bson::doc;
use wither::Model;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Stats {
    total_links: i64,
    total_opendirectories: i64,
    alive_opendirectories: i64,
    last_dump: Dump,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Dump {
    url: String,
    links: u64,
    size: u64,
    #[serde(with = "ts_milliseconds")]
    created: DateTime<Utc>,
}

pub async fn update_stats(_: &Opt, db: &Database, new_dump: bool) -> Result<()> {
    let old_dump = match read_stats().ok() {
        None => create_dump()?,
        Some(stats) => stats.last_dump,
    };

    let dump = if new_dump { create_dump()? } else { old_dump };

    let stats = create_stats(db, dump).await?;
    save_stats(&stats)?;
    Ok(())
}

async fn create_stats(db: &db::Database, last_dump: Dump) -> Result<Stats> {
    let total_links = db::Link::collection(&db.db)
        .estimated_document_count(None)
        .await?;
    let total_opendirectories = db::Link::collection(&db.db)
        .estimated_document_count(None)
        .await?;
    let alive_opendirectories = db::OpenDirectory::collection(&db.db)
        .count_documents(doc! {"unreachable": doc! {"!lt": 5}}, None)
        .await?;

    Ok(Stats {
        total_opendirectories,
        total_links,
        alive_opendirectories,
        last_dump,
    })
}

fn read_stats() -> Result<Stats> {
    Ok(serde_json::from_reader(File::open("stats.json")?)?)
}

fn save_stats(stats: &Stats) -> Result<()> {
    serde_json::to_writer_pretty(File::create("stats.json")?, stats)?;
    Ok(())
}

fn create_dump() -> Result<Dump> {
    Ok(Dump {
        url: "STUB".to_string(),
        links: 0,
        size: 0,
        created: chrono::offset::Utc::now(),
    })
}
