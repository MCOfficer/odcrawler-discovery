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

pub async fn update_stats(opt: &Opt, db: &Database, with_new_dump: bool) -> Result<()> {
    info!("Updating stats");
    let old_dump = match read_stats(opt).ok() {
        None => {
            warn!("Stats file couldn't be read, creating a new dump");
            create_dump()?
        }
        Some(stats) => stats.last_dump,
    };

    let dump = if with_new_dump {
        create_dump()?
    } else {
        old_dump
    };

    let stats = create_stats(db, dump).await?;
    save_stats(&opt, &stats)?;

    info!("Stats saved");
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

fn read_stats(opt: &Opt) -> Result<Stats> {
    let mut file = opt.public_dir.clone();
    file.push("stats.json");
    Ok(serde_json::from_reader(File::open(file)?)?)
}

fn save_stats(opt: &Opt, stats: &Stats) -> Result<()> {
    let mut file = opt.public_dir.clone();
    file.push("stats.json");
    serde_json::to_writer_pretty(File::create(file)?, stats)?;
    Ok(())
}

fn create_dump() -> Result<Dump> {
    info!("Creating dump");
    Ok(Dump {
        url: "STUB".to_string(),
        links: 0,
        size: 0,
        created: chrono::offset::Utc::now(),
    })
}
