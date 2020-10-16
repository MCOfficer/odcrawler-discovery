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
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Dump {
    url: String,
    links: u64,
    size: u64,
    #[serde(with = "ts_milliseconds")]
    created: DateTime<Utc>,
}

pub async fn create_dump(opt: &Opt, _: &Database) -> Result<()> {
    info!("Creating dump");

    let dump = Dump {
        url: "STUB".to_string(),
        links: 0,
        size: 0,
        created: Utc::now(),
    };
    save_json(&opt, &dump, "dump.json")?;

    Ok(())
}

pub async fn update_stats(opt: &Opt, db: &Database) -> Result<()> {
    info!("Updating stats");

    let total_links = db::Link::collection(&db.db)
        .estimated_document_count(None)
        .await?;
    let total_opendirectories = db::OpenDirectory::collection(&db.db)
        .estimated_document_count(None)
        .await?;
    let alive_opendirectories = db::OpenDirectory::collection(&db.db)
        .count_documents(doc! {"unreachable": doc! {"$lt": 5}}, None)
        .await?;

    let stats = Stats {
        total_opendirectories,
        total_links,
        alive_opendirectories,
    };
    save_json(&opt, &stats, "stats.json")?;

    info!("Stats saved");
    Ok(())
}

fn save_json(opt: &Opt, dto: &impl Serialize, filename: &str) -> Result<()> {
    let mut file = opt.public_dir.clone();
    file.push(filename);
    serde_json::to_writer_pretty(File::create(file)?, dto)?;
    Ok(())
}
