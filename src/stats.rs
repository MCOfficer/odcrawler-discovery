use crate::db::Database;
use crate::{db, Opt};
use anyhow::Result;
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
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

pub async fn create_dump(opt: &Opt, db: &Database) -> Result<()> {
    info!("Creating dump");

    let mut tempfile_path = opt.public_dir.clone();
    tempfile_path.push("dump.zip.tmp");
    let tempfile = File::create(&tempfile_path)?;
    let buffer = BufWriter::new(&tempfile);

    let mut zipfile = zip::ZipWriter::new(buffer);
    let inner_filename = format!("dump-{}.txt", Utc::now().format("%F-%H-%M-%S"));
    zipfile.start_file(inner_filename, zip::write::FileOptions::default())?;

    let ods: Vec<String> = db
        .get_opendirectories(false)
        .await?
        .filter_map(|r| async { r.ok() })
        .map(|od| od.url)
        .collect()
        .await;

    let mut read = Box::pin(
        db::Link::find(&db.db, None, None)
            .await?
            .filter_map(|r| async { r.ok().filter(|l| ods.contains(&l.opendirectory)) }),
    );

    let mut count = 0;
    while let Some(link) = read.next().await {
        let mut url = link.url;
        url.push('\n');
        zipfile.write_all(&*url.into_bytes())?;
        count += 1;
    }
    tempfile.sync_all()?;
    let size = tempfile.metadata()?.len();

    drop(zipfile);
    let mut targetfile = opt.public_dir.clone();
    targetfile.push("dump.zip");

    std::fs::rename(tempfile_path, targetfile)?;

    let dump = Dump {
        url: "https://discovery.odcrawler.xyz/dump.zip".to_string(),
        links: count,
        size,
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
        .count_documents(
            doc! {"unreachable": doc! {"$lt": crate::check_links::DEAD_OD_THRESHOLD}},
            None,
        )
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
