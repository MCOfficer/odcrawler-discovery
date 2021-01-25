use crate::db::Database;
use crate::{db, Opt};
use anyhow::{Context, Result};
use chrono::serde::ts_milliseconds;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use subprocess::{Exec, Redirection};
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
    size_uncompressed: u64,
    size: u64,
    #[serde(with = "ts_milliseconds")]
    created: DateTime<Utc>,
}

pub async fn create_dump(opt: &Opt, db: &Database) -> Result<()> {
    info!("Creating dump");

    let mut dump_file = opt.public_dir.clone();
    let filename = format!("dump-{}.txt.7z", Utc::now().format("%F-%H-%M-%S"));
    dump_file.push(&filename);

    let mut stdin = Exec::cmd("7z")
        .arg("a")
        .arg(&dump_file)
        .arg("-mx=1")
        .arg("-si")
        .stdout(Redirection::Pipe)
        .stderr(Redirection::Merge)
        .stream_stdin()?;

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
    let mut bytes_written = 0;
    while let Some(link) = read.next().await {
        let mut url = link.url;
        url.push('\n');
        let bytes = url.into_bytes();
        bytes_written += bytes.len();
        stdin.write_all(&*bytes)?;
        count += 1;
    }

    // Generious sleep because there's a tiny race condition
    // dump_file.exists() can be false immediately after 7z is done
    std::thread::sleep(Duration::from_secs(3));

    let dump = Dump {
        url: format!("https://discovery.odcrawler.xyz/{}", filename),
        links: count,
        size_uncompressed: bytes_written as u64,
        size: dump_file.metadata()?.len(),
        created: Utc::now(),
    };
    save_json(&opt, &dump, "dump.json").context("Failed to write json")?;

    // Clean up old dumps
    let dump_filename_pattern = Regex::new(r"dump-[\d-]+.txt.7z")?;
    for result in opt.public_dir.read_dir()? {
        let entry = result?;
        if entry.file_type()?.is_file()
            && entry.file_name().to_string_lossy() != filename
            && dump_filename_pattern.is_match(&entry.file_name().to_string_lossy())
        {
            info!("Removing old dump {}", entry.file_name().to_string_lossy());
            std::fs::remove_file(entry.path())?;
        };
    }

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
