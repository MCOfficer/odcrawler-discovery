#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

use crate::db::Database;
use anyhow::Result;
use futures::StreamExt;
use shrust::{Shell, ShellIO};
use simplelog::{ConfigBuilder, LevelFilter, WriteLogger};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use structopt::StructOpt;
use wither::bson::doc;
use wither::Model;

mod check_links;
mod db;
mod meili;
mod scans;
mod stats;

#[derive(StructOpt, Debug, Clone)]
pub struct Opt {
    /// The Path to the OpenDirectoryDownloader executable. This executable's scan dir is inferred automatically.
    #[structopt(
        long,
        default_value = "OpenDirectoryDownloader/OpenDirectoryDownloader"
    )]
    odd: PathBuf,

    /// Additional scan directories
    #[structopt(long)]
    scan_dir: Vec<PathBuf>,

    /// Meilisearch address
    #[structopt(long, default_value = "http://127.0.0.1:7700")]
    meili_url: String,

    /// Meilisearch master key
    #[structopt(long, env = "MEILI_MASTER_KEY", default_value = "")]
    meili_key: String,

    /// Directory for public files (e.g. stats.json)
    #[structopt(long, default_value = ".")]
    public_dir: PathBuf,
}

#[async_std::main]
async fn main() {
    WriteLogger::init(
        LevelFilter::Info,
        ConfigBuilder::new()
            .add_filter_ignore_str("surf::middleware::logger")
            .build(),
        File::create("odcrawler-discovery.log").unwrap(),
    )
    .unwrap();

    let mut opt = Opt::from_args();
    let mut odd_scan_dir = opt.odd.parent().unwrap().to_path_buf();
    odd_scan_dir.push("Scans");
    std::fs::create_dir_all(&odd_scan_dir).unwrap();
    opt.scan_dir.push(odd_scan_dir);
    dbg!(&opt);

    let db = db::Database::new().await.unwrap();

    let scheduler_db = db.clone();
    let scheduler_opt = opt.clone();
    let _scheduler_handle = std::thread::spawn(|| {
        async_std::task::block_on(scheduler_loop(scheduler_opt, scheduler_db))
    });

    let mut shell = Shell::new(());
    shell.new_command("add", "Adds an OD to the database", 1, |io, _, s| {
        writeln!(io, "STUB: add {} to DB", s[0])?;
        warn!("STUB: add {} to DB", s[0]);
        Ok(())
    });
    shell.new_command_noargs(
        "export",
        "Exports all links to Meilisearch",
        move |io, _| {
            let db_clone = db.clone();
            let opt_clone = opt.clone();
            if let Err(e) = async_std::task::block_on(export_all(&opt_clone, &db_clone)) {
                writeln!(io, "Error while exporting links: {}", e)?;
                error!("Error while exporting links: {}", e);
            };
            Ok(())
        },
    );
    shell.run_loop(&mut ShellIO::default());
}

pub async fn export_all(opt: &Opt, db: &Database) -> Result<()> {
    info!("Exporting all links to Meilisearch");

    let ods: Vec<String> = db
        .get_opendirectories(false)
        .await?
        .filter_map(|r| async { r.ok().map(|od| od.url) })
        .collect()
        .await;

    let total = AtomicUsize::new(0);

    db::Link::find(&db.db, doc! {}, None)
        .await?
        .filter_map(|l| async { l.ok().filter(|l| ods.contains(&l.opendirectory)) })
        .map(|l| l.into())
        .chunks(50_000)
        .for_each_concurrent(2, |chunk| async {
            let len = chunk.len();
            if let Err(e) = meili::add_links(opt, chunk, false).await {
                error!("Error adding links to Meilisearch: {}", e);
            };
            total.fetch_add(len, Ordering::Relaxed);
        })
        .await;

    info!("Exported {} documents", total.into_inner());

    Ok(())
}

#[async_trait]
trait Schedule {
    fn name(&self) -> &str;
    /// How often this should run:
    /// 1 = run every time
    /// 5 = run every 5 times
    fn frequency(&self) -> u16;
    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()>;
}

struct ProcessResults;
#[async_trait]
impl Schedule for ProcessResults {
    fn name(&self) -> &str {
        "process results"
    }

    fn frequency(&self) -> u16 {
        2
    }

    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()> {
        scans::process_scans(opt, db).await
    }
}

struct ScanOpendirectory;
#[async_trait]
impl Schedule for ScanOpendirectory {
    fn name(&self) -> &str {
        "scan opendirectory"
    }

    fn frequency(&self) -> u16 {
        4
    }

    async fn run(&self, _: &Opt, _: &mut Database) -> Result<()> {
        scans::scan_opendirectories()
    }
}

struct CheckLinks;
#[async_trait]
impl Schedule for CheckLinks {
    fn name(&self) -> &str {
        "check links"
    }

    fn frequency(&self) -> u16 {
        25
    }

    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()> {
        check_links::check_opendirectories(opt, db).await
    }
}

struct UpdateStats;
#[async_trait]
impl Schedule for UpdateStats {
    fn name(&self) -> &str {
        "update stats"
    }

    fn frequency(&self) -> u16 {
        3
    }

    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()> {
        stats::update_stats(opt, db, false).await
    }
}

async fn scheduler_loop(opt: Opt, mut db: Database) {
    info!("Started scheduler thread");

    let schedule_tasks: [Box<dyn Schedule>; 4] = [
        Box::new(ProcessResults),
        Box::new(ScanOpendirectory),
        Box::new(CheckLinks),
        Box::new(UpdateStats),
    ];

    let mut counter: u16 = 0;
    loop {
        std::thread::sleep(Duration::from_secs(3));

        for task in &schedule_tasks {
            if counter % task.frequency() == 0 {
                if let Err(e) = task.run(&opt, &mut db).await {
                    error!("Failed to run task '{}' due to error: \n{}", task.name(), e);
                    break;
                };
            }
        }
        counter = counter.overflowing_add(1).0;
    }
}
