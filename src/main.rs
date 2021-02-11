#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

use crate::db::Database;
use crate::elastic::ElasticLink;
use anyhow::Result;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use shrust::{Shell, ShellIO};
use simplelog::{Config, LevelFilter, WriteLogger};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use structopt::StructOpt;
use wither::bson::doc;
use wither::Model;

mod check_links;
mod db;
mod elastic;
mod scans;
mod stats;

macro_rules! enclose {
    ( ($( $x:ident ),*) $y:expr ) => {
        {
            $(let $x = $x.clone();)*
            $y
        }
    };
}

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

    /// Elasticsearch address
    #[structopt(long, default_value = "http://127.0.0.1:9200")]
    elastic_url: String,

    /// Elasticsearch password
    #[structopt(long, env = "ELASTIC_PASS", default_value = "")]
    elastic_pass: String,

    /// Directory for public files (e.g. stats.json)
    #[structopt(long, default_value = ".")]
    public_dir: PathBuf,

    // Disables the scheduler, allowing for exports etc.
    #[structopt(long)]
    disable_scheduler: bool,
}

#[async_std::main]
async fn main() {
    WriteLogger::init(
        LevelFilter::Info,
        Config::default(),
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

    if !opt.disable_scheduler {
        let scheduler_db = db.clone();
        let scheduler_opt = opt.clone();
        let _scheduler_handle = std::thread::spawn(|| {
            async_std::task::block_on(scheduler_loop(scheduler_opt, scheduler_db))
        });
    }

    let mut shell = Shell::new(());
    shell.new_command("add", "Adds an OD to the database", 1, |io, _, s| {
        writeln!(io, "STUB: add {} to DB", s[0])?;
        warn!("STUB: add {} to DB", s[0]);
        Ok(())
    });
    shell.new_command_noargs(
        "dump",
        "Creates a new Dump",
        enclose! { (opt, db) move |io, _| {
            if let Err(e) = async_std::task::block_on(stats::create_dump(&opt, &db)) {
                writeln!(io, "Error while creating dump: {}", e)?;
                error!("Error while creating dump: {}", e);
            };
            Ok(())
        }},
    );
    shell.new_command_noargs(
        "export",
        "Exports all links to Elasticsearch",
        enclose! { (db, opt) move |io, _| {
            if let Err(e) = async_std::task::block_on(export_all(&opt, &db)) {
                writeln!(io, "Error while exporting links: {}", e)?;
                error!("Error while exporting links: {}", e);
            };
            Ok(())
        }},
    );
    shell.run_loop(&mut ShellIO::default());
}

pub async fn export_all(opt: &Opt, db: &Database) -> Result<()> {
    info!("Adding or removing all links to/from Elasticsearch");

    let alive_ods: Vec<String> = db
        .get_opendirectories(false)
        .await?
        .filter_map(|r| async { r.ok().map(|od| od.url) })
        .collect()
        .await;

    let total = db::Link::collection(&db.db)
        .estimated_document_count(None)
        .await? as u64;
    let exported = AtomicU64::new(0);
    let pb = ProgressBar::new(total).with_style(
        ProgressStyle::default_bar().template("{percent}%, ETA {eta}   {wide_bar}   {pos}/~{len}"),
    );
    pb.enable_steady_tick(100);

    db::Link::find(&db.db, doc! {}, None)
        .await?
        .filter_map(|l| async { l.ok() })
        .map(|l| (alive_ods.contains(&l.opendirectory), ElasticLink::from(l)))
        .chunks(5_000)
        .for_each_concurrent(4, |chunk| async {
            let len = chunk.len();
            let chunk = chunk;
            let mut body = elastic::BulkBody::default();
            for (is_alive, link) in chunk {
                body.items.push(if is_alive {
                    elastic::BulkAction::Index(link)
                } else {
                    elastic::BulkAction::Delete(link.id)
                });
            }
            if let Err(e) = elastic::bulk_request(opt, body) {
                error!("Error exporting links to Elasticsearch: {}", e);
            };

            exported.fetch_add(len as u64, Ordering::Relaxed);

            let exported_inner = exported.load(Ordering::Relaxed);
            if exported_inner > total {
                pb.set_length(total);
            }
            pb.set_position(exported_inner as u64);
        })
        .await;

    info!("Exported {} documents", exported.into_inner());

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
        100
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
        stats::update_stats(opt, db).await
    }
}

struct CreateDump;
#[async_trait]
impl Schedule for CreateDump {
    fn name(&self) -> &str {
        "create dump"
    }

    fn frequency(&self) -> u16 {
        250
    }

    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()> {
        stats::create_dump(opt, db).await
    }
}

async fn scheduler_loop(opt: Opt, mut db: Database) {
    info!("Started scheduler thread");

    let schedule_tasks: [Box<dyn Schedule>; 5] = [
        Box::new(ProcessResults),
        Box::new(ScanOpendirectory),
        Box::new(CheckLinks),
        Box::new(UpdateStats),
        Box::new(CreateDump),
    ];

    let mut counter: u16 = 1;
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
