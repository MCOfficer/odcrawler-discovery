#[macro_use]
extern crate log;
#[macro_use]
extern crate async_trait;

use crate::db::Database;
use anyhow::Result;
use shrust::{Shell, ShellIO};
use simplelog::{ConfigBuilder, LevelFilter, WriteLogger};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

mod check_links;
mod db;
mod meili;
mod scans;

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

    let scheduler_db = db;
    let scheduler_opt = opt;
    let _scheduler_handle = std::thread::spawn(|| {
        async_std::task::block_on(scheduler_loop(scheduler_opt, scheduler_db))
    });

    let mut shell = Shell::new(());
    shell.new_command("add", "Adds an OD to the database", 1, |io, _, s| {
        writeln!(io, "STUB: add {} to DB", s[0])?;
        warn!("STUB: add {} to DB", s[0]);
        Ok(())
    });
    shell.run_loop(&mut ShellIO::default());
}

#[async_trait]
trait Schedule {
    fn name(&self) -> &str;
    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()>;
}

struct ProcessResults;
#[async_trait]
impl Schedule for ProcessResults {
    fn name(&self) -> &str {
        "process results"
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

    async fn run(&self, opt: &Opt, db: &mut Database) -> Result<()> {
        check_links::check_opendirectories(opt, db).await
    }
}

async fn scheduler_loop(opt: Opt, mut db: Database) {
    info!("Started scheduler thread");

    let schedule_tasks: [Box<dyn Schedule>; 3] = [
        Box::new(ProcessResults),
        Box::new(ScanOpendirectory),
        Box::new(CheckLinks),
    ];

    loop {
        std::thread::sleep(Duration::from_secs(3));

        for task in &schedule_tasks {
            if let Err(e) = task.run(&opt, &mut db).await {
                error!("Failed to run task '{}' due to error: \n{}", task.name(), e);
                break;
            };
        }
    }
}
