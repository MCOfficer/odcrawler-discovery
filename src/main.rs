#[macro_use]
extern crate log;

use crate::db::Database;
use anyhow::{Error, Result};
use shrust::{Shell, ShellIO};
use simplelog::{Config, LevelFilter, WriteLogger};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use structopt::StructOpt;

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

fn main() {
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

    let db = db::Database::new().unwrap();

    let scheduler_db = db;
    let scheduler_opt = opt;
    let _scheduler_handle = std::thread::spawn(|| scheduler_loop(scheduler_opt, scheduler_db));

    let mut shell = Shell::new(());
    shell.new_command("add", "Adds an OD to the database", 1, |io, _, s| {
        writeln!(io, "STUB: add {} to DB", s[0])?;
        warn!("STUB: add {} to DB", s[0]);
        Ok(())
    });
    shell.run_loop(&mut ShellIO::default());
}

trait Schedule {
    fn name(&self) -> &str;
    fn run(&self, opt: &Opt, db: &mut Database) -> Result<bool>;
}

struct ProcessResults;
impl Schedule for ProcessResults {
    fn name(&self) -> &str {
        "process results"
    }

    fn run(&self, opt: &Opt, db: &mut Database) -> Result<bool, Error> {
        scans::process_scans(opt, db)
    }
}

struct ScanOpendirectory;
impl Schedule for ScanOpendirectory {
    fn name(&self) -> &str {
        "scan opendirectory"
    }

    fn run(&self, _: &Opt, _: &mut Database) -> Result<bool, Error> {
        scans::scan_opendirectories()
    }
}

fn scheduler_loop(opt: Opt, mut db: Database) {
    info!("Started scheduler thread");

    let schedule_tasks: [Box<dyn Schedule>; 2] =
        [Box::new(ProcessResults {}), Box::new(ScanOpendirectory {})];

    loop {
        std::thread::sleep(Duration::from_secs(3));

        for task in &schedule_tasks {
            match task.run(&opt, &mut db) {
                Ok(did_something) => {
                    if did_something {
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to run task '{}' due to error: \n{}", task.name(), e);
                    break;
                }
            };
        }
    }
}
