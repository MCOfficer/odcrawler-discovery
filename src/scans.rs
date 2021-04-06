use crate::elastic;
use crate::Opt;
use anyhow::bail;
use anyhow::Result;
use flate2::read::GzDecoder;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use shared::db::{Database, SaveResult};
use std::io::BufReader;
use std::time::Duration;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct OdScanResult {
    pub root: OdScanDirectory,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct OdScanDirectory {
    pub url: String,
    pub subdirectories: Vec<OdScanDirectory>,
    pub files: Option<Vec<OdScanFile>>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct OdScanFile {
    pub url: String,
}

pub async fn process_scans(opt: &Opt, db: &mut Database) -> Result<()> {
    let mut files = vec![];

    for scan_dir in &opt.scan_dir {
        info!("Scanning directory {}", scan_dir.to_string_lossy());
        for entry in std::fs::read_dir(scan_dir)? {
            let path = entry?.path();
            let name = path.file_name().unwrap().to_string_lossy();
            if path.is_file()
                && (name.ends_with(".json") || name.ends_with(".json.gz"))
                && !name.starts_with("https___drive.google.com")
            {
                files.push(path);
            }
        }
    }

    if files.is_empty() {
        return Ok(());
    }

    let chosen_file = files.choose(&mut rand::thread_rng()).unwrap();
    info!("Selected {}", chosen_file.to_string_lossy());

    let reader = BufReader::new(std::fs::File::open(chosen_file)?);

    info!("Deserializing");
    let (root_url, mut files) = match chosen_file.extension().unwrap().to_string_lossy().as_ref() {
        "json" => {
            let scan_results: OdScanResult = serde_json::from_reader(reader)?;
            (
                scan_results.root.url.clone(),
                collect_files(scan_results.root),
            )
        }
        "gz" => {
            let scan_results: OdScanResult = serde_json::from_reader(GzDecoder::new(reader))?;
            (
                scan_results.root.url.clone(),
                collect_files(scan_results.root),
            )
        }
        f => bail!(format!(
            "Got filename with unknown extension, but it was somehow collected: {}",
            f
        )),
    };
    info!("Found {} files", files.len());

    let is_reachable =
        crate::check_links::link_is_reachable(&root_url, Duration::from_secs(30), true).await;
    let links = files
        .drain(..)
        .map(|f| shared::db::Link {
            id: None,
            url: f.url,
            opendirectory: root_url.clone(),
        })
        .collect();
    let save_result = db.save_scan_result(&root_url, links, is_reachable).await?;
    match save_result {
        SaveResult::Success => {
            if is_reachable {
                elastic::add_links_from_db(opt, db, &root_url).await?;
            }
        }
        _ => {
            error!("Couldn't save due to existing links/OD")
        }
    }

    let mut processed_dir = chosen_file.parent().unwrap().to_path_buf();
    processed_dir.push("processed");
    std::fs::create_dir_all(&processed_dir)?;
    let mut processed_file = processed_dir;
    processed_file.push(chosen_file.file_name().unwrap());
    info!("Moving file to {}", processed_file.to_string_lossy());
    std::fs::rename(chosen_file, processed_file)?;

    Ok(())
}

fn collect_files(dir: OdScanDirectory) -> Vec<OdScanFile> {
    info!("Extracting files");
    collect_files_recursive(dir)
}

fn collect_files_recursive(dir: OdScanDirectory) -> Vec<OdScanFile> {
    let mut files = dir.files.unwrap_or_default();

    for subdir in dir.subdirectories {
        files.extend(collect_files_recursive(subdir));
    }

    files
}

pub fn scan_opendirectories() -> Result<()> {
    warn!("STUB: scan_opendirectories");
    Ok(())
}
