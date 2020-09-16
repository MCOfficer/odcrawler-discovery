use crate::db::Database;
use crate::meili;
use crate::meili::Link;
use crate::Opt;
use anyhow::Result;
use flate2::read::GzDecoder;
use mongodb::bson::doc;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, Read};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ODScanResult {
    pub root: ODScanDirectory,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ODScanDirectory {
    pub url: String,
    pub subdirectories: Vec<ODScanDirectory>,
    pub files: Option<Vec<ODScanFile>>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ODScanFile {
    pub url: String,
    pub file_size: u64,
}

/// returns true if one has been processed
pub fn process_scans(opt: &Opt, db: &mut Database) -> Result<bool> {
    let mut files = vec![];

    for scan_dir in &opt.scan_dir {
        info!("Scanning directory {}", scan_dir.to_string_lossy());
        for entry in std::fs::read_dir(scan_dir)? {
            let path = entry?.path();
            let name = path.file_name().unwrap().to_string_lossy();
            if path.is_file()
                && (name.ends_with(".json") || name.ends_with("json.gz"))
                && !name.starts_with("https___drive.google.com")
            {
                files.push(path);
            }
        }
    }

    if files.is_empty() {
        return Ok(false);
    }

    let chosen_file = files.choose(&mut rand::thread_rng()).unwrap();
    info!("Selected {}", chosen_file.to_string_lossy());
    let reader = BufReader::new(std::fs::File::open(chosen_file)?);
    let scan_result: ODScanResult = if chosen_file.to_string_lossy().ends_with("gz") {
        serde_json::from_reader(GzDecoder::new(reader))?
    } else {
        serde_json::from_reader(reader)?
    };

    info!("Extracting files");
    let files = collect_files_recursive(&scan_result.root);
    info!("Found {} files", files.len());

    db.save_scan_result(&scan_result, &files)?;

    let mut links = vec![];
    for res in db.get_links(&scan_result.root.url)? {
        let doc = res?;
        links.push(Link {
            id: doc.get_object_id("_id").unwrap().to_string(),
            url: doc.get_str("url").unwrap().to_string(),
            size: doc.get_i64("size").unwrap() as u64,
        });
    }
    meili::add_links(opt, links)?;

    let mut processed_dir = chosen_file.parent().unwrap().to_path_buf();
    processed_dir.push("processed");
    std::fs::create_dir_all(&processed_dir)?;
    let mut processed_file = processed_dir;
    processed_file.push(chosen_file.file_name().unwrap());
    info!("Moving file to {}", processed_file.to_string_lossy());
    std::fs::rename(chosen_file, processed_file)?;

    Ok(true)
}

fn collect_files_recursive(dir: &ODScanDirectory) -> Vec<&ODScanFile> {
    let mut files = vec![];

    for subdir in &dir.subdirectories {
        files.extend(collect_files_recursive(subdir));
    }
    if let Some(own_files) = &dir.files {
        files.extend(own_files);
    }

    files
}

/// returns true if one has been processed
pub fn scan_opendirectories() -> Result<bool> {
    warn!("STUB: scan_opendirectories");
    Ok(true)
}
