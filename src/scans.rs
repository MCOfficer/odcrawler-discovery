use crate::db::Database;
use crate::elastic;
use crate::Opt;
use anyhow::bail;
use anyhow::Result;
use flate2::read::GzDecoder;
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
}

pub async fn process_scans(opt: &Opt, db: &mut Database) -> Result<()> {
    let mut files = vec![];

    for scan_dir in &opt.scan_dir {
        info!("Scanning directory {}", scan_dir.to_string_lossy());
        for entry in std::fs::read_dir(scan_dir)? {
            let path = entry?.path();
            let name = path.file_name().unwrap().to_string_lossy();
            if path.is_file()
                && (name.ends_with(".json") || name.ends_with(".json.gz") || name.ends_with(".txt"))
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

    let mut reader = BufReader::new(std::fs::File::open(chosen_file)?);

    info!("Extracting files");
    let (root_url, files) = match chosen_file.extension().unwrap().to_string_lossy().as_ref() {
        "txt" => {
            let mut content = String::new();
            reader.read_to_string(&mut content)?;
            let files: Vec<ODScanFile> = content
                .lines()
                .map(|l| ODScanFile { url: l.to_string() })
                .collect();
            if files.is_empty() {
                bail!("Got txt without lines (wat)");
            }

            let root_url_len = chosen_file.file_stem().unwrap().to_string_lossy().len();
            let root_url = files.get(0).unwrap().url.split_at(root_url_len).0;

            (root_url.to_string(), files)
        }
        "json" => {
            let scan_results: ODScanResult = serde_json::from_reader(reader)?;
            (
                scan_results.root.url.clone(),
                collect_files_recursive(scan_results.root),
            )
        }
        "gz" => {
            let scan_results: ODScanResult = serde_json::from_reader(GzDecoder::new(reader))?;
            (
                scan_results.root.url.clone(),
                collect_files_recursive(scan_results.root),
            )
        }
        f => bail!(format!(
            "Got filename with unknown extension, but it was somehow collected: {}",
            f
        )),
    };
    info!("Found {} files", files.len());

    db.save_scan_result(&root_url, &files).await?;

    let opendirectory = root_url;
    drop(files);

    elastic::add_links_from_db(opt, db, &opendirectory).await?;

    let mut processed_dir = chosen_file.parent().unwrap().to_path_buf();
    processed_dir.push("processed");
    std::fs::create_dir_all(&processed_dir)?;
    let mut processed_file = processed_dir;
    processed_file.push(chosen_file.file_name().unwrap());
    info!("Moving file to {}", processed_file.to_string_lossy());
    std::fs::rename(chosen_file, processed_file)?;

    Ok(())
}

fn collect_files_recursive(dir: ODScanDirectory) -> Vec<ODScanFile> {
    let mut files = vec![];

    for subdir in dir.subdirectories {
        files.extend(collect_files_recursive(subdir));
    }
    if let Some(own_files) = dir.files {
        files.extend(own_files);
    }

    files
}

pub fn scan_opendirectories() -> Result<()> {
    warn!("STUB: scan_opendirectories");
    Ok(())
}
