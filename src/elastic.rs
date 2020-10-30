use crate::db::Link;
use crate::{db, Opt};
use anyhow::Result;
use futures::StreamExt;
use isahc::http::header::CONTENT_TYPE;
use isahc::RequestExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::io::Read;
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub struct ElasticLink {
    #[serde(skip_serializing)]
    id: String,
    url: String,
    filename: String,
}

impl From<db::Link> for ElasticLink {
    fn from(l: Link) -> Self {
        Self {
            id: l.id.unwrap().to_string(),
            filename: PathBuf::from(&l.url)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            url: l.url,
        }
    }
}

pub async fn add_links_from_db(opt: &Opt, db: &db::Database, od: &str) -> Result<()> {
    db.get_links(&od)
        .await?
        .filter_map(|l| async { Some(l.ok()?.into()) })
        .chunks(50_000)
        .for_each(|chunk| async move {
            if let Err(e) = add_bulk(opt, &chunk) {
                warn!("Failed to add links to Elasticsearch: {}", e)
            }
        })
        .await;
    Ok(())
}

pub fn add_bulk(opt: &Opt, links: &[ElasticLink]) -> Result<()> {
    info!("Adding {} links to Elasticsearch", links.len());
    for chunk in links.chunks(5_000) {
        let body = chunk
            .iter()
            .map(|l| {
                format!(
                    "{}\n{}",
                    json!({"index": {"_id": l.id}}).to_string(),
                    serde_json::to_string(l).unwrap()
                )
            })
            .fold("".to_string(), |buf, l| format!("{}{}\n", buf, l));

        bulk_request(opt, body)?;
    }
    Ok(())
}

pub fn remove_bulk(opt: &Opt, ids: &[String]) -> Result<()> {
    info!("Removing {} links from Elasticsearch", ids.len());
    for chunk in ids.chunks(5_000) {
        let body = chunk
            .iter()
            .map(|id| format!("{}\n", json!({"delete": {"_id": id}}).to_string()))
            .fold("".to_string(), |buf, l| format!("{}{}\n", buf, l));

        bulk_request(opt, body)?;
    }
    Ok(())
}

fn bulk_request(opt: &Opt, body: String) -> Result<()> {
    let response = isahc::http::Request::put(format!("{}/links/_bulk", opt.elastic_url))
        .header(CONTENT_TYPE, "application/json")
        .body(body)?
        .send()?;
    if !response.status().is_success() {
        let mut buffer = String::new();
        response.into_body().read_to_string(&mut buffer)?;
        error!("Got non-success status code, response was\n {}", buffer);
    }
    Ok(())
}
