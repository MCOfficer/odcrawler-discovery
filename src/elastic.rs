use crate::Opt;
use anyhow::Result;
use futures::StreamExt;
use isahc::auth::{Authentication, Credentials};
use isahc::http::header::CONTENT_TYPE;
use isahc::prelude::Configurable;
use isahc::{RequestExt, ResponseExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared::db;
use shared::db::Link;
use std::io::Read;
use std::path::PathBuf;

#[derive(Clone, Serialize, Deserialize)]
pub struct ElasticLink {
    #[serde(skip_serializing)]
    pub id: String,
    pub url: String,
    pub filename: String,
    pub extension: Option<String>,
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
            extension: PathBuf::from(&l.url)
                .extension()
                .map(|e| e.to_string_lossy().into()),
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
        let mut body = BulkBody::default();
        for link in chunk {
            body.items.push(BulkAction::Index(link.clone()));
        }

        bulk_request(opt, body)?;
    }
    Ok(())
}

pub fn remove_bulk(opt: &Opt, ids: &[String]) -> Result<()> {
    info!("Removing {} links from Elasticsearch", ids.len());
    for chunk in ids.chunks(5_000) {
        let mut body = BulkBody::default();
        for id in chunk {
            body.items.push(BulkAction::Delete(id.to_string()));
        }

        bulk_request(opt, body)?;
    }
    Ok(())
}

pub enum BulkAction {
    Delete(String),
    Index(ElasticLink),
}

pub struct BulkBody {
    pub items: Vec<BulkAction>,
}

impl Default for BulkBody {
    fn default() -> Self {
        BulkBody { items: vec![] }
    }
}

impl ToString for BulkBody {
    fn to_string(&self) -> String {
        let mut buffer = String::new();
        for action in &self.items {
            let to_add = match action {
                BulkAction::Delete(id) => {
                    format!("{}\n", json!({"delete": {"_id": id}}).to_string())
                }
                BulkAction::Index(link) => format!(
                    "{}\n{}",
                    json!({"index": {"_id": link.id}}).to_string(),
                    serde_json::to_string(&link).unwrap()
                ),
            };
            buffer = format!("{}{}\n", buffer, to_add);
        }
        buffer
    }
}

pub fn bulk_request(opt: &Opt, body: BulkBody) -> Result<()> {
    let mut response = isahc::http::Request::put(format!("{}/links/_bulk", opt.elastic_url))
        .header(CONTENT_TYPE, "application/json")
        .authentication(Authentication::basic())
        .credentials(Credentials::new("elastic", opt.elastic_pass.clone()))
        .body(body.to_string())?
        .send()?;
    if !response.status().is_success() {
        let mut buffer = String::new();
        response.into_body().read_to_string(&mut buffer)?;
        error!("Got non-success status code, response was\n {}", buffer);
    } else {
        // Cleanly read & drop the response to avoid warnings
        // https://github.com/sagebind/isahc/issues/270#issuecomment-749083844
        let _ = response.copy_to(std::io::sink());
    }
    Ok(())
}
