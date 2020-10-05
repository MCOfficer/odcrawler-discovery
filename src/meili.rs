use crate::{db, Opt};
use anyhow::Result;
use meilisearch_sdk::client::Client;
use meilisearch_sdk::document::Document;
use meilisearch_sdk::progress::Status::Processed;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug)]
pub struct Link {
    pub id: String,
    pub filename: String,
    pub url: String,
}

impl From<db::Link> for Link {
    fn from(l: db::Link) -> Self {
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

impl Document for Link {
    type UIDType = String;

    fn get_uid(&self) -> &Self::UIDType {
        &self.id
    }
}

pub async fn add_links(opt: &Opt, links: Vec<Link>, wait_for_update: bool) -> Result<()> {
    let client = create_client(&opt);
    add_links_async(client, links, wait_for_update).await?;
    Ok(())
}
pub async fn remove_links(opt: &Opt, ids: Vec<String>) -> Result<()> {
    let client = create_client(&opt);
    remove_links_async(client, ids).await?;
    Ok(())
}

fn create_client(opt: &Opt) -> Client {
    Client::new(&opt.meili_url, &opt.meili_key)
}

#[allow(clippy::needless_lifetimes)] // Can't elide lifetimes here
async fn add_links_async<'a>(
    client: Client<'a>,
    links: Vec<Link>,
    wait_for_update: bool,
) -> Result<()> {
    let index = client.get_or_create("links").await?;
    info!("Adding {} documents to Meilisearch", links.len());
    for batch in links.chunks(5_000) {
        let progress = index.add_documents(batch, None).await?;

        if wait_for_update {
            loop {
                let status_res = progress.get_status().await;
                if status_res.is_ok() {
                    if let Processed(status) = status_res? {
                        if let Some(e) = status.error {
                            error!("{}", e);
                        }
                        break;
                    }
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            info!("Indexed: {}", index.get_stats().await?.number_of_documents);
        }
    }
    Ok(())
}

#[allow(clippy::needless_lifetimes)] // Can't elide lifetimes here
async fn remove_links_async<'a>(client: Client<'a>, ids: Vec<String>) -> Result<()> {
    let index = client.get_or_create("links").await?;
    info!("Removing {} documents from Meilisearch", ids.len());
    for batch in ids.chunks(100) {
        index.delete_documents(&batch).await?;
    }
    Ok(())
}
