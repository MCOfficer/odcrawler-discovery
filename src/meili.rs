use crate::Opt;
use anyhow::Result;
use meilisearch_sdk::client::Client;
use meilisearch_sdk::document::Document;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Link {
    pub id: String,
    pub url: String,
}

impl Document for Link {
    type UIDType = String;

    fn get_uid(&self) -> &Self::UIDType {
        &self.id
    }
}

pub fn add_links(opt: &Opt, links: Vec<Link>) -> Result<()> {
    let client: Client = Client::new(&opt.meili_url, &opt.meili_key);
    tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_all()
        .build()?
        .block_on(add_links_async(client, links))?;
    Ok(())
}

// No, you can't elide lifetimes here
async fn add_links_async<'a>(client: Client<'a>, links: Vec<Link>) -> Result<()> {
    let index = client.get_or_create("links").await?;
    info!("Adding {} documents to Meilisearch", links.len());
    for batch in links.chunks(100) {
        index.add_documents(batch, None).await?;
    }
    Ok(())
}
