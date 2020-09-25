use crate::scans::{ODScanFile, ODScanResult};
use anyhow::Result;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use wither::bson::{doc, oid::ObjectId, Document};
use wither::mongodb::options::ClientOptions;
use wither::mongodb::*;
use wither::prelude::*;
use wither::ModelCursor;

#[derive(Debug, Model, Serialize, Deserialize)]
pub struct OpenDirectory {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub url: String,
}

#[derive(Debug, Model, Serialize, Deserialize)]
pub struct Link {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub url: String,
}

impl Migrating for Link {
    fn migrations() -> Vec<Box<dyn wither::Migration>> {
        vec![Box::new(wither::IntervalMigration {
            name: "remove-filesize".to_string(),
            threshold: chrono::Utc.ymd(2020, 9, 28).and_hms(0, 0, 0),
            filter: doc! {"size": doc!{"$exists": true}},
            set: None,
            unset: Some(doc! {"size": ""}),
        })]
    }
}

#[derive(Clone)]
pub struct Database {
    pub db: wither::mongodb::Database,
}

impl Database {
    pub async fn new() -> Result<Self> {
        info!("Connecting to database");
        let mut options = ClientOptions::default();
        options.app_name = Some("odcrawler-discovery".to_string());
        let db = Client::with_options(options)?.database("odcrawler-discovery");

        // Disabled for this version of wither
        // OpenDirectory::sync(&client).await?;
        // Link::sync(&client).await?;
        Link::migrate(&db).await?;

        Ok(Self { db })
    }

    pub async fn get_links(&mut self, opendirectory: &str) -> Result<ModelCursor<Link>> {
        Ok(Model::find(&self.db, doc! {"url": opendirectory}, None).await?)
    }

    pub async fn save_scan_result(
        &mut self,
        scan_result: &ODScanResult,
        files: &[&ODScanFile],
    ) -> Result<()> {
        info!("Saving results");
        let document = doc! {"url": scan_result.root.url.clone()};

        if OpenDirectory::find_one(&self.db, document, None)
            .await?
            .is_some()
        {
            OpenDirectory {
                id: None,
                url: scan_result.root.url.clone(),
            }
            .save(&self.db, None)
            .await?
        };

        for chunk in files.chunks(1000) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(|f| doc! {"url": f.url.clone(), "opendirectory": scan_result.root.url.clone()})
                .collect();
            Link::collection(&self.db).insert_many(docs, None).await?;
        }

        Ok(())
    }
}
