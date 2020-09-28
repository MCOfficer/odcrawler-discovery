use crate::scans::{ODScanFile, ODScanResult};
use anyhow::Result;
use async_std::stream::StreamExt;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use wither::bson::{doc, oid::ObjectId, Document};
use wither::mongodb::options::ClientOptions;
use wither::mongodb::*;
use wither::prelude::*;
use wither::ModelCursor;

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDirectory {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub url: String,
    pub unreachable: i32,
}

impl Model for OpenDirectory {
    const COLLECTION_NAME: &'static str = "opendirectories";

    fn id(&self) -> Option<ObjectId> {
        self.id.clone()
    }

    fn set_id(&mut self, id: ObjectId) {
        self.id = Some(id)
    }
}

impl Migrating for OpenDirectory {
    fn migrations() -> Vec<Box<dyn wither::Migration>> {
        vec![Box::new(wither::IntervalMigration {
            name: "add-times-unreachable".to_string(),
            threshold: chrono::Utc.ymd(2020, 9, 30).and_hms(0, 0, 0),
            filter: doc! {"unreachable": doc!{"$exists": false}},
            set: Some(doc! {"unreachable": 0}),
            unset: None,
        })]
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Link {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub opendirectory: String,
    pub url: String,
    pub unreachable: i32,
}

impl Model for Link {
    const COLLECTION_NAME: &'static str = "links";

    fn id(&self) -> Option<ObjectId> {
        self.id.clone()
    }

    fn set_id(&mut self, id: ObjectId) {
        self.id = Some(id)
    }
}

impl Migrating for Link {
    fn migrations() -> Vec<Box<dyn wither::Migration>> {
        vec![
            Box::new(wither::IntervalMigration {
                name: "remove-filesize".to_string(),
                threshold: chrono::Utc.ymd(2020, 9, 30).and_hms(0, 0, 0),
                filter: doc! {"size": doc!{"$exists": true}},
                set: None,
                unset: Some(doc! {"size": ""}),
            }),
            Box::new(wither::IntervalMigration {
                name: "add-times-unreachable".to_string(),
                threshold: chrono::Utc.ymd(2020, 9, 30).and_hms(0, 0, 0),
                filter: doc! {"unreachable": doc!{"$exists": false}},
                set: Some(doc! {"unreachable": 0}),
                unset: None,
            }),
        ]
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
        OpenDirectory::migrate(&db).await?;
        Link::migrate(&db).await?;

        Ok(Self { db })
    }

    pub async fn get_random_opendirectory(&mut self) -> Result<OpenDirectory> {
        let cursor: ModelCursor<OpenDirectory> =
            OpenDirectory::find(&self.db, doc! {}, None).await?;
        let mut ods = cursor
            .filter_map(|x| x.ok())
            .collect::<Vec<OpenDirectory>>()
            .await;
        Ok(ods.remove((rand::random::<f64>() * ods.len() as f64).floor() as usize))
    }

    pub async fn get_links(&mut self, opendirectory: &str) -> Result<ModelCursor<Link>> {
        Ok(Link::find(&self.db, doc! {"opendirectory": opendirectory}, None).await?)
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
            .is_none()
        {
            OpenDirectory {
                id: None,
                url: scan_result.root.url.clone(),
                unreachable: 0,
            }
            .save(&self.db, None)
            .await?
        };

        for chunk in files.chunks(1000) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(|f| doc! {"url": f.url.clone(), "opendirectory": scan_result.root.url.clone(), "unreachable": 0})
                .collect();
            Link::collection(&self.db).insert_many(docs, None).await?;
        }

        Ok(())
    }
}
