use crate::scans::ODScanFile;
use anyhow::Result;
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use wither::bson::{doc, oid::ObjectId, Document};
use wither::mongodb::options::ClientOptions;
use wither::mongodb::*;
use wither::prelude::*;
use wither::{Model, ModelCursor};

#[derive(Debug, Model, Serialize, Deserialize)]
#[model(
    collection_name = "opendirectories",
    index(keys = r#"doc!{"url": 1}"#, options = r#"doc!{"unique": true}"#),
    index(keys = r#"doc!{"unreachable": 1}"#)
)]
pub struct OpenDirectory {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub url: String,
    pub unreachable: i32,
}

impl Migrating for OpenDirectory {
    fn migrations() -> Vec<Box<dyn wither::Migration>> {
        vec![
            Box::new(wither::IntervalMigration {
                name: "add-times-unreachable".to_string(),
                threshold: chrono::Utc.ymd(2020, 9, 30).and_hms(0, 0, 0),
                filter: doc! {"unreachable": doc!{"$exists": false}},
                set: Some(doc! {"unreachable": 0}),
                unset: None,
            }),
            Box::new(wither::IntervalMigration {
                name: "cap-times-unreachable".to_string(),
                threshold: chrono::Utc.ymd(2020, 11, 20).and_hms(0, 0, 0),
                filter: doc! {"unreachable": doc!{"$gt": 10}},
                set: Some(doc! {"unreachable": 10}),
                unset: None,
            }),
        ]
    }
}

#[derive(Debug, Model, Serialize, Deserialize)]
#[model(
    index(keys = r#"doc!{"url": 1}"#, options = r#"doc!{"unique": true}"#),
    index(keys = r#"doc!{"opendirectory": 1}"#)
)]
pub struct Link {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub opendirectory: String,
    pub url: String,
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
            Box::new(wither::IntervalMigration {
                name: "remove-times-unreachable".to_string(),
                threshold: chrono::Utc.ymd(2021, 3, 1).and_hms(0, 0, 0),
                filter: doc! {"unreachable": doc!{"$exists": true}},
                set: None,
                unset: Some(doc! {"unreachable": ""}),
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

        OpenDirectory::sync(&db).await?;
        Link::sync(&db).await?;
        OpenDirectory::migrate(&db).await?;
        Link::migrate(&db).await?;

        Ok(Self { db })
    }

    pub async fn get_opendirectories(&self, dead_ods: bool) -> Result<ModelCursor<OpenDirectory>> {
        let doc = if dead_ods {
            doc! {}
        } else {
            doc! { "unreachable": doc! { "$lt": crate::check_links::DEAD_OD_THRESHOLD} }
        };
        Ok(OpenDirectory::find(&self.db, doc, None).await?)
    }

    pub async fn get_links(&self, opendirectory: &str) -> Result<ModelCursor<Link>> {
        Ok(Link::find(&self.db, doc! {"opendirectory": opendirectory}, None).await?)
    }

    pub async fn save_scan_result(
        &mut self,
        root_url: &str,
        mut files: Vec<ODScanFile>,
        is_reachable: bool,
    ) -> Result<()> {
        info!("Saving results");
        // TODO: Use a transaction when the driver supports them

        OpenDirectory {
            id: None,
            url: root_url.to_string(),
            unreachable: if is_reachable { 0 } else { 10 },
        }
        .save(&self.db, None)
        .await?;

        let links: Vec<Document> = files
            .drain(..)
            .map(move |f| Link {
                id: None,
                url: f.url,
                opendirectory: root_url.to_string(),
            })
            .map(|l| l.document_from_instance().unwrap())
            .collect();

        Link::collection(&self.db).insert_many(links, None).await?;

        Ok(())
    }
}
