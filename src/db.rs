use crate::scans::ODScanFile;
use anyhow::Result;
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Link {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub id: Option<ObjectId>,
    pub opendirectory: String,
    pub url: String,
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
            Box::new(wither::IntervalMigration {
                name: "remove-times-unreachable".to_string(),
                threshold: chrono::Utc.ymd(2020, 10, 10).and_hms(0, 0, 0),
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
        options.app_name = Some("odcrawler-discovery-copy".to_string());
        let db = Client::with_options(options)?.database("odcrawler-discovery-copy");

        // Disabled for this version of wither
        // OpenDirectory::sync(&client).await?;
        // Link::sync(&client).await?;
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

    pub async fn save_scan_result(&mut self, root_url: &str, files: &[ODScanFile]) -> Result<()> {
        info!("Saving results");
        let document = doc! {"url": root_url.to_string()};

        if OpenDirectory::find_one(&self.db, document, None)
            .await?
            .is_none()
        {
            OpenDirectory {
                id: None,
                url: root_url.to_string(),
                unreachable: 0,
            }
            .save(&self.db, None)
            .await?
        };

        for chunk in files.chunks(1000) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(|f| doc! {"url": f.url.clone(), "opendirectory": root_url.to_string(), "unreachable": 0})
                .collect();
            Link::collection(&self.db).insert_many(docs, None).await?;
        }

        Ok(())
    }
}
