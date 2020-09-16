use crate::scans::{ODScanFile, ODScanResult};
use anyhow::Result;
use bson::Document;
use mongodb::bson::doc;
use mongodb::options::ClientOptions;
use mongodb::sync::Database as MongoDatabase;
use mongodb::sync::{Client, Collection};

#[derive(Clone)]
pub struct Database {
    pub mongo_client: Client,
    pub db: MongoDatabase,
    pub links: Collection,
    pub opendirectories: Collection,
}

impl Database {
    pub fn new() -> Result<Self> {
        info!("Connecting to database");
        let mut options = ClientOptions::default();
        options.app_name = Some("odcrawler-discovery".to_string());
        let mongo_client = Client::with_options(options)?;
        let db = mongo_client.database("odcrawler-discovery");
        let links = db.collection("links");
        let opendirectories = db.collection("opendirectories");

        Ok(Self {
            mongo_client,
            db,
            links,
            opendirectories,
        })
    }

    pub fn get_links(
        &mut self,
        opendirectory: &str,
    ) -> std::result::Result<
        impl Iterator<Item = std::result::Result<Document, mongodb::error::Error>>,
        mongodb::error::Error,
    > {
        self.links.find(doc! {"opendirectory": opendirectory}, None)
    }

    pub fn save_scan_result(
        &mut self,
        scan_result: &ODScanResult,
        files: &Vec<&ODScanFile>,
    ) -> Result<()> {
        info!("Saving results");
        let document = doc! {"url": scan_result.root.url.clone()};
        if self
            .opendirectories
            .find_one(document.clone(), None)?
            .is_none()
        {
            self.opendirectories.insert_one(document, None)?;
        }

        for chunk in files.chunks(1000) {
            let docs: Vec<Document> = chunk
                .iter()
                .map(|f| doc! {"url": f.url.clone(), "size": f.file_size, "opendirectory": scan_result.root.url.clone()})
                .collect();
            self.links.insert_many(docs, None)?;
        }

        Ok(())
    }
}
