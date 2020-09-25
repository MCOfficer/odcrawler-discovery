use crate::db::Database;
use anyhow::Result;
use async_std::prelude::*;
use async_std::stream::IntoStream;
use parallel_stream::prelude::*;
use surf::Client;
use wither::Model;

pub async fn check_opendirectory(db: &mut Database) -> Result<()> {
    let client = surf::client();
    let mut od = db.get_random_opendirectory().await?;
    let is_reachable = link_is_reachable(&client, &od.url).await;

    if !is_reachable {
        od.unreachable = od.unreachable.saturating_add(1);
        od.save(&db.db, None);
    } else {
        od.unreachable = 0;
        od.save(&db.db, None);

        let links = db.get_links(&od.url).await?;
        // TODO: Iterate over links and process them.
        /*
           if !link_is_reachable(&client, &l.url).await {
               l.unreachable = l.unreachable.saturating_add(1);
           } else {
               l.unreachable = 0;
           };
           l.save(&db.db, None);
        */
    }
    Ok(())
}

pub async fn link_is_reachable(client: &Client, link: &str) -> bool {
    match client.head(link).send().await {
        Ok(response) => response.status().is_success(), // TODO: Check for hashhackers 404s
        Err(_) => false,
    }
}
