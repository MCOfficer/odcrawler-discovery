use crate::db::Database;
use crate::db::OpenDirectory;
use crate::{meili, Opt};
use anyhow::Result;
use futures::StreamExt;
use std::time::Duration;
use wither::Model;

pub async fn check_opendirectories(opt: &Opt, db: &mut Database) -> Result<()> {
    info!("Checking ODs concurrently");
    db.get_opendirectories()
        .await?
        .filter_map(|res| async { res.ok() })
        .for_each_concurrent(8, |od| async {
            if let Err(e) = check_opendirectory(&opt, &db, od).await {
                error!("Error checking OD: {}", e);
            };
        })
        .await;
    Ok(())
}
pub async fn check_opendirectory(opt: &Opt, db: &Database, mut od: OpenDirectory) -> Result<()> {
    info!("Checking {}", &od.url);
    let is_reachable = link_is_reachable(&od.url, 15);

    if !is_reachable {
        info!("{} is unreachable", &od.url);
        od.unreachable = od.unreachable.saturating_add(1);
        od.save(&db.db, None).await?;

        if od.unreachable >= 5 {
            remove_od_links(opt, db, &od).await?;
        }
    } else {
        info!("{} is reachable", &od.url);
        od.unreachable = 0;
        od.save(&db.db, None).await?;
    }
    Ok(())
}

async fn remove_od_links(opt: &Opt, db: &Database, od: &OpenDirectory) -> Result<()> {
    info!("Removing links for OD {} from Meilisearch", od.url);
    db.get_links(&od.url)
        .await?
        .filter_map(|l| async { Some(l.ok()?.id?.to_string()) })
        .chunks(1000)
        .for_each(|chunk| async move {
            if let Err(e) = meili::remove_links(opt, chunk).await {
                warn!("Failed to remove chunk from Meilisearch: {}", e)
            }
        })
        .await;
    Ok(())
}

fn link_is_reachable(link: &str, timeout_src: u64) -> bool {
    debug!("Checking {}", link);
    let res = ureq::head(link)
        .timeout(Duration::from_secs(timeout_src))
        .call();
    res.ok() || res.redirect() // TODO: Check hashhackers 404
}
