use crate::db::Database;
use crate::db::OpenDirectory;
use crate::{elastic, Opt};
use anyhow::Result;
use futures::StreamExt;
use isahc::prelude::{Configurable, Request, RequestExt};
use std::time::Duration;
use wither::Model;

pub async fn check_opendirectories(opt: &Opt, db: &mut Database) -> Result<()> {
    info!("Checking ODs concurrently");
    db.get_opendirectories(true)
        .await?
        .filter_map(|res| async { res.ok() })
        .for_each_concurrent(32, |od| async {
            if let Err(e) = check_opendirectory(&opt, &db, od).await {
                error!("Error checking OD: {}", e);
            };
        })
        .await;
    Ok(())
}

pub async fn check_opendirectory(opt: &Opt, db: &Database, mut od: OpenDirectory) -> Result<()> {
    let is_reachable = link_is_reachable(&od.url, Duration::from_secs(30));

    if is_reachable.await {
        if od.unreachable > 5 {
            elastic::add_links_from_db(opt, db, &od.url).await?;
        }
        od.unreachable = 0;
        od.save(&db.db, None).await?;
    } else {
        od.unreachable = od.unreachable.saturating_add(1);
        od.save(&db.db, None).await?;

        // Only remove every 5 runs, just to make sure it's gone
        if od.unreachable == 5 || (od.unreachable != 0 && od.unreachable % 50 == 0) {
            remove_od_links(opt, db, &od).await?;
        }
    }
    Ok(())
}

async fn remove_od_links(opt: &Opt, db: &Database, od: &OpenDirectory) -> Result<()> {
    info!("Removing links for OD {} from Elasticsearch", od.url);
    db.get_links(&od.url)
        .await?
        .filter_map(|l| async { Some(l.ok()?.id?.to_string()) })
        .chunks(5_000)
        .for_each(|chunk| async move {
            let chunk = chunk;
            if let Err(e) = elastic::remove_bulk(opt, &chunk) {
                warn!("Failed to remove chunk from Elasticsearch: {}", e)
            }
        })
        .await;
    Ok(())
}

async fn link_is_reachable(link: &str, timeout: Duration) -> bool {
    // Patch for some non-comformant URLs
    let link = link.replace(" ", "%20");

    let mut builder = Request::head(&link)
        .connect_timeout(timeout)
        .timeout(timeout);

    // Workaround for hashhacker's "AI protection"
    if link.contains("driveindex.ga") {
        builder = builder
            .uri(link.replace("driveindex.ga", "hashhackers.com"))
            .header("Referer", &link);
    }

    let request = match builder.body("") {
        Ok(r) => r,
        Err(e) => {
            error!("Error building request for URI '{}': {}", &link, e);
            return false;
        }
    };

    if let Ok(response) = request.send_async().await {
        info!("Got {} for {}", response.status(), link);
        return response.status().is_success() || response.status().is_redirection();
    }
    // let isahc log any issues
    false
}
