use crate::db::Database;
use crate::db::OpenDirectory;
use crate::{elastic, Opt};
use anyhow::Result;
use futures::StreamExt;
use isahc::config::SslOption;
use isahc::prelude::{Configurable, Request, RequestExt};
use std::time::Duration;
use wither::Model;

pub const DEAD_OD_THRESHOLD: i32 = 10;

pub async fn check_opendirectories(opt: &Opt, db: &mut Database) -> Result<()> {
    info!("Checking ODs concurrently");
    db.get_opendirectories(true)
        .await?
        .filter_map(|res| async { res.ok() })
        .for_each_concurrent(128, |od| async {
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
        // Re-add links if it was dead
        if od.unreachable >= DEAD_OD_THRESHOLD {
            elastic::add_links_from_db(opt, db, &od.url).await?;
        }
        // Reset to 0 regardless
        if od.unreachable > 0 {
            od.unreachable = 0;
            od.save(&db.db, None).await?;
        }
    } else {
        // Remove links only if it was alive
        if od.unreachable + 1 == DEAD_OD_THRESHOLD {
            remove_od_links(opt, db, &od).await?;
        }
        // Increment if it's below the threshold
        if od.unreachable < DEAD_OD_THRESHOLD {
            od.unreachable = od.unreachable.saturating_add(1);
            od.save(&db.db, None).await?;
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

pub async fn link_is_reachable(link: &str, timeout: Duration) -> bool {
    // Patch for some non-comformant URLs
    let link = link.replace(" ", "%20");

    let mut builder = Request::head(&link)
        .connect_timeout(timeout)
        .timeout(timeout)
        .ssl_options(SslOption::DANGER_ACCEPT_INVALID_CERTS);

    // Workaround for hashhacker's "AI protection"
    if link.contains("driveindex.ga") {
        builder = builder
            .uri(link.replace("driveindex.ga", "hashhackers.com"))
            .header("Referer", &link);
    }

    let request = match builder.body(()) {
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
