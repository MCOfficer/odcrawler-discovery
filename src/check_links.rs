use crate::db::Database;
use crate::db::OpenDirectory;
use crate::{elastic, Opt};
use anyhow::Result;
use futures::StreamExt;
use isahc::config::SslOption;
use isahc::prelude::{Configurable, Request, RequestExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use wither::Model;

pub const DEAD_OD_THRESHOLD: i32 = 10;

pub async fn check_opendirectories(opt: &Opt, db: &mut Database) -> Result<()> {
    let ods: Mutex<Vec<(OpenDirectory, bool)>> = Mutex::new(vec![]);

    info!("Checking ODs concurrently");

    let total = OpenDirectory::collection(&db.db)
        .estimated_document_count(None)
        .await?;
    let count = AtomicUsize::new(0);

    db.get_opendirectories(true)
        .await?
        .filter_map(|res| async { res.ok() })
        .for_each_concurrent(128, |od| async {
            let reachable = link_is_reachable(&od.url, Duration::from_secs(20), false).await;
            match ods.lock() {
                Ok(mut ods) => {
                    ods.push((od, reachable));
                }
                Err(_) => {
                    error!("Poisoned Mutex, something went wrong in a different closure");
                }
            }

            let current = count.fetch_add(1, Ordering::Relaxed) + 1;
            if current % 100 == 0 {
                info!("Checked {}/{} links", current, total);
            }
        })
        .await;

    info!("Persisting results");
    // There are no other users of this mutex now
    for (od, reachable) in ods.into_inner().unwrap() {
        if let Err(e) = persists_checked_opendirectory(&opt, &db, od, reachable).await {
            error!("Error saving OD to DB: {}", e);
        };
    }

    Ok(())
}

pub async fn persists_checked_opendirectory(
    opt: &Opt,
    db: &Database,
    mut od: OpenDirectory,
    reachable: bool,
) -> Result<()> {
    if reachable {
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

pub async fn link_is_reachable(link: &str, timeout: Duration, log_status: bool) -> bool {
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
        if log_status {
            info!("Got {} for {}", response.status(), link);
        }
        return response.status().is_success() || response.status().is_redirection();
    }
    // let isahc log any issues
    false
}
