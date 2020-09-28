use crate::db::OpenDirectory;
use crate::db::{Database, Link};
use crate::{meili, Opt};
use anyhow::Result;
use async_std::prelude::*;
use parallel_stream::prelude::*;
use wither::Model;

pub async fn check_opendirectory(opt: &Opt, db: &mut Database) -> Result<()> {
    let mut od = db.get_random_opendirectory().await?;
    info!("Checking {}", &od.url);
    let is_reachable = link_is_reachable(&od.url, 15).await;

    if !is_reachable {
        info!("{} is unreachable", &od.url);
        od.unreachable = od.unreachable.saturating_add(1);
        od.save(&db.db, None).await?;

        if od.unreachable >= 5 {
            remove_dead_links(opt, db, &od).await?;
        }
    } else {
        info!("{} is reachable, proceeding", &od.url);
        od.unreachable = 0;
        od.save(&db.db, None).await?;

        let mut links: Vec<Link> = db
            .get_links(&od.url)
            .await?
            .filter_map(|l| l.ok())
            .collect()
            .await;

        info!("Checking {} links", links.len());
        links = links
            .into_par_stream() // TODO: Fancy tree-structure scanning
            .limit(8)
            .map(move |mut l| async move {
                if link_is_reachable(&l.url, 15).await {
                    l.unreachable = 0;
                } else {
                    l.unreachable = l.unreachable.saturating_add(1);
                };
                l
            })
            .collect()
            .await;

        for mut link in links {
            link.save(&db.db, None).await?;
        }

        remove_dead_links(opt, db, &od).await?;
    }
    Ok(())
}

async fn remove_dead_links(opt: &Opt, db: &mut Database, od: &OpenDirectory) -> Result<()> {
    let links: Vec<Link> = db
        .get_links(&od.url)
        .await?
        .filter_map(|l| l.ok())
        .collect()
        .await;
    let to_remove: Vec<String> = links
        .iter()
        .filter(|l| l.unreachable >= 5 || od.unreachable >= 5)
        .filter_map(|l| l.id.clone())
        .map(|oid| oid.to_string())
        .collect();
    meili::remove_links(opt, to_remove).await?;
    Ok(())
}

pub async fn link_is_reachable(link: &str, timeout_src: u64) -> bool {
    debug!("Checking {}", link);
    let request_fut = surf::head(link).send();
    let timeout = std::time::Duration::from_secs(timeout_src);
    if let Ok(res) = async_std::future::timeout(timeout, request_fut).await {
        if let Ok(response) = res {
            return response.status().is_success(); // TODO: Check for hashhackers 404s
        }
    }
    false
}
