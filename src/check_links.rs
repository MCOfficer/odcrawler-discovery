use crate::db::OpenDirectory;
use crate::db::{Database, Link};
use crate::{meili, Opt};
use anyhow::Result;
use async_std::prelude::*;
use rayon::prelude::*;
use std::time::Duration;
use wither::Model;

pub async fn check_opendirectory(opt: &Opt, db: &mut Database) -> Result<()> {
    let mut od = db.get_random_opendirectory().await?;
    info!("Checking {}", &od.url);
    let is_reachable = link_is_reachable(&od.url, 15);

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
            .into_par_iter()
            .update(|mut l| {
                if link_is_reachable(&l.url, 15) {
                    l.unreachable = 0;
                } else {
                    l.unreachable = l.unreachable.saturating_add(1);
                };
            })
            .collect();

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

fn link_is_reachable(link: &str, timeout_src: u64) -> bool {
    debug!("Checking {}", link);
    let res = ureq::head(link)
        .timeout(Duration::from_secs(timeout_src))
        .call();
    res.ok() || res.redirect() // TODO: Check hashhackers 404
}
