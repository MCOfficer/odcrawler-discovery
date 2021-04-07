use rocket::futures::StreamExt;
use rocket::{get, routes, Rocket, State};
use rocket_contrib::json::Json;
use rocket_contrib::templates::Template;
use shared::db;
use shared::db::Stats as DbStats;

#[derive(serde::Serialize)]
struct Stats {
    db: DbStats,
    load_one: f64,
    load_five: f64,
    load_fifteen: f64,
    mem_total: usize,
    mem_available: usize,
}

#[get("/json")]
async fn stats_json(db: State<'_, db::Database>) -> Json<Stats> {
    let load = mprober_lib::load_average::get_load_average().unwrap();
    let mem = mprober_lib::memory::free().unwrap().mem;
    let stats = Stats {
        db: db.stats().await.unwrap(),
        load_one: load.one,
        load_five: load.five,
        load_fifteen: load.fifteen,
        mem_total: mem.total,
        mem_available: mem.available,
    };
    Json(stats)
}

#[get("/")]
async fn stats(db: State<'_, db::Database>) -> Template {
    let stats = stats_json(db).await.into_inner();
    Template::render("index", &stats)
}

#[derive(serde::Serialize)]
struct OD {
    url: String,
    dead: bool,
}

#[derive(serde::Serialize)]
struct ODs {
    ods: Vec<OD>,
}

#[get("/ods/json")]
async fn ods_json(db: State<'_, db::Database>) -> Json<ODs> {
    let ods = db
        .get_opendirectories(true)
        .await
        .unwrap()
        .filter_map(|r| async { r.ok() })
        .map(|od| OD {
            url: od.url,
            dead: od.unreachable >= shared::DEAD_OD_THRESHOLD,
        })
        .collect()
        .await;
    Json(ODs { ods })
}

#[get("/ods")]
async fn ods(db: State<'_, db::Database>) -> Template {
    let ods = ods_json(db).await.into_inner().ods;
    Template::render("ods", &ODs { ods })
}

#[derive(serde::Serialize)]
struct Links {
    links: Vec<String>,
}

#[get("/od/json?<url>")]
async fn links_json(db: State<'_, db::Database>, url: &str) -> Json<Links> {
    let links = db
        .get_links(&url)
        .await
        .unwrap()
        .filter_map(|r| async { r.ok() })
        .map(|l| l.url)
        .collect()
        .await;
    Json(Links { links })
}

#[get("/od?<url>")]
async fn links(db: State<'_, db::Database>, url: &str) -> Template {
    let links = links_json(db, url).await.into_inner();
    Template::render("links", &links)
}

#[rocket::launch]
async fn launch() -> Rocket {
    rocket::ignite()
        .mount(
            "/",
            routes![stats_json, stats, ods_json, ods, links_json, links],
        )
        .attach(Template::fairing())
        .manage(db::Database::new().await.unwrap())
}
