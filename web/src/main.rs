use rocket::futures::StreamExt;
use rocket::{get, routes, Rocket};
use rocket_contrib::json::Json;
use rocket_contrib::templates::Template;
use shared::db;
use shared::db::Stats;

#[get("/stats.json")]
async fn stats_json() -> Json<Stats> {
    let db = db::Database::new().await.unwrap();
    Json(db.stats().await.unwrap())
}

#[get("/")]
async fn stats() -> Template {
    let stats = stats_json().await.into_inner();
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

#[get("/ods.json")]
async fn ods_json() -> Json<ODs> {
    let db = db::Database::new().await.unwrap();
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
async fn ods() -> Template {
    let ods = ods_json().await.into_inner().ods;
    Template::render("ods", &ODs { ods })
}

#[rocket::launch]
fn launch() -> Rocket {
    rocket::ignite()
        .mount("/", routes![stats_json, stats, ods_json, ods])
        .attach(Template::fairing())
}
