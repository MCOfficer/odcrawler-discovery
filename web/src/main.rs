use rocket::futures::StreamExt;
use rocket::{get, routes, Rocket};
use rocket_contrib::templates::Template;
use shared::db;

#[get("/")]
async fn index() -> Template {
    let db = db::Database::new().await.unwrap();
    let stats = db.stats().await.unwrap();
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

#[get("/ods")]
async fn ods() -> Template {
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
    Template::render("ods", &ODs { ods })
}

#[rocket::launch]
fn launch() -> Rocket {
    rocket::ignite()
        .mount("/", routes![index, ods])
        .attach(Template::fairing())
}
