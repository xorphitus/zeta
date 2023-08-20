use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use futures::TryStreamExt;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

async fn initialize_database(db_pool: &SqlitePool) -> Result<Option<()>, sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS wal (
             project_id TEXT NOT NULL,
             time       DATETIME NOT NULL,
             created_at DATETIME NOT NULL,
             payload    TEXT NOT NULL
         )"
    ).execute(db_pool).await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_created_at ON wal (created_at)"
    ).execute(db_pool).await?;

    return Ok(Some(()))
}

async fn save_to_db(db_pool: &SqlitePool, project_id: String, payload: String) -> Result<Option<()>, sqlx::Error> {
    let timestamp = chrono::Utc::now().to_rfc3339();
    sqlx::query("INSERT INTO wal (project_id, time, created_at, payload) VALUES (?1, ?2, ?3, ?4)")
        .bind(project_id)
        .bind(&timestamp)
        .bind(&timestamp)
        .bind(payload)
        .execute(db_pool).await?;

    return Ok(Some(()))
}

async fn dump_select_results(q :&str, pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut rows = sqlx::query(q).fetch(pool);

    while let Some(row) = rows.try_next().await? {
        let id: String = row.try_get("project_id")?;
        let p: String = row.try_get("payload")?;
        println!("ID: {} {}", id, p);
    }
    Ok(())
}


async fn get_project_data(
    path: web::Path<String>,
    query: web::Query<std::collections::HashMap<String, String>>,
    db_pool: web::Data<SqlitePool>,
) -> impl Responder {
    let id = path.into_inner();
    let q = query.get("q").cloned().unwrap_or_default();

    match dump_select_results(&q, &**db_pool).await {
        Ok(_) => {}
        Err(e) => {
            log::error!("query error: {}", e);
        }
    }
    HttpResponse::Ok().body(id)
}

async fn post_project_data(
    path: web::Path<String>,
    body: web::Bytes,
    db_pool: web::Data<SqlitePool>,
) -> impl Responder {
    let id = path.into_inner();
    let data = String::from_utf8(body.to_vec()).unwrap_or_default();

    let result  = save_to_db(&**db_pool, id, data).await;
    match result {
        Ok(_) => {
            HttpResponse::Created().finish()
        },
        Err(e) => {
            log::error!("{}", e);
            HttpResponse::InternalServerError().body("Failed to persist a write request")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init();

    let pool = SqlitePool::connect("sqlite::memory:").await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("Database connection error: {}", e))
    })?;

    initialize_database(&pool).await.map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, format!("Database initialization error: {}", e))
    })?;

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .route("/project/{id}/data", web::get().to(get_project_data))
            .route("/project/{id}/data", web::post().to(post_project_data))
    })
    .bind("127.0.0.1:8000")?
    .run()
    .await
}
