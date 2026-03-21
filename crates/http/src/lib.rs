use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use potatodb_engine::{PotatoDB, QueryResult};

#[derive(Clone)]
struct AppState {
    db: Arc<RwLock<PotatoDB>>,
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
}

#[derive(Serialize)]
struct QueryResponse {
    kind: String,
    message: Option<String>,
    display: Option<String>,
    rows: usize,
}

#[derive(Serialize)]
struct TableStats {
    table: String,
    parquet_files: usize,
    total_bytes: u64,
    oldest_file_age_secs: u64,
}

/// Starts the HTTP API server.
///
/// # Errors
///
/// Returns an error if binding to the address or serving fails.
pub async fn start_http(
    db: Arc<RwLock<PotatoDB>>,
    bind_addr: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = build_router(db);
    let listener = TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router(db: Arc<RwLock<PotatoDB>>) -> Router {
    let state = AppState { db };
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/tables", get(list_tables))
        .route("/tables/{name}/stats", get(table_stats))
        .route("/query", post(run_query))
        .route("/ws", get(ws_handler))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let db = state.db.read().await;
    let tables_total = db.table_names().len();
    let indexes_total = db.indexes().len();
    let views_total = db.view_names().len();
    let sequences_total = db.sequence_names().len();
    let functions_total = db.function_names().len();
    let users_total = db.user_info().len();

    drop(db);

    let body = format!(
        r"# HELP potatodb_tables_total Number of tables
# TYPE potatodb_tables_total gauge
potatodb_tables_total {tables_total}
# HELP potatodb_indexes_total Number of indexes
# TYPE potatodb_indexes_total gauge
potatodb_indexes_total {indexes_total}
# HELP potatodb_views_total Number of views
# TYPE potatodb_views_total gauge
potatodb_views_total {views_total}
# HELP potatodb_sequences_total Number of sequences
# TYPE potatodb_sequences_total gauge
potatodb_sequences_total {sequences_total}
# HELP potatodb_functions_total Number of user-defined functions
# TYPE potatodb_functions_total gauge
potatodb_functions_total {functions_total}
# HELP potatodb_users_total Number of users
# TYPE potatodb_users_total gauge
potatodb_users_total {users_total}
"
    );

    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; version=0.0.4"),
        )],
        body,
    )
}

async fn list_tables(State(state): State<AppState>) -> Json<Vec<String>> {
    let db = state.db.read().await;
    Json(db.table_names())
}

async fn table_stats(Path(name): Path<String>, State(state): State<AppState>) -> Json<TableStats> {
    let db = state.db.read().await;
    let parquet_files = db.parquet_file_count(&name).await.unwrap_or(0);
    let total_bytes = db.table_total_bytes(&name).await.unwrap_or(0);
    let oldest_file_age_secs = db.table_oldest_file_age_secs(&name).await.unwrap_or(0);
    drop(db);
    Json(TableStats {
        table: name,
        parquet_files,
        total_bytes,
        oldest_file_age_secs,
    })
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: AppState) {
    // Send a welcome message
    let _ = socket
        .send(Message::Text(
            r#"{"type":"connected","message":"PotatoDB WebSocket"}"#.into(),
        ))
        .await;

    // Poll for CDC events every second
    loop {
        tokio::select! {
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Handle subscribe commands like {"subscribe": "cdc", "table": "users"}
                        let _ = socket
                            .send(Message::Text(
                                format!(r#"{{"type":"ack","message":"received: {text}"}}"#).into(),
                            ))
                            .await;
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                // Poll CDC events
                let db = state.db.read().await;
                drop(db);
                // For now just send a heartbeat
                let _ = socket
                    .send(Message::Text(r#"{"type":"heartbeat"}"#.into()))
                    .await;
            }
        }
    }
}

async fn run_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let mut db = state.db.write().await;
    match db.execute(&req.sql).await {
        Ok(QueryResult::Message(msg)) => Json(QueryResponse {
            kind: "message".to_string(),
            message: Some(msg),
            display: None,
            rows: 0,
        }),
        Ok(QueryResult::Records(batches)) => {
            let rows = batches
                .iter()
                .map(arrow::array::RecordBatch::num_rows)
                .sum();
            Json(QueryResponse {
                kind: "records".to_string(),
                message: None,
                display: Some(potatodb_display::format_batches(&batches)),
                rows,
            })
        }
        Err(err) => Json(QueryResponse {
            kind: "error".to_string(),
            message: Some(err.to_string()),
            display: None,
            rows: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::util::ServiceExt;

    async fn setup() -> (Arc<RwLock<PotatoDB>>, Router, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let db = PotatoDB::new(tmp.path().to_string_lossy().to_string(), None)
            .await
            .unwrap();
        let db = Arc::new(RwLock::new(db));
        let app = build_router(db.clone());
        (db, app, tmp)
    }

    #[tokio::test]
    async fn test_http_routes_health_tables_and_query() {
        let (db, app, _tmp) = setup().await;

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        {
            let mut db = db.write().await;
            db.execute("CREATE TABLE http_t (id INT);").await.unwrap();
            db.execute("INSERT INTO http_t VALUES (1), (2);")
                .await
                .unwrap();
        }

        let tables = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/tables")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tables.status(), StatusCode::OK);

        let query = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        "{\"sql\":\"SELECT COUNT(*) AS c FROM http_t;\"}",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(query.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_metrics_endpoint_returns_prometheus_format() {
        let (db, app, _tmp) = setup().await;

        {
            let mut db = db.write().await;
            db.execute("CREATE TABLE metrics_t1 (id INT);")
                .await
                .unwrap();
            db.execute("CREATE TABLE metrics_t2 (id INT);")
                .await
                .unwrap();
            db.execute("CREATE VIEW metrics_v AS SELECT * FROM metrics_t1;")
                .await
                .unwrap();
            db.execute("CREATE SEQUENCE metrics_seq;").await.unwrap();
        }

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            text.contains("potatodb_tables_total"),
            "should contain tables metric"
        );
        assert!(
            text.contains("potatodb_indexes_total"),
            "should contain indexes metric"
        );
        assert!(
            text.contains("potatodb_views_total"),
            "should contain views metric"
        );
        assert!(
            text.contains("potatodb_sequences_total"),
            "should contain sequences metric"
        );
        assert!(
            text.contains("potatodb_functions_total"),
            "should contain functions metric"
        );
        assert!(
            text.contains("potatodb_users_total"),
            "should contain users metric"
        );

        assert!(text.contains("# HELP"), "should contain HELP comments");
        assert!(text.contains("# TYPE"), "should contain TYPE comments");
    }

    #[tokio::test]
    async fn test_metrics_counts_are_accurate() {
        let (db, app, _tmp) = setup().await;

        {
            let mut db = db.write().await;
            db.execute("CREATE TABLE m_t1 (id INT);").await.unwrap();
            db.execute("INSERT INTO m_t1 VALUES (1);").await.unwrap();
            db.execute("CREATE INDEX m_idx ON m_t1 (id);")
                .await
                .unwrap();
        }

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();

        for line in text.lines() {
            if line.starts_with("potatodb_tables_total") {
                let val: usize = line.split_whitespace().last().unwrap().parse().unwrap();
                assert!(val >= 1, "should have at least 1 table");
            }
            if line.starts_with("potatodb_indexes_total") {
                let val: usize = line.split_whitespace().last().unwrap().parse().unwrap();
                assert!(val >= 1, "should have at least 1 index");
            }
        }
    }

    #[tokio::test]
    async fn test_query_endpoint_error_handling() {
        let (_db, app, _tmp) = setup().await;

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/query")
                    .header("content-type", "application/json")
                    .body(Body::from("{\"sql\":\"SELECT * FROM nonexistent_table;\"}"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "error");
    }

    #[tokio::test]
    async fn test_table_stats_endpoint() {
        let (db, app, _tmp) = setup().await;

        {
            let mut db = db.write().await;
            db.execute("CREATE TABLE stats_t (id INT);").await.unwrap();
            db.execute("INSERT INTO stats_t VALUES (1);").await.unwrap();
        }

        let resp = app
            .oneshot(
                Request::builder()
                    .method(Method::GET)
                    .uri("/tables/stats_t/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["table"], "stats_t");
    }
}
