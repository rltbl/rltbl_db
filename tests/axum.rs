use axum::{
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use sql_json::{core::DbConnection, sqlite::SqliteConnection};
use std::sync::Arc;
use tower_service::Service;

#[cfg(feature = "postgres")]
use sql_json::postgres::PostgresConnection;

async fn get_root(State(conn): State<Arc<impl DbConnection>>) -> impl IntoResponse {
    let value = conn
        .query_value("SELECT value FROM test LIMIT 1", &[])
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    Html(value)
}

/// Test using axum with sql_json.
#[tokio::test]
async fn test_axum_sqlite() {
    let conn = SqliteConnection::connect("test_axum.db").await.unwrap();
    conn.execute("DROP TABLE IF EXISTS test", &[])
        .await
        .unwrap();
    conn.execute("CREATE TABLE test ( value TEXT )", &[])
        .await
        .unwrap();
    conn.execute("INSERT INTO test VALUES ('foo')", &[])
        .await
        .unwrap();

    let state = Arc::new(conn);
    let mut router: Router = Router::new().route("/", get(get_root)).with_state(state);

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/")
        .body(String::new())
        .unwrap();
    let response = router.call(request).await.unwrap();

    let (_, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("Read from response body");
    let result = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!("foo", result);
}

#[tokio::test]
#[cfg(feature = "postgres")]
async fn test_axum_postgres() {
    let client = PostgresConnection::connect("postgresql:///sql_json_db")
        .await
        .unwrap();
    client
        .execute("DROP TABLE IF EXISTS test", &[])
        .await
        .unwrap();
    client
        .execute("CREATE TABLE test ( value TEXT )", &[])
        .await
        .unwrap();
    client
        .execute("INSERT INTO test VALUES ('foo')", &[])
        .await
        .unwrap();

    let state = Arc::new(client);
    let mut router: Router = Router::new().route("/", get(get_root)).with_state(state);

    let request = axum::http::Request::builder()
        .method("GET")
        .uri("/")
        .body(String::new())
        .unwrap();
    let response = router.call(request).await.unwrap();

    let (_, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("Read from response body");
    let result = String::from_utf8(bytes.to_vec()).unwrap();
    assert_eq!("foo", result);
}
