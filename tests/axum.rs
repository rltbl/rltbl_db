use sql_json::{core::DbConnection, sqlite::SqliteConnection};
use std::sync::Arc;

use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use tower_service::Service;

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
async fn test_axum() {
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
    println!("REQUEST {request:?}");
    let response = router.call(request).await.unwrap();
    println!("RESPONSE {response:?}");

    let (_, body) = response.into_parts();
    let bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .expect("Read from response body");
    let result = String::from_utf8(bytes.to_vec()).unwrap();
    println!("RESULT {result}");
    assert_eq!("foo", result);
}
