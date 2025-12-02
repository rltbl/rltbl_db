use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use rltbl_db::{any::AnyPool, core::DbQuery};
use std::{marker::Sync, sync::Arc};
use tower_service::Service;

async fn get_root(State(pool): State<Arc<impl DbQuery + Sync>>) -> impl IntoResponse {
    let value = pool
        .query_value("SELECT value FROM test LIMIT 1", ())
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    Html(value)
}

async fn run_axum(url: &str) {
    let pool = AnyPool::connect(url).await.unwrap();
    pool.execute_batch(
        "DROP TABLE IF EXISTS test;\
         CREATE TABLE test ( value TEXT );\
         INSERT INTO test VALUES ('foo');",
    )
    .await
    .unwrap();

    let state = Arc::new(pool);
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

/// Test using axum with rltbl_db.
#[tokio::test]
async fn test_axum() {
    #[cfg(feature = "rusqlite")]
    run_axum(":memory:").await;
    #[cfg(feature = "tokio-postgres")]
    run_axum("postgresql:///rltbl_db").await;
}
