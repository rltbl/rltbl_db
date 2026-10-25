use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse},
    routing::get,
};
use rltbl_db::{AnyPool, row};
use std::sync::Arc;
use tower_service::Service;

async fn get_root(State(pool): State<Arc<AnyPool>>) -> impl IntoResponse {
    let value = pool
        .query("SELECT value FROM test LIMIT 1", ())
        .await
        .unwrap();
    let value: String = value.try_into_value::<String>().unwrap();
    Html(value)
}

async fn run_axum(url: &str) {
    let pool = AnyPool::connect(url).await.unwrap();
    let kind = pool.syntax().name();
    let cascade = match kind.to_string().as_str() {
        "sqlite" => "",
        "postgres" => " CASCADE",
        _ => panic!("Invalid kind: '{}'", pool.syntax().name()),
    };
    pool.execute_batch(&format!(
        "DROP TABLE IF EXISTS test{cascade};\
         CREATE TABLE test ( value TEXT )",
    ))
    .await
    .unwrap();

    pool.insert(
        "test",
        &["value"],
        &[row! {
            "value" => "foo",
        }],
    )
    .await
    .unwrap();

    pool.cache("SELECT 1 FROM test", ()).await.unwrap();
    pool.cache("SELECT 1 FROM test", ()).await.unwrap();

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
    #[cfg(feature = "libsql")]
    run_axum(":memory:").await;
}
