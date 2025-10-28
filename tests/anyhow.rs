use anyhow::Result;
use sql_json::{any::AnyConnection, core::DbQuery};

#[tokio::test]
async fn test_anyhow() {
    let conn = AnyConnection::connect(":memory:").await.unwrap();
    let value = query_u64(conn).await.expect("1");
    assert_eq!(value, 1);
}

async fn query_u64(conn: AnyConnection) -> Result<u64> {
    Ok(conn.query_u64("SELECT 1", &[]).await?)
}
