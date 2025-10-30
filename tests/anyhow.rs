use anyhow::Result;
use rltbl_db::{any::AnyPool, core::DbQuery};

#[tokio::test]
async fn test_anyhow() {
    let conn = AnyPool::connect(":memory:").await.unwrap();
    let value = query_u64(conn).await.expect("1");
    assert_eq!(value, 1);
}

async fn query_u64(conn: AnyPool) -> Result<u64> {
    Ok(conn.query_u64("SELECT 1", &[]).await?)
}
