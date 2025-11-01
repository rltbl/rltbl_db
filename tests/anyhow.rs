use anyhow::Result;
use rltbl_db::{any::AnyPool, core::DbQuery};

#[tokio::test]
async fn test_anyhow() {
    let pool = AnyPool::connect(":memory:").await.unwrap();
    let value = query_u64(pool).await.expect("1");
    assert_eq!(value, 1);
}

async fn query_u64(pool: AnyPool) -> Result<u64> {
    Ok(pool.query_u64("SELECT 1", ()).await?)
}
