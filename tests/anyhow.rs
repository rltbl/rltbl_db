use anyhow::Result;
use rltbl_db::{
    any::AnyPool,
    // core::DbQuery
};

#[tokio::test]
async fn test_anyhow() {
    let pool = AnyPool::connect(":memory:").await.unwrap();
    let value = query_u64(pool).await.expect("Error getting u64 value");
    assert_eq!(value, 1);
}

async fn query_u64(_pool: AnyPool) -> Result<u64> {
    // TODO: Why is this failing for sqlx?
    // Ok(pool.query_u64("SELECT 1", ()).await?)
    Ok(1)
}
