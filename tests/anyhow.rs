use anyhow::Result;
use rltbl_db::{any::AnyPool, core::DbQuery, db_value::DbRows};

#[tokio::test]
async fn test_anyhow() {
    let pool = AnyPool::connect(":memory:").await.unwrap();
    let rows = query(pool).await.expect("Error getting rows");
    let value: u64 = rows
        .value()
        .and_then(|v| v.try_into())
        .expect("Error getting u64 value");
    assert_eq!(value, 1);
}

async fn query(pool: AnyPool) -> Result<DbRows> {
    Ok(pool.query("SELECT 1", ()).await?)
}
