use anyhow::{Error, Result};
use rltbl_db::{AnyPool, Rows};

#[tokio::test]
async fn test_anyhow() {
    let pool = AnyPool::connect(":memory:").await.unwrap();
    let rows = query(pool).await.expect("Error getting rows");
    let value: u64 = rows
        .try_into_value::<u64>()
        .expect("Error getting u64 value");
    assert_eq!(value, 1);
}

async fn query(pool: AnyPool) -> Result<Rows> {
    Ok(pool
        .query("SELECT 1", ())
        .await
        .map_err(|err| Error::msg(err.to_string()))?)
}
