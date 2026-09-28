use anyhow::Result;
use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime, tokio_postgres::NoTls};
use rlt::{BenchSuite, IterInfo, IterReport, Status, cli::BenchCli};
use std::time::Instant;

#[derive(Clone)]
pub(crate) struct TokioPostgresDriver {
    name: &'static str,
    // TODO: Just use the IterReport fields for this.
    tests_run: usize,
}

impl TokioPostgresDriver {
    pub async fn test(bench: &BenchCli) {
        let tokio_postgres_driver = TokioPostgresDriver {
            name: "tokio_postgres_driver",
            tests_run: 0,
        };
        rlt::cli::run(bench.clone(), tokio_postgres_driver)
            .await
            .unwrap();
    }
}

#[async_trait]
impl BenchSuite for TokioPostgresDriver {
    type WorkerState = Pool;

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is initialize the state for all of the workers.
    // That said, maybe what needs to be done to get a per-worker state is to somehow
    // use the worker_id.
    // Initialize the state for a worker
    async fn state(&self, _worker_id: u32) -> Result<Self::WorkerState> {
        eprintln!("Connecting to the postgres database.");
        let mut cfg = Config::new();
        let db_name = "rltbl_db";
        cfg.dbname = Some(db_name.to_string());
        let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
        Ok(pool)
    }

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is to run the setup procedure for all of the workers (as judged by the
    // number of rows observed in each of the four tables once the test is running), i.e.,
    // before any of them run.
    // That said, maybe what needs to be done to get a per-worker setup is to somehow
    // use the worker_id.
    // Setup procedure before each worker starts.
    async fn setup(&mut self, pool: &mut Self::WorkerState, _worker_id: u32) -> Result<()> {
        eprintln!("Preparing the database.");
        let client = pool.get().await.unwrap();
        let stmt = client
            .prepare("DROP TABLE IF EXISTS rltbl_driver CASCADE")
            .await
            .unwrap();
        let _ = client.query(&stmt, &[]).await.unwrap();

        let stmt = client
            .prepare("CREATE TABLE rltbl_driver (foo INT, bar TEXT)")
            .await
            .unwrap();
        let _ = client.query(&stmt, &[]).await.unwrap();

        let stmt = client
            .prepare("CREATE VIEW rltbl_driver_view AS SELECT * FROM rltbl_driver")
            .await
            .unwrap();
        let _ = client.query(&stmt, &[]).await.unwrap();

        // Add a few tens of thousands of values to the table:
        let mut values = vec![];
        for i in 0..5 {
            for j in 0..30000 {
                values.push(format!("({i}, '{j}')"));
            }
        }
        let values = values.join(", ");
        let stmt = client
            .prepare(&format!(
                "INSERT INTO rltbl_driver (foo, bar) VALUES {}",
                values
            ))
            .await
            .unwrap();
        let _ = client.query(&stmt, &[]).await.unwrap();
        Ok(())
    }

    async fn bench(&mut self, pool: &mut Self::WorkerState, _: &IterInfo) -> Result<IterReport> {
        eprintln!(
            "Running test '{}', iteration #{}.",
            self.name, self.tests_run
        );
        let start = Instant::now();

        let client = pool.get().await.unwrap();
        let sql = "SELECT foo, bar \
                   FROM rltbl_driver_view \
                   WHERE foo > $1 \
                   ORDER BY foo";
        let stmt = client.prepare(&sql).await.unwrap();
        let rows = client.query(&stmt, &[&0_i32]).await.unwrap();

        for row in rows.iter() {
            let _ = row.try_get::<usize, Option<i32>>(0).unwrap().unwrap();
        }

        if rand::random() && rand::random() {
            let sql = "INSERT INTO rltbl_driver (foo, bar) VALUES ($1, $2)";
            let stmt = client.prepare(&sql).await.unwrap();
            let _ = client.query(&stmt, &[&1_i32, &"1"]).await.unwrap();
        }

        let duration = start.elapsed();
        self.tests_run += 1;

        Ok(IterReport {
            duration,
            status: Status::success(0),
            // Not used:
            items: 0,
            bytes: 0,
        })
    }

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is to run the teardown procedure for all of the workers, i.e., after they
    // are all done.
    // That said, maybe what needs to be done to get a per-worker teardown is to somehow
    // use the worker_id.
    // Teardown procedure after each worker finishes.
    async fn teardown(self, _pool: Self::WorkerState, _info: IterInfo) -> Result<()> {
        eprintln!(
            "Test is over after {} iterations. Tearing down.",
            self.tests_run
        );
        Ok(())
    }
}
