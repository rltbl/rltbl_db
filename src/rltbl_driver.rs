use anyhow::Result;
use async_trait::async_trait;
use rand;
use rlt::{BenchSuite, IterInfo, IterReport, Status, cli::BenchCli};
use rltbl_db::{AnyPool, values};
use std::time::Instant;

#[derive(Clone)]
pub(crate) struct RltblDriver {
    name: &'static str,
    url: String,
    // TODO: Just use the IterReport fields for this.
    tests_run: usize,
}

impl RltblDriver {
    pub async fn test(name: &str, bench: &BenchCli) {
        let url = match name.to_lowercase().as_str() {
            "tokio" | "tokio-postgres" | "tokio-postgresql" => "postgresql:///rltbl_db",
            "rusqlite" | "libsql" => ":memory:",
            _ => panic!("Invalid driver: '{name}'"),
        };
        let rltbl_driver = RltblDriver {
            name: "rltbl_driver",
            url: url.to_string(),
            tests_run: 0,
        };

        rlt::cli::run(bench.clone(), rltbl_driver).await.unwrap();
    }
}

#[async_trait]
impl BenchSuite for RltblDriver {
    type WorkerState = AnyPool;

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is initialize the state for all of the workers.
    // That said, maybe what needs to be done to get a per-worker state is to somehow
    // use the worker_id. In any case ...
    /// Initialize the state for a worker
    async fn state(&self, _worker_id: u32) -> Result<Self::WorkerState> {
        eprintln!("Connecting to the database at '{}'.", self.url);
        Ok(AnyPool::connect(&self.url).await.unwrap())
    }

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is to run the setup procedure for all of the workers (as judged by the
    // number of rows observed in each of the four tables once the test is running), i.e.,
    // before any of them run.
    // That said, maybe what needs to be done to get a per-worker setup is to somehow
    // use the worker_id.
    /// Setup procedure before each worker starts.
    async fn setup(&mut self, pool: &mut Self::WorkerState, _worker_id: u32) -> Result<()> {
        eprintln!("Preparing the database.");
        let table = "rltbl_driver";
        pool.drop_table(table).await.unwrap();

        pool.execute(&format!("CREATE TABLE {table} ( foo INT, bar TEXT )"), ())
            .await
            .unwrap();
        pool.execute(
            &format!("CREATE VIEW {table}_view AS SELECT * FROM {table}"),
            (),
        )
        .await
        .unwrap();

        // Add a few tens of thousands of values to the table:
        let mut values = vec![];
        for i in 0..5 {
            for j in 0..30000 {
                values.push(format!("({i}, '{j}')"));
            }
        }
        let values = values.join(", ");
        pool.execute(
            &format!("INSERT INTO {table} (foo, bar) VALUES {}", values),
            (),
        )
        .await
        .unwrap();
        Ok(())
    }

    /// The actual content of the test.
    async fn bench(&mut self, pool: &mut Self::WorkerState, _: &IterInfo) -> Result<IterReport> {
        eprintln!(
            "Running test '{}', iteration #{}.",
            self.name, self.tests_run
        );
        let start = Instant::now();

        let rows = pool
            .query(
                &format!(
                    "SELECT foo, bar \
                     FROM rltbl_driver_view \
                     WHERE foo > {pp}1
                     ORDER BY foo",
                    pp = pool.syntax().param_prefix(),
                ),
                &values![0_i32],
            )
            .await
            .unwrap();

        for row in rows.iter() {
            let _ = row.get("foo").unwrap();
        }

        if rand::random() && rand::random() {
            pool.execute(
                &format!(
                    "INSERT INTO rltbl_driver (foo, bar) VALUES ({pp}1, {pp}2)",
                    pp = pool.syntax().param_prefix()
                ),
                &values![1_i32, "1"],
            )
            .await
            .unwrap();
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
    /// Teardown procedure after each worker finishes.
    async fn teardown(self, _pool: Self::WorkerState, _info: IterInfo) -> Result<()> {
        eprintln!(
            "Test is over after {} iterations. Tearing down.",
            self.tests_run
        );
        Ok(())
    }
}
