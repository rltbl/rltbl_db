use anyhow::Result;
use async_trait::async_trait;
use rand::{
    SeedableRng as _,
    distr::{Distribution as _, Uniform},
    rngs::StdRng,
};
use rlt::{BenchSuite, IterInfo, IterReport, Status, cli::BenchCli};
use rltbl_db::{AnyPool, cache::CachingStrategy};
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, num::NonZero, str::FromStr, time::Instant};

#[derive(Clone)]
pub(crate) struct CachingPerformance {
    name: &'static str,
    seed: i64,
    url: String,
    tables: Vec<String>,
    strategy: CachingStrategy,
    edit_rate: usize,
    // TODO: Just use the IterReport fields for this.
    tests_run: usize,
}

impl CachingPerformance {
    pub(crate) async fn test(
        kind: &str,
        bench: &BenchCli,
        strategy: &str,
        edit_rate: usize,
        totals_file: &str,
        seed: i64,
    ) {
        let url = match kind.to_lowercase().as_str() {
            "postgres" | "postgresql" => "postgresql:///rltbl_db",
            "sqlite" => ":memory:",
            _ => panic!("Invalid kind: '{kind}'"),
        };
        let strategy = CachingStrategy::from_str(strategy).unwrap();

        // Read in the baselines from a JSON on disk:
        let mut baselines: CachingBaselines = {
            let baselines = slurp::read_all_to_string(totals_file).unwrap();
            serde_json::from_str(&baselines).unwrap()
        };

        let caching_perf = CachingPerformance {
            name: "caching_performance",
            seed,
            url: url.to_string(),
            tables: ["alpha", "beta", "gamma", "delta"]
                .iter()
                .map(|t| t.to_string())
                .collect(),
            strategy,
            edit_rate,
            tests_run: 0,
        };

        // Set the number of iterations to run using the baseline info:
        let mut bench = bench.clone();
        let iterations = baselines.get_iterations(kind, &strategy);
        bench.iterations = NonZero::new(iterations);

        // Save a copy of the baseline:
        let save_baseline = bench.save_baseline.clone();

        // Mark the start time of the overall test:
        let overall_now = Instant::now();

        // Run the test for however many iterations were configured above:
        rlt::cli::run(bench, caching_perf.clone()).await.unwrap();

        // Check that the overall running time is no longer than the baseline time
        // (within a margin of error):
        let elapsed = overall_now.elapsed().as_secs();
        eprintln!("Total elapsed time: {elapsed}s");

        if let Some(_) = save_baseline {
            let expected = (((elapsed + 5) as f64 / 10_f64).ceil() as u64) * 10;
            baselines.save(totals_file, kind, &strategy, iterations, expected);
        } else {
            baselines.compare_with(kind, &strategy, elapsed);
        }
    }

    fn random_between(&mut self, min: usize, max: usize) -> usize {
        let between = Uniform::try_from(min..max).unwrap();
        let mut rng = if self.seed < 0 {
            StdRng::from_rng(&mut rand::rng())
        } else {
            self.seed += 10;
            StdRng::seed_from_u64(self.seed as u64)
        };
        between.sample(&mut rng)
    }

    fn random_table<'a>(&mut self) -> String {
        let index = self.random_between(0, self.tables.len());
        self.tables[index].to_string()
    }
}

#[async_trait]
impl BenchSuite for CachingPerformance {
    type WorkerState = AnyPool;

    // The comment below is from the source code for the trait in rlt, but I think what it
    // actually does is initialize the state for all of the workers.
    // That said, maybe what needs to be done to get a per-worker state is to somehow
    // use the worker_id.
    // Initialize the state for a worker
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
    // Setup procedure before each worker starts.
    async fn setup(&mut self, pool: &mut Self::WorkerState, _worker_id: u32) -> Result<()> {
        eprintln!("Preparing the database.");
        for table in &self.tables {
            pool.drop_table(&table).await.unwrap();
            pool.drop_view(&format!("{table}_view")).await.unwrap();
            pool.execute(&format!("CREATE TABLE {table} ( foo INT, bar INT )"), ())
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
                    values.push(format!("({i}, {j})"));
                }
            }
            let values = values.join(", ");
            pool.execute(
                &format!("INSERT INTO {table} (foo, bar) VALUES {}", values),
                (),
            )
            .await
            .unwrap();
        }

        pool.set_caching_strategy(&self.strategy);
        pool.set_cache_aware_query(true);

        Ok(())
    }

    async fn bench(&mut self, pool: &mut Self::WorkerState, _: &IterInfo) -> Result<IterReport> {
        eprintln!(
            "Running test '{}', iteration #{}.",
            self.name, self.tests_run
        );
        let start = Instant::now();

        let select_table = self.random_table();
        pool.cache(
            &format!("SELECT foo, SUM(bar) FROM {select_table}_view GROUP BY foo ORDER BY foo"),
            (),
        )
        .await
        .unwrap();
        if self.edit_rate != 0 && self.random_between(0, self.edit_rate) == 0 {
            let table_to_edit = self.random_table();
            pool.execute(
                &format!("INSERT INTO {table_to_edit} (foo) VALUES (1), (1)"),
                (),
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
    // Teardown procedure after each worker finishes.
    async fn teardown(self, _: Self::WorkerState, _info: IterInfo) -> Result<()> {
        eprintln!(
            "Test is over after {} iterations. Tearing down.",
            self.tests_run
        );
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CachingBaselinesEntry {
    iterations: u64,
    expected_time: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct CachingBaselines {
    sqlite_none: CachingBaselinesEntry,
    postgresql_none: CachingBaselinesEntry,
    sqlite_truncate_all: CachingBaselinesEntry,
    postgresql_truncate_all: CachingBaselinesEntry,
    sqlite_truncate: CachingBaselinesEntry,
    postgresql_truncate: CachingBaselinesEntry,
    sqlite_trigger: CachingBaselinesEntry,
    postgresql_trigger: CachingBaselinesEntry,
    sqlite_memory: CachingBaselinesEntry,
    postgresql_memory: CachingBaselinesEntry,
}

impl CachingBaselines {
    fn save(
        &mut self,
        totals_file: &str,
        kind: &str,
        strategy: &CachingStrategy,
        iterations: u64,
        expected_time: u64,
    ) {
        self.save_iterations(kind, strategy, iterations);
        self.save_expected_time(kind, strategy, expected_time);
        let mut output = File::create(totals_file).expect("Error creating file");
        writeln!(
            output,
            "{}",
            serde_json::to_string(self).expect("Error serializing")
        )
        .expect("Error writing to file");
    }

    fn get_iterations(&self, kind: &str, strategy: &CachingStrategy) -> u64 {
        match kind {
            "sqlite" => match strategy {
                CachingStrategy::None => self.sqlite_none.iterations,
                CachingStrategy::TruncateAll => self.sqlite_truncate_all.iterations,
                CachingStrategy::Truncate => self.sqlite_truncate.iterations,
                CachingStrategy::Trigger => self.sqlite_trigger.iterations,
                CachingStrategy::Memory(_) => self.sqlite_memory.iterations,
            },
            "postgres" => match strategy {
                CachingStrategy::None => self.postgresql_none.iterations,
                CachingStrategy::TruncateAll => self.postgresql_truncate_all.iterations,
                CachingStrategy::Truncate => self.postgresql_truncate.iterations,
                CachingStrategy::Trigger => self.postgresql_trigger.iterations,
                CachingStrategy::Memory(_) => self.postgresql_memory.iterations,
            },
            _ => panic!(),
        }
    }

    fn save_iterations(&mut self, kind: &str, strategy: &CachingStrategy, iterations: u64) {
        match kind {
            "sqlite" => match strategy {
                CachingStrategy::None => self.sqlite_none.iterations = iterations,
                CachingStrategy::TruncateAll => self.sqlite_truncate_all.iterations = iterations,
                CachingStrategy::Truncate => self.sqlite_truncate.iterations = iterations,
                CachingStrategy::Trigger => self.sqlite_trigger.iterations = iterations,
                CachingStrategy::Memory(_) => self.sqlite_memory.iterations = iterations,
            },
            "postgres" => match strategy {
                CachingStrategy::None => self.postgresql_none.iterations = iterations,
                CachingStrategy::TruncateAll => {
                    self.postgresql_truncate_all.iterations = iterations
                }
                CachingStrategy::Truncate => self.postgresql_truncate.iterations = iterations,
                CachingStrategy::Trigger => self.postgresql_trigger.iterations = iterations,
                CachingStrategy::Memory(_) => self.postgresql_memory.iterations = iterations,
            },
            _ => panic!(),
        }
    }

    fn get_expected_time(&self, kind: &str, strategy: &CachingStrategy) -> u64 {
        match kind {
            "sqlite" => match strategy {
                CachingStrategy::None => self.sqlite_none.expected_time,
                CachingStrategy::TruncateAll => self.sqlite_truncate_all.expected_time,
                CachingStrategy::Truncate => self.sqlite_truncate.expected_time,
                CachingStrategy::Trigger => self.sqlite_trigger.expected_time,
                CachingStrategy::Memory(_) => self.sqlite_memory.expected_time,
            },
            "postgres" => match strategy {
                CachingStrategy::None => self.postgresql_none.expected_time,
                CachingStrategy::TruncateAll => self.postgresql_truncate_all.expected_time,
                CachingStrategy::Truncate => self.postgresql_truncate.expected_time,
                CachingStrategy::Trigger => self.postgresql_trigger.expected_time,
                CachingStrategy::Memory(_) => self.postgresql_memory.expected_time,
            },
            _ => panic!(),
        }
    }

    fn save_expected_time(&mut self, kind: &str, strategy: &CachingStrategy, expected_time: u64) {
        match kind {
            "sqlite" => match strategy {
                CachingStrategy::None => self.sqlite_none.expected_time = expected_time,
                CachingStrategy::TruncateAll => {
                    self.sqlite_truncate_all.expected_time = expected_time
                }
                CachingStrategy::Truncate => self.sqlite_truncate.expected_time = expected_time,
                CachingStrategy::Trigger => self.sqlite_trigger.expected_time = expected_time,
                CachingStrategy::Memory(_) => self.sqlite_memory.expected_time = expected_time,
            },
            "postgres" => match strategy {
                CachingStrategy::None => self.postgresql_none.expected_time = expected_time,
                CachingStrategy::TruncateAll => {
                    self.postgresql_truncate_all.expected_time = expected_time
                }
                CachingStrategy::Truncate => self.postgresql_truncate.expected_time = expected_time,
                CachingStrategy::Trigger => self.postgresql_trigger.expected_time = expected_time,
                CachingStrategy::Memory(_) => self.postgresql_memory.expected_time = expected_time,
            },
            _ => panic!(),
        }
    }

    fn compare_with(&self, kind: &str, strategy: &CachingStrategy, elapsed: u64) {
        let expected = self.get_expected_time(kind, strategy);
        // We allow for a 5% noise threshold:
        if elapsed as f64 > expected as f64 * 1.05_f64 {
            panic!("Took longer than {expected}s.");
        }
    }
}
