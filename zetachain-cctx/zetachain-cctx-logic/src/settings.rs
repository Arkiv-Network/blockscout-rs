pub struct IndexerSettings {
    pub retry_interval: u64,

    pub polling_interval: u64,
}


fn default_concurrency() -> u32 {
    thread::available_parallelism().unwrap_or(1).into()
}