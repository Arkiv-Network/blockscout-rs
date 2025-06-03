use std::thread;
use serde::Deserialize;

use crate::client::RpcSettings;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexerSettings {
   pub concurrency: u32,
   pub polling_interval: u64,
}


fn default_concurrency() -> u32 {
    thread::available_parallelism()
    .map(|p| p.get() as u32)
    .unwrap_or(1)
}


impl Default for IndexerSettings {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(), 
            polling_interval: 1000,
        }
    }
}