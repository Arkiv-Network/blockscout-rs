use std::{num::NonZeroU32, sync::Arc, time::Duration};

use anyhow::Error;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use reqwest::{Method, Request, Response, Url};
use serde::{Deserialize, Serialize};
use tokio::time::timeout;
use tracing::instrument;

use crate::models::{CCTXResponse, CrossChainTx, InboundHashToCctxResponse, PagedCCTXResponse, PagedTokenResponse};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcSettings {
    pub url: String,
    pub request_per_second: u32,
    pub num_of_retries: u32,
    pub retry_delay_ms: u32,
}

type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

impl Client {
    pub fn new(settings: RpcSettings) -> Self {
        let http = reqwest::Client::new();
        let limiter = Arc::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(settings.request_per_second).unwrap(),
        )));
        Self {
            settings,
            http,
            limiter,
        }
    }
}
pub struct Client {
    pub settings: RpcSettings,
    pub http: reqwest::Client,
    limiter: Arc<Limiter>,
}

impl Client {
    #[instrument(level="debug",skip(self, request), fields(url = %request.url()))]
    async fn make_request(&self, request: Request) -> anyhow::Result<Response> {
        for attempt in 1..=self.settings.num_of_retries {
            let permit = timeout(
                Duration::from_millis(self.settings.retry_delay_ms.into()),
                self.limiter.until_ready(),
            )
            .await;

            match permit {
                Ok(_) => {
                    return self
                        .http
                        .execute(request)
                        .await
                        .map_err(|e| anyhow::anyhow!("HTTP request error: {}", e));
                }
                Err(_) => {
                    tracing::warn!(
                        request = ?request.url(),
                        attempt,
                        MAX_RETRIES =? self.settings.num_of_retries,
                        "Rate limiter wait timed out, retrying..."
                    );
                }
            }
        }

        Err(anyhow::anyhow!(
            "Exceeded maximum retry attempts ({}) waiting for rate limiter",
            self.settings.num_of_retries,
        ))
    }

    #[instrument(level="debug",skip_all)]
    pub async fn fetch_cctx(&self, index: &str) -> anyhow::Result<CrossChainTx> {
        let mut url: Url = self.settings.url.parse().unwrap();
        url.set_path(&format!("{}crosschain/cctx/{}", url.path(), index));
        let request = Request::new(Method::GET, url);
        let response = self.make_request(request).await?.error_for_status()?;
        let body = response.json::<CCTXResponse>().await?;
        Ok(body.cross_chain_tx)
    }

    #[instrument(level="debug",skip_all)]
    pub async fn list_cctxs(
        &self,
        pagination_key: Option<&str>,
        unordered: bool,
        batch_size: u32
    ) -> Result<PagedCCTXResponse, Error> {
        let mut url: Url = self.settings.url.parse().unwrap();
        let path = url.path();
        url.set_path(&format!("{}crosschain/cctx", path));
        url.query_pairs_mut()
            .append_pair("pagination.limit", &batch_size.to_string())
            .append_pair("unordered", &unordered.to_string())
            .finish();

        if let Some(pagination_key) = pagination_key {
            url.query_pairs_mut()
                .append_pair("pagination.key", pagination_key);
        }

        let request = Request::new(Method::GET, url.clone());
        let response = self
            .make_request(request)
            .await?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("HTTP request error: {}", e))?;

        let text = response.text().await?;
        let body = serde_json::from_str::<PagedCCTXResponse>(&text)
            .map_err(|e| anyhow::anyhow!("JSON parsing error: {}\n{}", e, text))?;    
        Ok(body)
    }

    #[instrument(level="debug",skip_all)]
    pub async fn get_inbound_hash_to_cctx_data(
        &self,
        cctx_index: &str,
    ) -> Result<InboundHashToCctxResponse, Error> {
        let mut url: Url = self.settings.url.parse().unwrap();
        let path = url.path();
        url.set_path(&format!("{}crosschain/inboundHashToCctxData/{}", path, cctx_index));

        let request = Request::new(Method::GET, url.clone());
        let response = self
            .make_request(request)
            .await?;

        // Handle 404 by returning an empty result
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(InboundHashToCctxResponse {
                cross_chain_txs: Vec::new(),
            });
        }

        let response = response.error_for_status()
            .map_err(|e| anyhow::anyhow!("HTTP request error: {}", e))?;

        let text = response.text().await?;
        let body = serde_json::from_str::<InboundHashToCctxResponse>(&text)
            .map_err(|e| anyhow::anyhow!("JSON parsing error: {}\n{}", e, text))?;    
        Ok(body)
    }

    #[instrument(level="debug",skip_all)]
    pub async fn list_tokens(
        &self,
        pagination_key: Option<&str>,
        batch_size: u32
    ) -> Result<PagedTokenResponse, Error> {
        let mut url: Url = self.settings.url.parse().unwrap();
        let path = url.path();
        url.set_path(&format!("{}fungible/foreign_coins", path));
        url.query_pairs_mut()
            .append_pair("pagination.limit", &batch_size.to_string())
            .finish();

        if let Some(pagination_key) = pagination_key {
            url.query_pairs_mut()
                .append_pair("pagination.key", pagination_key);
        }

        let request = Request::new(Method::GET, url.clone());
        let response = self
            .make_request(request)
            .await?
            .error_for_status()
            .map_err(|e| anyhow::anyhow!("HTTP request error: {}", e))?;

        let text = response.text().await?;
        let body = serde_json::from_str::<PagedTokenResponse>(&text)
            .map_err(|e| anyhow::anyhow!("JSON parsing error: {}\n{}", e, text))?;    
        Ok(body)
    }
}

fn default_request_per_second() -> u32 {
    100
}

fn default_num_of_retries() -> u32 {
    10
}

fn default_retry_delay_ms() -> u32 {
    1000
}

impl Default for RpcSettings {
    fn default() -> Self {
        Self {
            url: "http://localhost".to_string(),
            request_per_second: default_request_per_second(),
            num_of_retries: default_num_of_retries(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}
