use async_trait::async_trait;
use crate::models::CrossChainTx;

#[async_trait]
pub trait EventBroadcaster: Send + Sync {
    /// Broadcast that a CCTX has been updated
    async fn broadcast_cctx_update(&self, cctx_index: String, cctx_data: CrossChainTx);
    
    /// Broadcast that new CCTXs have been imported
    async fn broadcast_new_cctxs(&self, cctx_indices: Vec<String>);
}

/// No-op implementation for when WebSocket events are disabled
pub struct NoOpBroadcaster;

#[async_trait]
impl EventBroadcaster for NoOpBroadcaster {
    async fn broadcast_cctx_update(&self, _cctx_index: String, _cctx_data: CrossChainTx) {
        // No-op
    }
    
    async fn broadcast_new_cctxs(&self, _cctx_indices: Vec<String>) {
        // No-op
    }
} 