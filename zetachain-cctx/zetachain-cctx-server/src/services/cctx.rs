use std::sync::Arc;

use sea_orm::DatabaseConnection;
use tonic::{Request, Response, Status};

use zetachain_cctx_proto::blockscout::zetachain_cctx::v1::{
    GetCctxInfoRequest,GetCctxInfoResponse,cctx_info_service_server::CctxInfoService
};



#[derive(Debug, Default)]
pub struct CctxService {
    db: Arc<DatabaseConnection>,
}

impl CctxService {
    pub fn new(db: Arc<DatabaseConnection>) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl CctxInfoService for CctxService {
    async fn get_cctx_info(&self, request: Request<GetCctxInfoRequest>) -> Result<Response<GetCctxInfoResponse>, Status> {

        let request = request.into_inner();

        // let cctx = Model::find()
        //     .filter(cctx::Column::Id.eq(request.cctx_id))
        //     .one(&self.db)
        //     .await?;


        
        Ok(Response::new(GetCctxInfoResponse {
            cctx: None,
        }))
    }
}






