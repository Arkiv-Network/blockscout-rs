use std::sync::Arc;
use std::time::Duration;

use anyhow::Ok;

use tokio::join;
use tokio::task::JoinHandle;
use uuid::Uuid;
use crate::database::ZetachainCctxDatabase;
use crate::models::PagedCCTXResponse;
use zetachain_cctx_entity::sea_orm_active_enums::{WatermarkType};
use zetachain_cctx_entity::{
    cross_chain_tx, watermark,
};

use sea_orm::{ColumnTrait, DbBackend, Statement};

use crate::{client::Client, settings::IndexerSettings};
use chrono::Utc;
use sea_orm::ActiveModelTrait;
use futures::StreamExt;
use sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, QueryFilter};
use tracing::{instrument, Instrument};

use futures::stream::{select_with_strategy, PollNext};



pub struct Indexer {
    pub settings: IndexerSettings,
    pub db: Arc<DatabaseConnection>,
    pub client: Arc<Client>,
    pub database: Arc<ZetachainCctxDatabase>,
}

enum IndexerJob {
    StatusUpdate(cross_chain_tx::Model, Uuid),             //cctx index and id to be updated
    GapFill(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
    HistoricalDataFetch(watermark::Model, Uuid), // Watermark (pointer) to the next page of cctxs to be fetched
}


async fn update_cctx_status(
    job_id: Uuid,
    database: Arc<ZetachainCctxDatabase>,
    client: &Client,
    existing_cctx: cross_chain_tx::Model,
) -> anyhow::Result<()> {
    let fetched_cctx = client.get_cctx(&existing_cctx.index).await?;
    database.update_cctx_status(job_id, existing_cctx, fetched_cctx).await.map_err(|e| anyhow::anyhow!(e))?;
    Ok(())
}

#[instrument(skip(database, client), fields(job_id = %job_id))]
async fn gap_fill(
    job_id: Uuid,
    database: Arc<ZetachainCctxDatabase>,
    client: &Client,
    watermark: watermark::Model,
) -> anyhow::Result<()> {
    let PagedCCTXResponse { cross_chain_tx : cctxs, pagination } = client.list_cctx(Some(&watermark.pointer), true, 100,job_id).await.unwrap();
    
    let earliest_cctx = cctxs.last().unwrap();
    let last_synced_cctx = database.get_cctx(earliest_cctx.index.clone()).await?;

    if last_synced_cctx.is_none() {
        database.batch_insert_transactions(job_id, &cctxs).await?;    
    }
    database.move_watermark(watermark, pagination.next_key).await?;
    Ok(())
}

#[instrument(skip(db), fields(watermark_id = %watermark.id))]
pub async fn unlock_watermark(db: &DatabaseConnection, watermark: watermark::Model) -> anyhow::Result<()> {
    let res = watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(false),
        updated_at: ActiveValue::Set(chrono::Utc::now().naive_utc()),
        ..Default::default()
    })
    .filter(watermark::Column::Id.eq(watermark.id))
    .exec(db)
    .instrument(tracing::info_span!("unlocking watermark", watermark_id = %watermark.id))
    .await?;

    tracing::info!("unlocked watermark: {:?}", res);
    Ok(())
}

pub async fn lock_watermark(db: &DatabaseConnection, watermark: watermark::Model) -> anyhow::Result<()> {
    watermark::Entity::update(watermark::ActiveModel {
        id: ActiveValue::Unchanged(watermark.id),
        lock: ActiveValue::Set(true),
        ..Default::default()
    }).filter(watermark::Column::Id.eq(watermark.id))
    .exec(db)
    .await?;
    Ok(())
}
async fn unlock_cctx(db: &DatabaseConnection, id: i32) -> anyhow::Result<()> {
    cross_chain_tx::Entity::update(cross_chain_tx::ActiveModel {
        id: ActiveValue::Unchanged(id),
        lock: ActiveValue::Set(false),
        ..Default::default()
    })
    .filter(cross_chain_tx::Column::Id.eq(id))
    .exec(db)
    .await?;
    Ok(())
}
#[instrument(skip(database, client), fields(job_id = %job_id))]
async fn historical_sync(
    database: Arc<ZetachainCctxDatabase>,
    client: &Client,
    watermark: watermark::Model,
    job_id: Uuid,
    batch_size: u32,
) -> anyhow::Result<()> {
    let response = client
        .list_cctx(Some(&watermark.pointer), true, batch_size,job_id)
        .instrument( tracing::info_span!("fetching historical data from node", job_id = %job_id))
        .await?;
    let cross_chain_txs = response.cross_chain_tx;
    let pagination = response.pagination;
    //atomically insert cctxs and update watermark

    if let Err(e) = database.batch_insert_transactions(job_id, &cross_chain_txs).await {
        tracing::error!(error = %e, "Failed to batch insert transactions, pointer = {}", watermark.pointer);
        tracing::error!("cross_chain_txs: {:?}", cross_chain_txs);
        return Err(e);
    }
    database.move_watermark(watermark, pagination.next_key).await?;
    Ok(())
}



#[instrument(skip(database, client), fields(job_id = %job_id))]
async fn realtime_fetch(job_id: Uuid,database: Arc<ZetachainCctxDatabase>, client: &Client) -> anyhow::Result<()> {
    let response = client
        .list_cctx(None, false, 10, job_id)
        .instrument(tracing::info_span!("requesting realtime cctxs"))
        .await
        .unwrap();
    let txs = response.cross_chain_tx;
    if txs.is_empty() {
        tracing::info!("No new cctxs found");
        return Ok(());
    }
    let next_key = response.pagination.next_key;

    //check whether the latest fetched cctx is present in the database
    //we fetch transaction in LIFO order
    let latest_fetched = txs.first().unwrap();
    let latest_loaded = database.get_cctx(latest_fetched.index.clone()).await?;


    //if latest fetched cctx is in the db that means that the upper boundary is already covered and there is no new cctxs to save
    if latest_loaded.is_some() {
        tracing::info!("latest cctx already exists, skipping");
        return Ok(());
    }
    //now we need to check the lower boudary ( the earliest of the fetched cctxs)
    let earliest_fetched = txs.last().unwrap();
    let earliest_loaded = database.get_cctx(earliest_fetched.index.clone()).await?;
    
    if earliest_loaded.is_none() {
            // the lower boundary is not covered, so there could be more transaction that happened earlier, that we haven't observed
            //we have to save current pointer to continue fetching until we hit a known transaction
            tracing::info!("earliest cctx not found, creating new realtime watermark");
            database.create_realtime_watermark(next_key).await?;
    } 

    if let Err(e) = database.batch_insert_transactions(job_id, &txs).await {
        tracing::error!(error = %e, "Failed to batch insert transactions");
        return Err(e);
    }

    Ok(())
}


fn prio_left(_: &mut ()) -> PollNext {
    PollNext::Left
}
impl Indexer {
    pub fn new(
        settings: IndexerSettings,
        db: Arc<DatabaseConnection>,
        client: Arc<Client>,
        database: Arc<ZetachainCctxDatabase>,
    ) -> Self {
        Self {
            settings,
            db,
            client,
            database,
        }
    }

    #[instrument(skip(self))]
    fn realtime_fetch_handler(&self) -> JoinHandle<()> {
        
        let polling_interval = self.settings.polling_interval;
        let client = self.client.clone();
        let database = self.database.clone();
        tokio::spawn(async move {
            
            loop {
                let job_id = Uuid::new_v4();
                println!("realtime fetch job_id: {}", job_id);
                realtime_fetch(job_id, database.clone(), &client)
                .instrument(tracing::info_span!("realtime fetch", job_id = %job_id))
                .await
                .unwrap();
                tokio::time::sleep(Duration::from_millis(polling_interval)).await;
            }
        })
    }

    #[instrument(skip(self))]
    pub async fn run(&self)-> anyhow::Result<()> {
        
        tracing::info!("initializing indexer");
        let db = self.db.clone();
        // select cctxs where outbound_params.tx_finalization_status is not final
        
        tracing::info!("checking if historical watermark exists");

        //insert historical watermarks if there are no watermarks for historical type
        let historical_watermark = watermark::Entity::find()
        .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
        .one(db.as_ref())
        .await?;

        tracing::info!("removing lock from cctxs");
        cross_chain_tx::Entity::update_many()
        .filter(cross_chain_tx::Column::Lock.eq(true))
        .set(cross_chain_tx::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(db.as_ref())
        .await?;
        
        if historical_watermark.is_none() {  
            tracing::info!("inserting historical watermark");
            watermark::Entity::insert(watermark::ActiveModel {
                watermark_type: ActiveValue::Set(WatermarkType::Historical),
                lock: ActiveValue::Set(false),
                pointer: ActiveValue::Set("MH==".to_string()), //0 in base64
                created_at: ActiveValue::Set(Utc::now().naive_utc()),
                updated_at: ActiveValue::Set(Utc::now().naive_utc()),
                id: ActiveValue::NotSet,
            })
            .exec(db.as_ref())
            .await?;
        } else {
            tracing::info!("historical watermark already exist, pointer: {}", historical_watermark.unwrap().pointer);
        }
        watermark::Entity::update_many()
        .filter(watermark::Column::Lock.eq(true))
        .set(watermark::ActiveModel {
            lock: ActiveValue::Set(false),
            ..Default::default()
        })
        .exec(db.as_ref())
        .await?;

        tracing::info!("setup completed, initializing streams");
        let status_update_batch_size = self.settings.status_update_batch_size;
        let status_update_stream = Box::pin(async_stream::stream! {
            loop {

                let statement = format!(r#"
                WITH cctxs AS (
                    SELECT cctx.id, cctx.index, cctx.last_status_update_timestamp
                    FROM cross_chain_tx cctx
                    JOIN cctx_status cs ON cctx.id = cs.cross_chain_tx_id
                    WHERE cs.status IN ('PendingInbound', 'PendingOutbound', 'PendingRevert')
                    AND cctx.lock = false
                    ORDER BY cctx.last_status_update_timestamp ASC
                    LIMIT {status_update_batch_size}
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE cross_chain_tx cctx
                SET lock = true, last_status_update_timestamp = NOW()
                WHERE id IN (SELECT id FROM cctxs)
                RETURNING id, index, last_status_update_timestamp, lock, creator, index, relayed_message, protocol_contract_version, zeta_fees
                "#
            );

                let statement = Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    statement,
                    vec![],
                );

                let job_id = Uuid::new_v4();
                match cross_chain_tx::Entity::find()
                .from_raw_sql(statement)
                .all(db.as_ref())
                .instrument(tracing::info_span!("fetching cctxs for status update", job_id = %job_id))
                .await {
                    std::result::Result::Ok(cctxs) => {
                        if cctxs.is_empty() {
                            tracing::info!("job_id: {} no cctxs to update", job_id);
                        } else {
                        for cctx in cctxs {
                            yield IndexerJob::StatusUpdate(cctx, job_id);
                            }
                        }
                        
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to fetch cctxs for status update");
                    }
                }
                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
                
            }
        });

        let db = self.db.clone();
        // checks whether the realtme fetcher hasn't actually fetched all the data in a single request, so we might be lagging behind
        let gap_fill_stream = Box::pin(async_stream::stream! {
            loop {

                let watermarks = watermark::Entity::find()
                    .filter(watermark::Column::WatermarkType.eq(WatermarkType::Realtime))
                    .filter(watermark::Column::Lock.eq(false))
                    .filter(watermark::Column::UpdatedAt.lt(Utc::now() - Duration::from_secs(10))) //TODO: make it configurable
                    .all(db.as_ref())
                    .await
                    .unwrap();

                for watermark in watermarks {
                    //update watermark lock to true
                    watermark::Entity::update(watermark::ActiveModel {
                        id: ActiveValue::Set(watermark.id),
                        lock: ActiveValue::Set(true),
                        ..Default::default()
                    })
                    .filter(watermark::Column::Id.eq(watermark.id))
                    .exec(db.as_ref())
                    .await
                    .unwrap();

                    yield IndexerJob::GapFill(watermark, Uuid::new_v4());
                }

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        let db = self.db.clone();
        let historical_stream = Box::pin(async_stream::stream! {
            loop {
                let job_id = Uuid::new_v4();
                let watermarks = watermark::Entity::find()
                    .filter(watermark::Column::WatermarkType.eq(WatermarkType::Historical))
                    .filter(watermark::Column::Lock.eq(false))
                    .one(db.as_ref())
                    .instrument(tracing::info_span!("looking for historical watermark",job_id = %job_id))
                    .await
                    .unwrap();

                    //TODO: add updated_by field to watermark model
                if let Some(watermark) = watermarks {
                    tracing::info!("job_id: {} historical watermark found", job_id);
                    let mut model: watermark::ActiveModel = watermark.clone().into();
                    model.id = ActiveValue::Unchanged(watermark.id); // Ensure primary key is set
                    model.lock = ActiveValue::Set(true);
                    model.updated_at = ActiveValue::Set(Utc::now().naive_utc());
                    model.update(db.as_ref()).await.unwrap();
                    yield IndexerJob::HistoricalDataFetch(watermark, job_id);
                } else {
                    tracing::info!("job_id: {} historical watermark is absent or locked", job_id);
                }
                

                tokio::time::sleep(Duration::from_millis(self.settings.polling_interval)).await;
            }
        });

        // Priority strategy:
        // 1. Gap fill (medium priority - if there's a gap, we're lagging behind)
        // 2. Status update for new cctxs (medium priority)
        // 3. Historical sync (lowest priority - can lag without affecting realtime)
        let combined_stream =
            select_with_strategy(status_update_stream, historical_stream, prio_left);
        let combined_stream = select_with_strategy(gap_fill_stream, combined_stream, prio_left);
        

        // Realtime data fetch must run at configured frequency, so we run it in parallel as a dedicated thread 
        let realtime_handler = self.realtime_fetch_handler();
        
        let streaming = combined_stream
            .for_each_concurrent(Some(self.settings.concurrency as usize), |job| {
                let client = self.client.clone();
                let db = self.db.clone();
                let database = self.database.clone();
                let batch_size = self.settings.historical_batch_size;
                tokio::spawn(async move {
                    match job {
                        IndexerJob::StatusUpdate(existing_cctx, job_id) => {
                            let cctx_id = existing_cctx.id;
                            if let Err(e) =    update_cctx_status(job_id, database.clone(), &client, existing_cctx)
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to update cctx status");
                                unlock_cctx(&db, cctx_id).await.unwrap();
                            }
                        }
                        IndexerJob::GapFill(watermark, job_id) => {
                            if let Err(e) = gap_fill(job_id, database.clone(), &client, watermark.clone())
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to fetch gap fill data");
                            }
                            unlock_watermark(&db, watermark).await.unwrap();
                        }
                        IndexerJob::HistoricalDataFetch(watermark, job_id) => { 
                            if let Err(e) =historical_sync(database.clone(), &client, watermark.clone(), job_id, batch_size)
                            .await {
                                tracing::error!(error = %e, job_id = %job_id, "Failed to fetch historical data");
                                unlock_watermark(&db, watermark).await.unwrap();
                            }
                            // unlock_watermark(&db, watermark).await.unwrap();
                        }
                    }
                });
                futures::future::ready(())
            });
        join!(realtime_handler, streaming).0.map_err(anyhow::Error::from)
        
    
    }
}
