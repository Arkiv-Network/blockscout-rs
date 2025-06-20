use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add unique constraint on the index column of cross_chain_tx table
        manager
            .create_index(
                Index::create()
                    .name("idx_cross_chain_tx_index_unique")
                    .table(CrossChainTx::Table)
                    .col(CrossChainTx::Index)
                    .unique()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the unique index
        manager
            .drop_index(
                Index::drop()
                    .name("idx_cross_chain_tx_index_unique")
                    .table(CrossChainTx::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
enum CrossChainTx {
    Table,
    Index,
} 