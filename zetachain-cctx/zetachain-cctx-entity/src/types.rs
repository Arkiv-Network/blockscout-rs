use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "Text")]
pub enum TxFinalizationStatus {
    #[sea_orm(string_value = "NotFinalized")]
    NotFinalized,
    #[sea_orm(string_value = "Finalized")]
    Finalized,
    #[sea_orm(string_value = "Executed")]
    Executed,
}

impl From<String> for TxFinalizationStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "NotFinalized" => TxFinalizationStatus::NotFinalized,
            "Finalized" => TxFinalizationStatus::Finalized,
            "Executed" => TxFinalizationStatus::Executed,
            _ => panic!("Invalid TxFinalizationStatus: {}", value),
        }
    }
} 

#[derive(Debug, Clone, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(
    rs_type = "String",
    db_type = "Enum",
    enum_name = "watermark_type"
)]
pub enum WatermarkType {
    #[sea_orm(string_value = "realtime")]
    Realtime,
    #[sea_orm(string_value = "historical")]
    Historical,
}

