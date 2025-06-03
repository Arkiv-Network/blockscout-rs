use crate::sea_orm_active_enums::{TxFinalizationStatus, WatermarkType};

impl TryFrom<String> for TxFinalizationStatus {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "NotFinalized" => Ok(TxFinalizationStatus::NotFinalized),
            "Finalized" => Ok(TxFinalizationStatus::Finalized),
            "Executed" => Ok(TxFinalizationStatus::Executed),
            _ => Err(format!("Invalid TxFinalizationStatus: {}", value)),
        }
    }
}

impl TryFrom<String> for WatermarkType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "realtime" => Ok(WatermarkType::Realtime),
            "historical" => Ok(WatermarkType::Historical),
            _ => Err(format!("Invalid WatermarkType: {}", value)),
        }
    }
} 