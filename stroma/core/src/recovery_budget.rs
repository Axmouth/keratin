//! Numeric, payload-free limit diagnostics. Never recovery authority.
//! The marker survives existing string-only recovery errors without a wire change.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryBudget {
    InspectionPages,
    InspectionRecords,
    InspectionBytes,
    SnapshotBytes,
    ReplayOperations,
    ArtifactBytes,
    StageRecords,
    StageBytes,
    RecordBytes,
    ReplicaCount,
}
impl RecoveryBudget {
    fn code(self) -> &'static str {
        match self {
            Self::InspectionPages => "inspection_pages",
            Self::InspectionRecords => "inspection_records",
            Self::InspectionBytes => "inspection_bytes",
            Self::SnapshotBytes => "snapshot_bytes",
            Self::ReplayOperations => "replay_operations",
            Self::ArtifactBytes => "artifact_bytes",
            Self::StageRecords => "stage_records",
            Self::StageBytes => "stage_bytes",
            Self::RecordBytes => "record_bytes",
            Self::ReplicaCount => "replica_count",
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecoveryBudgetExceeded {
    pub budget: RecoveryBudget,
    pub limit: u64,
    /// Accepted/charged work before this step, not fully verified recovery proof.
    pub completed: u64,
    /// Additional work refused by this check.
    pub requested: u64,
    pub unchanged_retry_can_help: bool,
}
impl RecoveryBudgetExceeded {
    pub fn message(budget: RecoveryBudget, limit: u64, completed: u64, requested: u64) -> String {
        format!(
            "recovery {} budget exhausted: limit={limit}, completed={completed}, requested={requested} [recovery-budget-v1:{}:{limit}:{completed}:{requested}]",
            if budget == RecoveryBudget::ReplayOperations {
                "replay operation"
            } else {
                budget.code()
            },
            budget.code()
        )
    }
    /// Recognize only our bounded numeric marker. Unknown/legacy errors stay unknown.
    pub fn from_message(message: &str) -> Option<Self> {
        let tail = message.split_once("[recovery-budget-v1:")?.1;
        let end = tail.find(']')?;
        if end > 128 {
            return None;
        }
        let mut parts = tail[..end].split(':');
        let budget = match parts.next()? {
            "inspection_pages" => RecoveryBudget::InspectionPages,
            "inspection_records" => RecoveryBudget::InspectionRecords,
            "inspection_bytes" => RecoveryBudget::InspectionBytes,
            "snapshot_bytes" => RecoveryBudget::SnapshotBytes,
            "replay_operations" => RecoveryBudget::ReplayOperations,
            "artifact_bytes" => RecoveryBudget::ArtifactBytes,
            "stage_records" => RecoveryBudget::StageRecords,
            "stage_bytes" => RecoveryBudget::StageBytes,
            "record_bytes" => RecoveryBudget::RecordBytes,
            "replica_count" => RecoveryBudget::ReplicaCount,
            _ => return None,
        };
        let limit = parts.next()?.parse::<u64>().ok()?;
        let completed = parts.next()?.parse::<u64>().ok()?;
        let requested = parts.next()?.parse::<u64>().ok()?;
        if parts.next().is_some() || completed > limit || requested <= limit - completed {
            return None;
        }
        Some(Self {
            budget,
            limit,
            completed,
            requested,
            unchanged_retry_can_help: false,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn markers_survive_wrappers_but_reject_unknown_or_non_exhausted_work() {
        let text = RecoveryBudgetExceeded::message(RecoveryBudget::InspectionBytes, 100, 90, 20);
        let wrapped = format!("remote error: {text:?}");
        let error = RecoveryBudgetExceeded::from_message(&wrapped).unwrap();
        assert_eq!(error.completed, 90);
        assert_eq!(error.requested, 20);
        assert!(!error.unchanged_retry_can_help);
        for text in [
            "budget exceeded",
            "[recovery-budget-v1:new_kind:1:0:2]",
            "[recovery-budget-v1:stage_bytes:100:90:10]",
            "[recovery-budget-v1:stage_bytes:10:11:1]",
            "[recovery-budget-v1:stage_bytes:1:0:2:3]",
        ] {
            assert!(RecoveryBudgetExceeded::from_message(text).is_none());
        }
    }
}
