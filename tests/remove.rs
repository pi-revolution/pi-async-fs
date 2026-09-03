//! 删除目标证据与失败载体的公开值契约测试。
//!
//! 本文件只验证无 I/O 的证据分类、资产所有权和诊断；文件／空目录安全删除、
//! 非协调删除及并发边界由 `tests/local_remove.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::{RemoveFailure, RemoveTargetEvidence};
use pi_result::ErrorKind;

// 用标准哈希器验证相等删除证据必须产生一致哈希。
fn hash_of(value: &RemoveTargetEvidence) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 验证删除目标证据全部变体的格式、判等、排序和 Hash，保护“本次未删除”、
// “本次已删除”与“结果未知”三种恢复含义互不混淆。
#[test]
fn test_remove_target_evidence_value_traits_cover_every_variant() {
    assert_send_sync_static::<RemoveTargetEvidence>();

    let not_removed = RemoveTargetEvidence::NotRemovedByOperation;
    let same_not_removed = RemoveTargetEvidence::NotRemovedByOperation;
    let removed = RemoveTargetEvidence::RemovedByOperation;
    let unknown = RemoveTargetEvidence::Unknown;

    assert_eq!(format!("{not_removed:?}"), "NotRemovedByOperation");
    assert_eq!(format!("{removed:?}"), "RemovedByOperation");
    assert_eq!(format!("{unknown:?}"), "Unknown");
    assert_eq!(not_removed.to_string(), "target not removed by operation");
    assert_eq!(removed.to_string(), "target removed by operation");
    assert_eq!(unknown.to_string(), "target removal outcome unknown");

    assert_eq!(not_removed, same_not_removed);
    assert_ne!(not_removed, removed);
    assert!(not_removed < removed);
    assert!(removed < unknown);
    assert_eq!(hash_of(&not_removed), hash_of(&same_not_removed));
}

// 验证 `RemoveFailure` 同时保存统一错误和目标状态证据，借用访问、消费拆解
// 与诊断输出均不丢失信息或改变错误分类。
#[test]
fn test_remove_failure_preserves_error_and_target_evidence() {
    let failure = RemoveFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::Conflict),
        RemoveTargetEvidence::NotRemovedByOperation,
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );
    assert_eq!(
        failure.to_string(),
        "conflict; target: target not removed by operation"
    );
    let debug = format!("{failure:?}");
    assert!(debug.contains("RemoveFailure"));
    assert!(debug.contains("conflict"));
    assert!(debug.contains("NotRemovedByOperation"));

    let (error, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(evidence, RemoveTargetEvidence::NotRemovedByOperation);
}
