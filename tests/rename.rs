//! 改名提交证据与失败载体的公开值契约测试。
//!
//! 本文件不操作文件系统，只验证错误恢复所需的证据、所有权和诊断；真实原子
//! 不替换、并发和平台边界由 `tests/local_rename.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::{RenameCommitEvidence, RenameFailure};
use pi_result::ErrorKind;

// 用标准哈希器验证相等改名提交证据必须产生一致哈希。
fn hash_of(value: &RenameCommitEvidence) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 验证改名提交证据所有变体在 Debug、Display、判等、排序和 Hash 上一致，
// 并确保“未知”不会被降级成“本次未改名”。
#[test]
fn test_rename_commit_evidence_value_traits_cover_every_variant() {
    assert_send_sync_static::<RenameCommitEvidence>();

    let not_renamed = RenameCommitEvidence::NotRenamedByOperation;
    let same_not_renamed = RenameCommitEvidence::NotRenamedByOperation;
    let renamed = RenameCommitEvidence::RenamedByOperation;
    let unknown = RenameCommitEvidence::Unknown;

    assert_eq!(format!("{not_renamed:?}"), "NotRenamedByOperation");
    assert_eq!(format!("{renamed:?}"), "RenamedByOperation");
    assert_eq!(format!("{unknown:?}"), "Unknown");
    assert_eq!(not_renamed.to_string(), "rename not committed by operation");
    assert_eq!(renamed.to_string(), "rename committed by operation");
    assert_eq!(unknown.to_string(), "rename commit outcome unknown");

    assert_eq!(not_renamed, same_not_renamed);
    assert_ne!(not_renamed, renamed);
    assert!(not_renamed < renamed);
    assert!(renamed < unknown);
    assert_eq!(hash_of(&not_renamed), hash_of(&same_not_renamed));
}

// 验证 `RenameFailure` 完整保留错误与提交证据，通过借用和 `into_parts`
// 读取时分类不变，诊断同时呈现两项事实而不制造额外提交结论。
#[test]
fn test_rename_failure_preserves_error_and_commit_evidence() {
    let failure = RenameFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::AlreadyExists),
        RenameCommitEvidence::NotRenamedByOperation,
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation
    );
    assert_eq!(
        failure.to_string(),
        "already exists; commit: rename not committed by operation"
    );
    let debug = format!("{failure:?}");
    assert!(debug.contains("RenameFailure"));
    assert!(debug.contains("already exists"));
    assert!(debug.contains("NotRenamedByOperation"));

    let (error, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(evidence, RenameCommitEvidence::NotRenamedByOperation);
}
