//! 严格新建复制的公开结果与失败证据类型测试。
//!
//! 本文件验证纯值接口、所有公开变体、极值、资产所有权和诊断脱敏；真实复制、
//! 原子发布、取消、竞争与暂存清理由 `tests/local_copy_new.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::{
    CopyFailure, CopyOutcome, CopyStagingEvidence, CopyTargetEvidence,
};
use pi_result::ErrorKind;

fn hash_of<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 验证最终目标发布证据的所有变体、0 与 `u64::MAX` 长度，以及 Debug、
// Display、判等、排序和 Hash 的一致性，不把未知发布误写成未发布。
#[test]
fn test_copy_target_evidence_value_traits_cover_every_variant_and_length() {
    assert_send_sync_static::<CopyTargetEvidence>();

    let not_published = CopyTargetEvidence::NotPublishedByOperation;
    let same_not_published = CopyTargetEvidence::NotPublishedByOperation;
    let unknown_empty = CopyTargetEvidence::PublicationUnknown {
        content_byte_len: 0,
    };
    let unknown_large = CopyTargetEvidence::PublicationUnknown {
        content_byte_len: u64::MAX,
    };

    assert_eq!(format!("{not_published:?}"), "NotPublishedByOperation");
    assert_eq!(
        format!("{unknown_large:?}"),
        "PublicationUnknown { content_byte_len: 18446744073709551615 }"
    );
    assert_eq!(
        not_published.to_string(),
        "target not published by operation"
    );
    assert_eq!(
        unknown_empty.to_string(),
        "target publication outcome unknown; complete content length: 0 bytes"
    );

    assert_eq!(not_published, same_not_published);
    assert_ne!(unknown_empty, unknown_large);
    assert!(not_published < unknown_empty);
    assert!(unknown_empty < unknown_large);
    assert_eq!(hash_of(&not_published), hash_of(&same_not_published));
}

// 验证暂存物证据四种互斥状态的值语义和诊断文本，特别保护“未知”与
// “已知仍存在”“本次已删除”之间不可互换的恢复含义。
#[test]
fn test_copy_staging_evidence_value_traits_cover_every_variant() {
    assert_send_sync_static::<CopyStagingEvidence>();

    let not_created = CopyStagingEvidence::NotCreatedByOperation;
    let same_not_created = CopyStagingEvidence::NotCreatedByOperation;
    let removed = CopyStagingEvidence::CreatedThenRemovedByOperation;
    let present = CopyStagingEvidence::KnownPresentAtCompletion;
    let unknown = CopyStagingEvidence::Unknown;

    assert_eq!(format!("{not_created:?}"), "NotCreatedByOperation");
    assert_eq!(format!("{removed:?}"), "CreatedThenRemovedByOperation");
    assert_eq!(format!("{present:?}"), "KnownPresentAtCompletion");
    assert_eq!(format!("{unknown:?}"), "Unknown");
    assert_eq!(
        not_created.to_string(),
        "staging object not created by operation"
    );
    assert_eq!(
        removed.to_string(),
        "staging object created then removed by operation"
    );
    assert_eq!(
        present.to_string(),
        "staging object known present at completion"
    );
    assert_eq!(unknown.to_string(), "staging object outcome unknown");

    assert_eq!(not_created, same_not_created);
    assert!(not_created < removed);
    assert!(removed < present);
    assert!(present < unknown);
    assert_eq!(hash_of(&not_created), hash_of(&same_not_created));
}

// 验证成功结果完整保存逻辑内容长度的 0 与 `u64::MAX` 边界，并保持
// 构造、getter、格式化、排序和 Hash 一致；不推导实际物理传输字节数。
#[test]
fn test_copy_outcome_preserves_full_u64_logical_length() {
    assert_send_sync_static::<CopyOutcome>();

    let empty = CopyOutcome::new(0);
    let same_empty = CopyOutcome::new(0);
    let largest = CopyOutcome::new(u64::MAX);

    assert_eq!(empty.content_byte_len(), 0);
    assert_eq!(largest.content_byte_len(), u64::MAX);
    assert_eq!(format!("{largest:?}"), "CopyOutcome { content_byte_len: 18446744073709551615 }");
    assert_eq!(largest.to_string(), "copied content length: 18446744073709551615 bytes");
    assert_eq!(empty, same_empty);
    assert!(empty < largest);
    assert_eq!(hash_of(&empty), hash_of(&same_empty));
}

// 验证 `CopyFailure` 同时拥有主错误、目标证据和暂存证据，借用访问与
// `into_parts` 均不丢失或串换资产，诊断也不会省略任一恢复维度。
#[test]
fn test_copy_failure_preserves_error_and_both_independent_evidence_values() {
    let failure = CopyFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::AlreadyExists),
        CopyTargetEvidence::NotPublishedByOperation,
        CopyStagingEvidence::CreatedThenRemovedByOperation,
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::CreatedThenRemovedByOperation
    );
    assert_eq!(
        failure.to_string(),
        "already exists; target: target not published by operation; staging: staging object created then removed by operation"
    );
    let debug = format!("{failure:?}");
    assert!(debug.contains("CopyFailure"));
    assert!(debug.contains("already exists"));
    assert!(debug.contains("NotPublishedByOperation"));
    assert!(debug.contains("CreatedThenRemovedByOperation"));

    let (error, target, staging) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(target, CopyTargetEvidence::NotPublishedByOperation);
    assert_eq!(
        staging,
        CopyStagingEvidence::CreatedThenRemovedByOperation
    );
}
