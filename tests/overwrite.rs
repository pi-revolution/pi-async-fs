//! 覆盖写目标证据与失败资产的公开值契约测试。
//!
//! 本文件验证进度／目标证据、原 buffer 归还和诊断脱敏；真实覆盖写的原子性、
//! 取消、角色和文件尾处理由 `tests/local_file_overwrite.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::{
    OverwriteFailure, OverwriteTargetEvidence, TransferProgress,
};
use pi_result::ErrorKind;

// 验证覆盖写目标证据所有变体及精确前缀长度的值语义，保证排序、Hash 和
// 格式化不会把“修改已开始”或“未知”误报为目标未改变。
#[test]
fn test_overwrite_target_evidence_value_traits_cover_every_variant() {
    let unchanged = OverwriteTargetEvidence::Unchanged;
    let prefix_two = OverwriteTargetEvidence::ExactInputPrefix { bytes: 2 };
    let same_prefix = OverwriteTargetEvidence::ExactInputPrefix { bytes: 2 };
    let prefix_three = OverwriteTargetEvidence::ExactInputPrefix { bytes: 3 };
    let mutation = OverwriteTargetEvidence::MutationStarted;
    let unknown = OverwriteTargetEvidence::Unknown;

    assert_eq!(unchanged.to_string(), "target unchanged");
    assert_eq!(prefix_two.to_string(), "target is exact input prefix of 2 bytes");
    assert_eq!(mutation.to_string(), "target mutation started");
    assert_eq!(unknown.to_string(), "target state unknown");
    assert_eq!(format!("{unchanged:?}"), "Unchanged");
    assert_eq!(
        format!("{prefix_two:?}"),
        "ExactInputPrefix { bytes: 2 }"
    );
    assert_eq!(format!("{mutation:?}"), "MutationStarted");
    assert_eq!(format!("{unknown:?}"), "Unknown");

    assert_eq!(prefix_two, same_prefix);
    assert!(unchanged < prefix_two);
    assert!(prefix_two < prefix_three);
    assert!(prefix_three < mutation);
    assert!(mutation < unknown);

    let mut left = DefaultHasher::new();
    prefix_two.hash(&mut left);
    let mut right = DefaultHasher::new();
    same_prefix.hash(&mut right);
    assert_eq!(left.finish(), right.finish());
}

// 验证覆盖失败原样归还 Vec 的指针、容量和内容，同时保留错误、传输进度与
// 目标证据；Debug／Display 必须对敏感 buffer 内容保持脱敏。
#[test]
fn test_overwrite_failure_preserves_all_assets_and_redacts_buffer() {
    let mut buffer = Vec::with_capacity(32);
    buffer.extend_from_slice(b"SECRET-CONTENT");
    let pointer = buffer.as_ptr();
    let capacity = buffer.capacity();
    let failure = OverwriteFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::Conflict),
        buffer,
        TransferProgress::Exact { bytes: 3 },
        OverwriteTargetEvidence::MutationStarted,
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(failure.buffer(), b"SECRET-CONTENT");
    assert_eq!(failure.progress(), &TransferProgress::Exact { bytes: 3 });
    assert_eq!(
        failure.target_evidence(),
        &OverwriteTargetEvidence::MutationStarted
    );
    let debug = format!("{failure:?}");
    let display = failure.to_string();
    assert!(!debug.contains("SECRET-CONTENT"));
    assert!(!display.contains("SECRET-CONTENT"));

    let (error, buffer, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(buffer.as_ptr(), pointer);
    assert_eq!(buffer.capacity(), capacity);
    assert_eq!(buffer, b"SECRET-CONTENT");
    assert_eq!(progress, TransferProgress::Exact { bytes: 3 });
    assert_eq!(evidence, OverwriteTargetEvidence::MutationStarted);
}
