//! 写入传输进度与 `BufferFailure` 公开契约测试。
//!
//! 本文件验证进度确定性、数值边界、原 buffer 所有权归还和诊断脱敏；真实
//! append／overwrite 失败与取消由对应本地文件集成测试覆盖。

use pi_async_fs::{BufferFailure, TransferProgress};
use pi_result::error_stack::Report;
use pi_result::ErrorKind;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// 验证精确、至少和未知三类进度在最小字节数、精确值和不确定谓词上互斥，
// 特别保护精确零与未知零下界不被混同。
#[test]
fn test_transfer_progress_distinguishes_exact_lower_bound_and_unknown() {
    let exact_zero = TransferProgress::Exact { bytes: 0 };
    let at_least = TransferProgress::AtLeast { bytes: 4 };
    let unknown = TransferProgress::Unknown;

    assert_eq!(exact_zero.minimum_bytes(), 0);
    assert_eq!(exact_zero.exact_bytes(), Some(0));
    assert!(exact_zero.is_exact());
    assert!(!exact_zero.is_uncertain());

    assert_eq!(at_least.minimum_bytes(), 4);
    assert_eq!(at_least.exact_bytes(), None);
    assert!(!at_least.is_exact());
    assert!(at_least.is_uncertain());

    assert_eq!(unknown.minimum_bytes(), 0);
    assert_eq!(unknown.exact_bytes(), None);
    assert!(!unknown.is_exact());
    assert!(unknown.is_uncertain());
}

// 验证 Clone、判等与 Hash 同时考虑字节数和确定性，同为 4 字节的 Exact 与
// AtLeast 必须保持不同恢复语义。
#[test]
fn test_transfer_progress_clone_equality_and_hash_include_certainty() {
    let exact = TransferProgress::Exact { bytes: 4 };
    let same = exact.clone();
    let at_least = TransferProgress::AtLeast { bytes: 4 };
    let unknown = TransferProgress::Unknown;

    assert_eq!(exact, same);
    assert_ne!(exact, at_least);
    assert_ne!(at_least, unknown);
    assert_eq!(unknown, unknown.clone());

    let mut exact_hasher = DefaultHasher::new();
    exact.hash(&mut exact_hasher);
    let mut same_hasher = DefaultHasher::new();
    same.hash(&mut same_hasher);
    assert_eq!(exact_hasher.finish(), same_hasher.finish());
}

// 验证全部确定性变体的 Display／Debug 清楚表达“恰好”“至少”或“未知”，
// 供失败日志在不查看 buffer 时可靠解释提交进度。
#[test]
fn test_transfer_progress_formats_all_certainty_variants() {
    let exact = TransferProgress::Exact { bytes: 4 };
    let at_least = TransferProgress::AtLeast { bytes: 4 };
    let unknown = TransferProgress::Unknown;

    assert_eq!(exact.to_string(), "exactly 4 bytes");
    assert_eq!(at_least.to_string(), "at least 4 bytes");
    assert_eq!(unknown.to_string(), "unknown progress");
    assert_eq!(format!("{exact:?}"), "Exact { bytes: 4 }");
    assert_eq!(format!("{at_least:?}"), "AtLeast { bytes: 4 }");
    assert_eq!(format!("{unknown:?}"), "Unknown");
}

// 验证 Exact 与 AtLeast 都能无损保存 `usize::MAX`，且只有 Exact 返回
// `Some(max)`，不执行隐式加一或缩小转换。
#[test]
fn test_transfer_progress_preserves_usize_maximum_boundary() {
    let exact = TransferProgress::Exact { bytes: usize::MAX };
    let at_least = TransferProgress::AtLeast { bytes: usize::MAX };

    assert_eq!(exact.minimum_bytes(), usize::MAX);
    assert_eq!(exact.exact_bytes(), Some(usize::MAX));
    assert_eq!(at_least.minimum_bytes(), usize::MAX);
    assert_eq!(at_least.exact_bytes(), None);
}

// 验证写失败通过借用和 `into_parts` 原样归还 Vec 的内容、指针与容量，同时
// 保留错误和精确进度，证明恢复不要求重复分配。
#[test]
fn test_buffer_failure_returns_the_original_buffer_and_all_failure_facts() {
    let mut buffer = Vec::with_capacity(32);
    buffer.extend_from_slice(b"payload");
    let original_pointer = buffer.as_ptr();
    let original_capacity = buffer.capacity();
    let failure = BufferFailure::new(
        Report::new(ErrorKind::Io),
        buffer,
        TransferProgress::Exact { bytes: 2 },
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::Io);
    assert_eq!(failure.buffer().as_slice(), b"payload");
    assert_eq!(failure.buffer().as_ptr(), original_pointer);
    assert_eq!(failure.buffer().capacity(), original_capacity);
    assert_eq!(failure.progress().exact_bytes(), Some(2));

    let (error, recovered, progress) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Io);
    assert_eq!(recovered.as_slice(), b"payload");
    assert_eq!(recovered.as_ptr(), original_pointer);
    assert_eq!(recovered.capacity(), original_capacity);
    assert_eq!(progress.exact_bytes(), Some(2));
}

// 验证 `BufferFailure` 可承载不实现 Debug 的 buffer，且 Debug／Display
// 只显示类型、错误和进度，绝不泄漏敏感 payload。
#[test]
fn test_buffer_failure_formats_without_requiring_or_exposing_buffer_debug() {
    struct NonDebugSecret(&'static str);

    let failure = BufferFailure::new(
        Report::new(ErrorKind::Io),
        NonDebugSecret("never-log-this-payload"),
        TransferProgress::Exact { bytes: 0 },
    );

    assert_eq!(failure.buffer().0, "never-log-this-payload");
    let debug = format!("{failure:?}");
    let display = failure.to_string();

    assert!(debug.contains("BufferFailure"));
    assert!(debug.contains(std::any::type_name::<NonDebugSecret>()));
    assert!(debug.contains("<redacted>"));
    assert!(debug.contains("Exact"));
    assert!(!debug.contains("never-log-this-payload"));
    assert!(display.contains("io error"));
    assert!(display.contains("exactly 0 bytes"));
    assert!(!display.contains("never-log-this-payload"));
}
