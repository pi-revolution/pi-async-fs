//! 创建操作的公开成功／失败载体与证据类型契约测试。
//!
//! 本文件只验证纯值、资产所有权、脱敏格式和顺序保持；本地文件／目录真实
//! 创建、并发准入和失败副作用由 `tests/local_create_*.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::{
    CreateDirectoriesFailure, CreateFailure, CreateTargetEvidence,
    CrossProcessCreateSuccess,
};
use pi_result::ErrorKind;

// 用标准哈希器验证相等创建证据必须产生一致哈希。
fn hash_of(value: &CreateTargetEvidence) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 验证创建目标证据的全部变体及其 Debug、Display、判等、排序和 Hash，
// 保证“未创建”“已创建”“未知”不会在错误恢复中被混同。
#[test]
fn test_create_target_evidence_value_traits_cover_every_variant() {
    assert_send_sync_static::<CreateTargetEvidence>();

    let not_created = CreateTargetEvidence::NotCreatedByOperation;
    let same_not_created = CreateTargetEvidence::NotCreatedByOperation;
    let created = CreateTargetEvidence::CreatedByOperation;
    let unknown = CreateTargetEvidence::Unknown;

    assert_eq!(format!("{not_created:?}"), "NotCreatedByOperation");
    assert_eq!(format!("{created:?}"), "CreatedByOperation");
    assert_eq!(format!("{unknown:?}"), "Unknown");
    assert_eq!(not_created.to_string(), "target not created by operation");
    assert_eq!(created.to_string(), "target created by operation");
    assert_eq!(unknown.to_string(), "target creation outcome unknown");

    assert_eq!(not_created, same_not_created);
    assert_ne!(not_created, created);
    assert!(not_created < created);
    assert!(created < unknown);
    assert_eq!(hash_of(&not_created), hash_of(&same_not_created));
}

// 验证 `CreateFailure` 对统一错误和目标证据的共享访问、消费拆解与诊断输出
// 完整一致，且不会在格式化时改变错误分类或所有权。
#[test]
fn test_create_failure_preserves_error_and_target_evidence() {
    let failure = CreateFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::AlreadyExists),
        CreateTargetEvidence::NotCreatedByOperation,
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert_eq!(
        failure.to_string(),
        "already exists; target: target not created by operation"
    );
    let debug = format!("{failure:?}");
    assert!(debug.contains("CreateFailure"));
    assert!(debug.contains("already exists"), "{debug}");
    assert!(debug.contains("NotCreatedByOperation"));

    let (error, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(evidence, CreateTargetEvidence::NotCreatedByOperation);
}

// 验证递归创建失败保留“确定已创建”和“状态不确定”定位符的原始顺序及
// 所有权，同时只公开计数而不要求或泄漏定位符的 Debug 内容。
#[test]
fn test_create_directories_failure_preserves_ordered_assets_without_formatting_them() {
    struct NonDebugLocator(&'static str);

    let failure = CreateDirectoriesFailure::new(
        pi_result::error_stack::Report::new(ErrorKind::PermissionDenied),
        vec![NonDebugLocator("secret-parent"), NonDebugLocator("secret-child")],
        vec![NonDebugLocator("secret-uncertain")],
    );

    assert_eq!(failure.error().current_context(), &ErrorKind::PermissionDenied);
    assert_eq!(failure.confirmed_created().len(), 2);
    assert_eq!(failure.confirmed_created()[0].0, "secret-parent");
    assert_eq!(failure.confirmed_created()[1].0, "secret-child");
    assert_eq!(failure.uncertain_targets().len(), 1);
    assert_eq!(failure.uncertain_targets()[0].0, "secret-uncertain");

    let debug = format!("{failure:?}");
    let display = failure.to_string();
    assert!(debug.contains("CreateDirectoriesFailure"));
    assert!(debug.contains("permission denied"), "{debug}");
    assert!(debug.contains("confirmed_created_count: 2"));
    assert!(debug.contains("uncertain_targets_count: 1"));
    assert!(!debug.contains("secret-"));
    assert_eq!(
        display,
        "permission denied; confirmed created: 2; uncertain targets: 1"
    );
    assert!(!display.contains("secret-"));

    let (error, confirmed, uncertain) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::PermissionDenied);
    assert_eq!(
        confirmed.into_iter().map(|locator| locator.0).collect::<Vec<_>>(),
        ["secret-parent", "secret-child"]
    );
    assert_eq!(
        uncertain.into_iter().map(|locator| locator.0).collect::<Vec<_>>(),
        ["secret-uncertain"]
    );
}

// 验证协调创建成功载体可分别借用 authority、可变访问文件资源并最终一次性
// 取回两项非 Debug 资产，格式化不得泄漏任一内部值。
#[test]
fn test_cross_process_create_success_preserves_both_non_debug_assets() {
    struct Authority(u32);
    struct FileResource(Vec<u8>);

    let mut success = CrossProcessCreateSuccess::new(
        Authority(7),
        FileResource(vec![1, 2, 3]),
    );

    assert_eq!(success.authority().0, 7);
    assert_eq!(success.file().0, [1, 2, 3]);
    success.file_mut().0.push(4);
    assert_eq!(success.file().0, [1, 2, 3, 4]);
    assert_eq!(
        format!("{success:?}"),
        "CrossProcessCreateSuccess { authority: <present>, file: <present> }"
    );
    assert_eq!(
        success.to_string(),
        "cross-process authority and file resource are present"
    );

    let (authority, file) = success.into_parts();
    assert_eq!(authority.0, 7);
    assert_eq!(file.0, [1, 2, 3, 4]);
}
