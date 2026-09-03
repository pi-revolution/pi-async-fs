//! 本地目录严格创建与递归创建的真实集成测试。
//!
//! 覆盖懒执行、现有条目、父目录缺失、部分成功证据、符号链接别名、幂等性和
//! 同进程并发。并发用例共享调度资源，完整文件必须串行运行以稳定证据。

use std::path::PathBuf;

use pi_async_fs::{
    CreateTargetEvidence, FileNamespace, LocalFileNamespace,
};
use pi_result::ErrorKind;

// 验证 `create_dir` 在首次 poll 前没有副作用，并在外部抢先创建同名目录后
// 严格失败为 AlreadyExists + NotCreatedByOperation，且不修改赢家。
#[test]
fn test_create_dir_is_lazy_and_strictly_rejects_existing_targets() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("lazy");

    let future = namespace.create_dir(&target);
    std::fs::create_dir(&target).expect("external creation before first poll");
    let failure = async_global_executor::block_on(future)
        .expect_err("an existing target must be rejected");

    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert!(target.is_dir());
}

// 验证严格目录创建只建立最终一级；父目录不存在时返回 NotFound、报告未创建，
// 并证明没有隐式生成任何祖先目录。
#[test]
fn test_create_dir_creates_only_one_level_and_reports_precommit_failures() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();

    let target = temporary.path().join("single");
    async_global_executor::block_on(namespace.create_dir(&target))
        .expect("single directory creation");
    assert!(target.is_dir());

    let nested = temporary.path().join("missing-parent").join("child");
    let failure = async_global_executor::block_on(namespace.create_dir(&nested))
        .expect_err("strict creation must not create an ancestor");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert!(!temporary.path().join("missing-parent").exists());
}

// 验证现有目录、常规文件、有效与悬空最终符号链接均作为已存在条目被拒绝，
// 所有原对象类型和文件内容保持不变。
#[test]
fn test_create_dir_rejects_every_existing_final_entry_without_modifying_it() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let directory = temporary.path().join("directory");
    let file = temporary.path().join("file");
    std::fs::create_dir(&directory).expect("existing directory");
    std::fs::write(&file, b"preserve").expect("existing file");

    for target in [&directory, &file] {
        let failure = async_global_executor::block_on(
            namespace.create_dir(target),
        )
        .expect_err("every existing target kind must be rejected");
        assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
        assert_eq!(
            failure.target_evidence(),
            &CreateTargetEvidence::NotCreatedByOperation
        );
    }
    assert_eq!(std::fs::read(&file).expect("preserved file"), b"preserve");

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let link = temporary.path().join("link");
        let dangling = temporary.path().join("dangling");
        symlink(&directory, &link).expect("directory symlink");
        symlink(temporary.path().join("absent"), &dangling)
            .expect("dangling symlink");

        for target in [&link, &dangling] {
            let failure = async_global_executor::block_on(
                namespace.create_dir(target),
            )
            .expect_err("a final symlink is an existing entry");
            assert_eq!(
                failure.error().current_context(),
                &ErrorKind::AlreadyExists
            );
            assert_eq!(
                failure.target_evidence(),
                &CreateTargetEvidence::NotCreatedByOperation
            );
        }
        assert!(std::fs::symlink_metadata(&link)
            .expect("link remains")
            .file_type()
            .is_symlink());
        assert!(std::fs::symlink_metadata(&dangling)
            .expect("dangling link remains")
            .file_type()
            .is_symlink());
    }
}

// 验证不同 namespace 句柄和父目录符号链接别名并发指向同一条目时恰有一个
// 创建成功，其余只报告冲突／已存在且最终只有一个真实目录。
#[test]
fn test_create_dir_converges_across_clones_and_parent_aliases() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let real_parent = temporary.path().join("real");
    std::fs::create_dir(&real_parent).expect("real parent");

    #[cfg(unix)]
    let targets = {
        use std::os::unix::fs::symlink;

        let alias = temporary.path().join("alias");
        symlink(&real_parent, &alias).expect("parent alias");
        vec![real_parent.join("child"), alias.join("child")]
    };
    #[cfg(not(unix))]
    let targets = vec![real_parent.join("child"), real_parent.join("child")];

    let namespace = LocalFileNamespace::new();
    let results = async_global_executor::block_on(async move {
        let tasks = targets
            .into_iter()
            .enumerate()
            .map(|(index, target)| {
                let namespace = if index == 0 {
                    namespace.clone()
                } else {
                    LocalFileNamespace::new()
                };
                async_global_executor::spawn(async move {
                    namespace.create_dir(&target).await
                })
            })
            .collect::<Vec<_>>();

        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await);
        }
        results
    });

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    for failure in results.into_iter().filter_map(Result::err) {
        assert!(matches!(
            failure.error().current_context(),
            ErrorKind::AlreadyExists | ErrorKind::Conflict
        ));
        assert_eq!(
            failure.target_evidence(),
            &CreateTargetEvidence::NotCreatedByOperation
        );
    }
    assert!(real_parent.join("child").is_dir());
}

// 验证 `create_dir_all` 懒执行、包含最终目标、重复调用幂等，并明确空本地路径
// 表示已有相对基点而不是创建匿名目录。
#[test]
fn test_create_dir_all_is_lazy_idempotent_and_obeys_inclusive_target() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("one").join("two").join("three");

    let future = namespace.create_dir_all(&target);
    assert!(!temporary.path().join("one").exists());
    async_global_executor::block_on(future).expect("recursive creation");
    assert!(target.is_dir());

    async_global_executor::block_on(namespace.create_dir_all(&target))
        .expect("an existing directory tree is an idempotent success");
    async_global_executor::block_on(
        namespace.create_dir_all(&PathBuf::new()),
    )
    .expect("an empty local path denotes the existing relative base");
}

// 验证递归创建遇到中间常规文件时不回滚已经确认创建的祖先，失败资产按顺序
// 精确列出已创建目录，阻塞文件及其内容保持不变。
#[test]
fn test_create_dir_all_reports_confirmed_partial_ancestors_without_rollback() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let blocker = temporary.path().join("blocker");
    std::fs::write(&blocker, b"preserve").expect("blocking file");

    let created = temporary.path().join("created");
    let target = created.join("..").join("blocker").join("child");
    let failure = async_global_executor::block_on(
        namespace.create_dir_all(&target),
    )
    .expect_err("a file in the requested hierarchy must stop creation");

    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(failure.confirmed_created(), std::slice::from_ref(&created));
    assert!(failure.uncertain_targets().is_empty());
    assert!(created.is_dir());
    assert_eq!(std::fs::read(&blocker).expect("blocker remains"), b"preserve");
    assert!(!blocker.join("child").exists());
}

// 验证最终常规文件不能满足目录语义；Unix 上路径中的目录符号链接按正常解析
// 继续创建后代，同时保留链接本身。
#[test]
fn test_create_dir_all_accepts_directory_symlinks_but_rejects_other_objects() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let file = temporary.path().join("file");
    std::fs::write(&file, b"preserve").expect("blocking file");

    let failure = async_global_executor::block_on(
        namespace.create_dir_all(&file),
    )
    .expect_err("a final regular file cannot satisfy directory semantics");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert!(failure.confirmed_created().is_empty());
    assert!(failure.uncertain_targets().is_empty());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let real = temporary.path().join("real");
        let link = temporary.path().join("link");
        std::fs::create_dir(&real).expect("real directory");
        symlink(&real, &link).expect("directory symlink");
        let child = link.join("child");
        async_global_executor::block_on(namespace.create_dir_all(&child))
            .expect("an existing directory symlink follows normal path rules");
        assert!(real.join("child").is_dir());
        assert!(std::fs::symlink_metadata(&link)
            .expect("link remains")
            .file_type()
            .is_symlink());
    }
}

// 验证 16 个并发递归创建最终收敛为完整目录树：至少一项成功，其余活动预留
// 冲突均不报告不确定目标，全部结束后重试为幂等成功。
#[test]
fn test_create_dir_all_concurrent_calls_converge_without_hiding_active_conflicts() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let target = temporary.path().join("shared").join("deep").join("leaf");
    let namespace = LocalFileNamespace::new();

    let results = async_global_executor::block_on(async move {
        let tasks = (0..16)
            .map(|_| {
                let namespace = namespace.clone();
                let target = target.clone();
                async_global_executor::spawn(async move {
                    namespace.create_dir_all(&target).await
                })
            })
            .collect::<Vec<_>>();
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await);
        }
        (target, results)
    });

    let target = results.0;
    assert!(target.is_dir());
    assert!(results.1.iter().any(Result::is_ok));
    for failure in results.1.into_iter().filter_map(Result::err) {
        assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
        assert!(failure.uncertain_targets().is_empty());
        for created in failure.confirmed_created() {
            assert!(created.is_dir());
            assert!(target.starts_with(created));
        }
    }
    async_global_executor::block_on(
        LocalFileNamespace::new().create_dir_all(&target),
    )
    .expect("retry after all active reservations are released is idempotent");
}
