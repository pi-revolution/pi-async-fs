//! `LocalFileNamespace::create_new` 的真实文件创建集成测试。
//!
//! 覆盖全部访问角色、懒执行、严格不替换、父目录、别名、32 路并发以及返回
//! 资源立即参与协调。并发用例必须串行独占测试进程资源运行。

use futures_lite::future::block_on;
use pi_async_fs::{
    CreateTargetEvidence, FileAccessMode, FileIo, FileNamespace,
    LocalFileNamespace, RemoveTargetEvidence,
};
use pi_result::ErrorKind;

fn expect_create_failure<T>(
    result: Result<T, pi_async_fs::CreateFailure>,
    message: &str,
) -> pi_async_fs::CreateFailure {
    match result {
        Ok(_) => panic!("{message}"),
        Err(failure) => failure,
    }
}

// 验证六种冻结访问角色均可原子创建空文件且不执行初始内容 I/O；返回资源与
// 路径元信息都报告长度零，证明角色选择不改变创建内容。
#[test]
fn test_create_new_supports_every_frozen_access_role_without_initial_io() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let modes = [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ];

    for (index, mode) in modes.into_iter().enumerate() {
        let target = temporary.path().join(format!("file-{index}"));
        let file = block_on(namespace.create_new(&target, mode))
            .expect("every local access role can own a newly created empty file");
        assert!(target.is_file());
        assert_eq!(std::fs::metadata(&target).expect("metadata").len(), 0);
        assert_eq!(block_on(file.byte_len()).expect("resource length"), 0);
    }
}

// 验证 Future 懒执行及严格不替换：现有文件、目录、有效／悬空最终链接全部
// 返回未创建证据，并保留原类型与内容。
#[test]
fn test_create_new_is_lazy_strict_and_preserves_every_existing_entry() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let file = temporary.path().join("file");
    let directory = temporary.path().join("directory");

    let future = namespace.create_new(&file, FileAccessMode::Overwrite);
    std::fs::write(&file, b"preserve").expect("external creation before poll");
    let failure = expect_create_failure(
        block_on(future),
        "existing file must be rejected",
    );
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert_eq!(std::fs::read(&file).expect("file remains"), b"preserve");

    std::fs::create_dir(&directory).expect("existing directory");
    let failure = expect_create_failure(
        block_on(namespace.create_new(&directory, FileAccessMode::Read)),
        "existing directory must be rejected",
    );
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert!(directory.is_dir());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let link = temporary.path().join("link");
        let dangling = temporary.path().join("dangling");
        symlink(&file, &link).expect("file symlink");
        symlink(temporary.path().join("absent"), &dangling)
            .expect("dangling symlink");
        for target in [&link, &dangling] {
            let failure = expect_create_failure(
                block_on(namespace.create_new(target, FileAccessMode::Append)),
                "a final symlink is an existing namespace entry",
            );
            assert_eq!(
                failure.error().current_context(),
                &ErrorKind::AlreadyExists
            );
            assert_eq!(
                failure.target_evidence(),
                &CreateTargetEvidence::NotCreatedByOperation
            );
            assert!(std::fs::symlink_metadata(target)
                .expect("link remains")
                .file_type()
                .is_symlink());
        }
    }
}

// 验证文件创建不会隐式创建父目录；缺失父级返回 NotFound + 未创建证据，
// 父目录和最终文件在失败后都不存在。
#[test]
fn test_create_new_does_not_create_missing_parents() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let parent = temporary.path().join("missing");
    let target = parent.join("file");

    let failure = expect_create_failure(
        block_on(namespace.create_new(&target, FileAccessMode::Append)),
        "file creation must not create its parent hierarchy",
    );
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation
    );
    assert!(!parent.exists());
}

// 验证同一 namespace 的 32 个克隆并发创建同一路径时恰有一个成功，其余
// 只可能是已存在／冲突且都报告本次未创建，最终只有一个常规文件。
#[test]
fn test_create_new_has_exactly_one_winner_across_namespace_clones() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let target = temporary.path().join("shared");
    let namespace = LocalFileNamespace::new();

    let results = block_on(async move {
        let tasks = (0..32)
            .map(|_| {
                let namespace = namespace.clone();
                let target = target.clone();
                async_global_executor::spawn(async move {
                    namespace
                        .create_new(&target, FileAccessMode::Append)
                        .await
                })
            })
            .collect::<Vec<_>>();
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await);
        }
        (target, results)
    });

    assert!(results.0.is_file());
    assert_eq!(results.1.iter().filter(|result| result.is_ok()).count(), 1);
    for failure in results.1.into_iter().filter_map(Result::err) {
        assert!(matches!(
            failure.error().current_context(),
            ErrorKind::AlreadyExists | ErrorKind::Conflict
        ));
        assert_eq!(
            failure.target_evidence(),
            &CreateTargetEvidence::NotCreatedByOperation
        );
    }
}

// 验证 Unix 真实父目录与其符号链接别名会收敛到同一目录条目：两个并发创建
// 恰有一个成功，不因路径拼写不同产生两个资源身份。
#[cfg(unix)]
#[test]
fn test_create_new_converges_parent_symlink_aliases_to_one_target() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let real_parent = temporary.path().join("real");
    let alias_parent = temporary.path().join("alias");
    std::fs::create_dir(&real_parent).expect("real parent");
    symlink(&real_parent, &alias_parent).expect("parent symlink");
    let targets = [real_parent.join("file"), alias_parent.join("file")];

    let namespace = LocalFileNamespace::new();
    let first_namespace = namespace.clone();
    let second_namespace = namespace.clone();
    let first_target = targets[0].clone();
    let second_target = targets[1].clone();
    let (first, second) = block_on(async move {
        let first = async_global_executor::spawn(async move {
            first_namespace
                .create_new(&first_target, FileAccessMode::Read)
                .await
        });
        let second = async_global_executor::spawn(async move {
            second_namespace
                .create_new(&second_target, FileAccessMode::Read)
                .await
        });
        (first.await, second.await)
    });

    assert_eq!([first.is_ok(), second.is_ok()].into_iter().filter(|value| *value).count(), 1);
    assert!(real_parent.join("file").is_file());
}

// 验证 `create_new` 返回前新文件资源已经登记：资源存活时安全删除冲突且无
// 删除副作用，释放后相同删除立即成功。
#[test]
fn test_create_new_resource_immediately_participates_in_safe_removal() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let target = temporary.path().join("target");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.create_new(
        &target,
        FileAccessMode::Read,
    ))
    .expect("new file resource");

    let failure = block_on(namespace.remove_file(&target))
        .expect_err("the returned live resource must already be coordinated");
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );
    drop(file);
    block_on(namespace.remove_file(&target))
        .expect("removal succeeds after the created resource is released");
}
