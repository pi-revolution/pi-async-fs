//! 本地文件与空目录删除 API 的真实文件系统集成测试。
//!
//! 测试区分进程内协调删除和显式非协调删除，验证稳定身份别名、资源生命周期、
//! 符号链接、严格错误、非递归目录语义及创建/删除竞争后的可观察状态。

use futures_lite::future::block_on;
use pi_async_fs::{
    CreateTargetEvidence, FileAccessMode, FileIo, FileNamespace,
    LocalFileNamespace, ReadTargetRegion, RemoveTargetEvidence,
};
use pi_result::ErrorKind;

#[test]
// 验证协调文件删除会识别硬链接别名，并等待同一底层文件的全部资源释放。
fn test_safe_remove_file_rejects_every_open_resource_and_hard_link_alias() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let original = temporary.path().join("original");
    let alias = temporary.path().join("alias");
    std::fs::write(&original, b"payload").expect("source file");
    std::fs::hard_link(&original, &alias).expect("hard-link alias");
    let namespace = LocalFileNamespace::new();
    let first = block_on(namespace.open(&original, FileAccessMode::Read))
        .expect("first managed resource");
    let second = block_on(namespace.open(&alias, FileAccessMode::Append))
        .expect("second managed resource through hard link");

    for target in [&original, &alias] {
        let failure = block_on(namespace.remove_file(target))
            .expect_err("any managed resource for the stable file must conflict");
        assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
        assert_eq!(
            failure.target_evidence(),
            &RemoveTargetEvidence::NotRemovedByOperation
        );
        assert!(target.exists());
    }

    drop(first);
    let failure = block_on(namespace.remove_file(&original))
        .expect_err("one remaining hard-link resource still blocks removal");
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    drop(second);

    block_on(namespace.remove_file(&original))
        .expect("removal succeeds after every managed resource is released");
    assert!(!original.exists());
    assert_eq!(std::fs::read(&alias).expect("alias remains"), b"payload");
    block_on(namespace.remove_file(&alias)).expect("last name can be removed");
}

#[test]
// 验证非协调删除只移除目录项，已打开资源仍遵循平台安全的原生生命周期。
fn test_uncoordinated_remove_file_preserves_an_existing_resource_lifetime() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let target = temporary.path().join("target");
    std::fs::write(&target, b"payload").expect("source file");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&target, FileAccessMode::Read))
        .expect("managed resource");

    block_on(namespace.remove_file_uncoordinated(&target))
        .expect("explicitly uncoordinated removal follows native lifetime rules");
    assert!(!target.exists());

    let mut buffer = [0_u8; 7];
    block_on(file.read_exact_at(
        0,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("the already-open resource remains memory-safe and readable");
    assert_eq!(&buffer, b"payload");
}

#[test]
// 验证文件删除严格惰性、缺失不视为幂等成功，并且绝不退化成目录删除。
fn test_remove_file_is_strict_lazy_and_never_switches_to_directory_removal() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("lazy");
    std::fs::write(&target, b"payload").expect("source file");

    let future = namespace.remove_file(&target);
    std::fs::remove_file(&target).expect("external removal before first poll");
    let failure = block_on(future).expect_err("missing is not idempotent success");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );

    let directory = temporary.path().join("directory");
    std::fs::create_dir(&directory).expect("directory target");
    let failure = block_on(namespace.remove_file(&directory))
        .expect_err("file removal must not delete a directory");
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );
    assert!(directory.is_dir());
}

#[cfg(unix)]
#[test]
// 验证文件删除处理最终符号链接本身，不跟随或干扰其目标资源。
fn test_remove_file_removes_final_symlink_itself_without_following_target() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("target");
    let link = temporary.path().join("link");
    let dangling = temporary.path().join("dangling");
    std::fs::write(&target, b"payload").expect("target file");
    symlink(&target, &link).expect("file symlink");
    symlink(temporary.path().join("absent"), &dangling)
        .expect("dangling symlink");
    let target_resource = block_on(namespace.open(&target, FileAccessMode::Read))
        .expect("target resource");

    block_on(namespace.remove_file(&link))
        .expect("removing a link is independent from its target resource");
    block_on(namespace.remove_file_uncoordinated(&dangling))
        .expect("a dangling final link is still a removable entry");
    assert!(target.exists());
    assert!(std::fs::symlink_metadata(&link).is_err());
    assert!(std::fs::symlink_metadata(&dangling).is_err());
    drop(target_resource);
}

#[test]
// 验证目录删除仅接受空目录、重复调用严格报错，且两种入口均不递归。
fn test_remove_dir_only_removes_empty_directories_and_is_strict() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let empty = temporary.path().join("empty");
    std::fs::create_dir(&empty).expect("empty directory");

    block_on(namespace.remove_dir(&empty)).expect("safe empty-directory removal");
    assert!(!empty.exists());
    let failure = block_on(namespace.remove_dir(&empty))
        .expect_err("repeated removal is a strict not-found failure");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );

    let nonempty = temporary.path().join("nonempty");
    std::fs::create_dir(&nonempty).expect("nonempty directory");
    std::fs::write(nonempty.join("child"), b"preserve").expect("child file");
    let failure = block_on(namespace.remove_dir(&nonempty))
        .expect_err("safe removal must never recurse");
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );
    assert_eq!(std::fs::read(nonempty.join("child")).expect("child remains"), b"preserve");

    let failure = block_on(namespace.remove_dir_uncoordinated(&nonempty))
        .expect_err("uncoordinated removal still must never recurse");
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation
    );
    assert!(nonempty.join("child").is_file());
}

#[cfg(unix)]
#[test]
// 验证目录删除拒绝最终符号链接，并完整保留链接及其目标目录。
fn test_remove_dir_rejects_final_symlinks_without_deleting_either_object() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("target");
    let link = temporary.path().join("link");
    std::fs::create_dir(&target).expect("target directory");
    symlink(&target, &link).expect("directory symlink");

    for coordinated in [true, false] {
        let failure = if coordinated {
            block_on(namespace.remove_dir(&link))
        } else {
            block_on(namespace.remove_dir_uncoordinated(&link))
        }
        .expect_err("directory removal must not follow a final symlink");
        assert_eq!(
            failure.target_evidence(),
            &RemoveTargetEvidence::NotRemovedByOperation
        );
        assert!(target.is_dir());
        assert!(std::fs::symlink_metadata(&link)
            .expect("link remains")
            .file_type()
            .is_symlink());
    }
}

#[test]
// 验证子项已解除链接后，其存活文件资源不再阻止父空目录删除。
fn test_remove_dir_does_not_conflict_with_an_unlinked_child_file_resource() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let directory = temporary.path().join("directory");
    let child = directory.join("child");
    std::fs::create_dir(&directory).expect("directory");
    std::fs::write(&child, b"payload").expect("child file");
    let child_resource = block_on(namespace.open(&child, FileAccessMode::Read))
        .expect("managed child resource");

    block_on(namespace.remove_file_uncoordinated(&child))
        .expect("unlink child while its native object remains open");
    block_on(namespace.remove_dir(&directory))
        .expect("an unlinked child object is no longer a directory entry");

    let mut buffer = [0_u8; 7];
    block_on(child_resource.read_exact_at(
        0,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("child resource remains valid after parent name removal");
    assert_eq!(&buffer, b"payload");
}

#[test]
// 压力验证同一父目录上的删除与子项创建不能同时以互相矛盾的次序提交。
fn test_remove_dir_and_child_creation_never_both_commit_out_of_order() {
    let temporary = tempfile::tempdir().expect("temporary directory");

    for iteration in 0..32 {
        let parent = temporary.path().join(format!("parent-{iteration}"));
        let child = parent.join("child");
        std::fs::create_dir(&parent).expect("fresh parent");
        let namespace = LocalFileNamespace::new();
        let remove_namespace = namespace.clone();
        let create_namespace = namespace.clone();
        let remove_parent = parent.clone();
        let create_child = child.clone();

        let (remove_result, create_result) = block_on(async move {
            let remove = async_global_executor::spawn(async move {
                remove_namespace.remove_dir(&remove_parent).await
            });
            let create = async_global_executor::spawn(async move {
                create_namespace.create_dir(&create_child).await
            });
            (remove.await, create.await)
        });

        assert!(
            remove_result.is_err() || create_result.is_err(),
            "an empty-directory removal and child creation cannot both commit"
        );
        if let Err(failure) = &remove_result {
            assert_eq!(
                failure.target_evidence(),
                &RemoveTargetEvidence::NotRemovedByOperation
            );
        }
        if let Err(failure) = &create_result {
            assert_eq!(
                failure.target_evidence(),
                &CreateTargetEvidence::NotCreatedByOperation
            );
        }

        match (parent.exists(), child.exists()) {
            (false, false) => assert!(remove_result.is_ok()),
            (true, true) => assert!(create_result.is_ok()),
            state => panic!("invalid namespace state after race: {state:?}"),
        }
    }
}
