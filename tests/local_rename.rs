//! `LocalFileNamespace::rename` 的真实文件系统集成测试。
//!
//! 本文件验证严格“不替换”发布、惰性、符号链接、目录树、并发竞争、
//! 已打开资源的对象身份以及跨文件系统失败边界；不把平台原生失败伪装成复制。

use futures_lite::future::block_on;
use pi_async_fs::{FileNamespace, LocalFileNamespace, RenameCommitEvidence};
#[cfg(target_os = "linux")]
use pi_async_fs::{FileAccessMode, FileIo, MmapRange, ReadTargetRegion};
use pi_result::ErrorKind;

#[test]
// 验证文件与非空目录树均可移动，并且源目录项在成功后消失。
fn test_rename_moves_regular_files_and_nonempty_directories_without_replacement() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();

    let source_file = temporary.path().join("source-file");
    let destination_file = temporary.path().join("destination-file");
    std::fs::write(&source_file, b"payload").expect("source file");
    block_on(namespace.rename(&source_file, &destination_file))
        .expect("strict file rename");
    assert!(!source_file.exists());
    assert_eq!(
        std::fs::read(&destination_file).expect("destination content"),
        b"payload"
    );

    let source_directory = temporary.path().join("source-directory");
    let destination_directory = temporary.path().join("destination-directory");
    std::fs::create_dir(&source_directory).expect("source directory");
    std::fs::write(source_directory.join("child"), b"child")
        .expect("directory child");
    block_on(namespace.rename(&source_directory, &destination_directory))
        .expect("nonempty directory is renamed as one namespace subtree");
    assert!(!source_directory.exists());
    assert_eq!(
        std::fs::read(destination_directory.join("child"))
            .expect("renamed child"),
        b"child"
    );
}

#[test]
// 验证任意已存在目标都被拒绝，且失败证据与源、目标内容保持一致。
fn test_rename_rejects_every_existing_destination_and_preserves_both_sides() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();

    for kind in ["file", "directory"] {
        let source = temporary.path().join(format!("source-{kind}"));
        let destination = temporary.path().join(format!("destination-{kind}"));
        std::fs::write(&source, kind.as_bytes()).expect("source file");
        if kind == "file" {
            std::fs::write(&destination, b"preserve").expect("destination file");
        } else {
            std::fs::create_dir(&destination).expect("destination directory");
        }

        let failure = block_on(namespace.rename(&source, &destination))
            .expect_err("strict rename must not replace any destination object");
        assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
        assert_eq!(
            failure.commit_evidence(),
            &RenameCommitEvidence::NotRenamedByOperation
        );
        assert_eq!(std::fs::read(&source).expect("source remains"), kind.as_bytes());
        if kind == "file" {
            assert_eq!(std::fs::read(&destination).expect("destination remains"), b"preserve");
        } else {
            assert!(destination.is_dir());
        }
    }
}

#[test]
// 验证同一目录项、硬链接目标、缺失源和缺失目标父目录的严格失败语义。
fn test_rename_rejects_same_entry_hard_link_and_missing_inputs() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    std::fs::write(&source, b"payload").expect("source file");

    let same_failure = block_on(namespace.rename(&source, &source))
        .expect_err("the same directory entry is not an idempotent success");
    assert_eq!(
        same_failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation
    );
    assert_eq!(std::fs::read(&source).expect("source remains"), b"payload");

    let hard_link = temporary.path().join("hard-link");
    std::fs::hard_link(&source, &hard_link).expect("hard link");
    let hard_link_failure = block_on(namespace.rename(&source, &hard_link))
        .expect_err("an existing hard-link destination must be rejected");
    assert_eq!(
        hard_link_failure.error().current_context(),
        &ErrorKind::AlreadyExists
    );
    assert!(source.exists());
    assert!(hard_link.exists());

    let missing = temporary.path().join("missing");
    let destination = temporary.path().join("new-destination");
    let missing_failure = block_on(namespace.rename(&missing, &destination))
        .expect_err("missing source is strict failure");
    assert_eq!(missing_failure.error().current_context(), &ErrorKind::NotFound);
    assert!(!destination.exists());

    let missing_parent_destination = temporary
        .path()
        .join("missing-parent")
        .join("destination");
    let parent_failure = block_on(namespace.rename(
        &source,
        &missing_parent_destination,
    ))
    .expect_err("rename does not create a destination parent");
    assert_eq!(parent_failure.error().current_context(), &ErrorKind::NotFound);
    assert!(source.exists());
}

#[cfg(unix)]
#[test]
// 验证最终符号链接（包括悬空链接）自身被改名，而非其指向的对象。
fn test_rename_moves_final_symlinks_themselves_including_dangling_links() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("target");
    std::fs::write(&target, b"payload").expect("target file");

    let source_link = temporary.path().join("source-link");
    let destination_link = temporary.path().join("destination-link");
    symlink(&target, &source_link).expect("source symlink");
    block_on(namespace.rename(&source_link, &destination_link))
        .expect("rename final symlink itself");
    assert!(target.is_file());
    assert!(std::fs::symlink_metadata(&source_link).is_err());
    assert!(std::fs::symlink_metadata(&destination_link)
        .expect("destination link")
        .file_type()
        .is_symlink());

    let dangling_source = temporary.path().join("dangling-source");
    let dangling_destination = temporary.path().join("dangling-destination");
    symlink(temporary.path().join("absent"), &dangling_source)
        .expect("dangling source");
    block_on(namespace.rename(&dangling_source, &dangling_destination))
        .expect("rename dangling link itself");
    assert!(std::fs::symlink_metadata(&dangling_source).is_err());
    assert!(std::fs::symlink_metadata(&dangling_destination)
        .expect("renamed dangling link")
        .file_type()
        .is_symlink());
}

#[test]
// 验证操作在首次轮询前无副作用，届时出现的目标会赢得“不替换”竞争。
fn test_rename_is_lazy_and_destination_creation_wins_before_first_poll() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, b"source").expect("source file");

    let future = namespace.rename(&source, &destination);
    std::fs::write(&destination, b"destination")
        .expect("external destination before first poll");
    let failure = block_on(future).expect_err("existing destination wins");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation
    );
    assert_eq!(std::fs::read(&source).expect("source remains"), b"source");
    assert_eq!(std::fs::read(&destination).expect("destination remains"), b"destination");
}

#[test]
// 验证目录不能移动进自身后代，失败后原目录层次完整保留。
fn test_rename_rejects_moving_a_directory_into_its_descendant() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    let child = source.join("child");
    let destination = child.join("moved");
    std::fs::create_dir_all(&child).expect("source hierarchy");

    let failure = block_on(namespace.rename(&source, &destination))
        .expect_err("a directory cannot move into its own descendant");
    assert_eq!(
        failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation
    );
    assert!(source.is_dir());
    assert!(child.is_dir());
    assert!(!destination.exists());
}

#[test]
// 压力验证两个源竞争同一目标时恰有一个成功，且结果内容与剩余源一致。
fn test_concurrent_renames_to_one_destination_have_exactly_one_winner() {
    let temporary = tempfile::tempdir().expect("temporary directory");

    for iteration in 0..32 {
        let first_source = temporary.path().join(format!("first-{iteration}"));
        let second_source = temporary.path().join(format!("second-{iteration}"));
        let destination = temporary.path().join(format!("destination-{iteration}"));
        std::fs::write(&first_source, b"first").expect("first source");
        std::fs::write(&second_source, b"second").expect("second source");
        let namespace = LocalFileNamespace::new();
        let first_namespace = namespace.clone();
        let second_namespace = namespace.clone();
        let first_destination = destination.clone();
        let second_destination = destination.clone();
        let first_path = first_source.clone();
        let second_path = second_source.clone();

        let (first, second) = block_on(async move {
            let first = async_global_executor::spawn(async move {
                first_namespace
                    .rename(&first_path, &first_destination)
                    .await
            });
            let second = async_global_executor::spawn(async move {
                second_namespace
                    .rename(&second_path, &second_destination)
                    .await
            });
            (first.await, second.await)
        });

        assert_eq!([first.is_ok(), second.is_ok()].into_iter().filter(|value| *value).count(), 1);
        let content = std::fs::read(&destination).expect("one published destination");
        match content.as_slice() {
            b"first" => {
                assert!(!first_source.exists());
                assert!(second_source.exists());
            }
            b"second" => {
                assert!(first_source.exists());
                assert!(!second_source.exists());
            }
            other => panic!("unexpected destination content: {other:?}"),
        }
        for failure in [first.err(), second.err()].into_iter().flatten() {
            assert!(matches!(
                failure.error().current_context(),
                ErrorKind::AlreadyExists | ErrorKind::Conflict | ErrorKind::NotFound
            ));
            assert_eq!(
                failure.commit_evidence(),
                &RenameCommitEvidence::NotRenamedByOperation
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
// 验证 Linux 改名只改变目录项，既有文件资源和映射仍绑定原底层对象。
fn test_rename_keeps_open_file_and_mmap_bound_to_the_same_object() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, b"payload").expect("source file");
    let read_file = block_on(namespace.open(&source, FileAccessMode::Read))
        .expect("read resource");
    let mmap_file = block_on(namespace.open(&source, FileAccessMode::ReadMmap))
        .expect("mmap resource");
    // SAFETY: 本测试使用独立临时目录，映射存活期间没有其它进程或库外句柄
    // 截断、替换或修改该文件；改名只改变目录项，不改变已映射对象的身份。
    let mapping = unsafe {
        block_on(mmap_file.map_read_only_uncoordinated(
            MmapRange::new(0, 7).expect("mapping range"),
        ))
    }
    .expect("read mapping");

    block_on(namespace.rename(&source, &destination))
        .expect("Linux permits rename while handles and mappings remain open");
    assert!(!source.exists());
    assert_eq!(mapping.as_bytes(), b"payload");
    drop(mapping);
    let mut buffer = [0_u8; 7];
    block_on(read_file.read_exact_at(
        0,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("old read handle remains bound to renamed file");
    assert_eq!(&buffer, b"payload");
    assert_eq!(std::fs::read(&destination).expect("renamed file"), b"payload");
}

#[cfg(target_os = "linux")]
#[test]
// 验证跨文件系统改名明确失败，绝不隐式退化为具有不同故障语义的复制删除。
fn test_cross_filesystem_rename_never_falls_back_to_copy_and_delete() {
    use std::os::unix::fs::MetadataExt;

    let other_root = std::path::Path::new("/dev/shm");
    assert!(
        other_root.is_dir(),
        "该专项要求 Linux /dev/shm；环境缺失时不能把未执行计为通过",
    );
    let temporary = tempfile::tempdir().expect("temporary directory");
    assert_ne!(
        std::fs::metadata(temporary.path())
            .expect("source metadata")
            .dev(),
        std::fs::metadata(other_root)
            .expect("destination metadata")
            .dev(),
        "该专项要求默认临时目录与 /dev/shm 位于不同文件系统",
    );
    let other_temporary = tempfile::tempdir_in(other_root)
        .expect("temporary directory on another filesystem");
    let source = temporary.path().join("source");
    let destination = other_temporary.path().join("destination");
    std::fs::write(&source, b"payload").expect("source file");

    let failure = block_on(LocalFileNamespace::new().rename(
        &source,
        &destination,
    ))
    .expect_err("cross-filesystem rename cannot degrade to copy-and-delete");
    assert_eq!(
        failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation
    );
    assert!(source.is_file());
    assert!(!destination.exists());
}
