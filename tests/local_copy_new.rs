//! `LocalFileNamespace::copy_new` 的真实文件系统与并发集成测试。
//!
//! 本文件以最终目标不可见中间态为核心，覆盖分块边界、惰性、严格不替换、
//! 临时资源清理、取消、发布竞争、源对象身份、跨文件系统和 API 组合语义。
//! 多个用例会复制大文件并观察时序，必须串行运行，不能与资源压力测试并发。

use std::collections::BTreeSet;
use std::ffi::OsString;

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    CopyStagingEvidence, CopyTargetEvidence, FileAccessMode, FileIo,
    FileNamespace, LocalFileNamespace, MmapRange, OverwriteTargetEvidence,
    ReadTargetRegion, TransferProgress,
};
use pi_result::ErrorKind;

// 以原生名称建立独立目录快照，用于证明临时资源最终没有泄漏。
fn directory_names(path: &std::path::Path) -> BTreeSet<OsString> {
    std::fs::read_dir(path)
        .expect("read test directory")
        .map(|entry| entry.expect("read test entry").file_name())
        .collect()
}

#[test]
// 验证空文件、单字节及传输窗口前后边界均得到完整、精确的目标内容。
fn test_copy_new_copies_empty_boundary_and_multiple_transfer_windows() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let lengths = [0, 1, 64 * 1024 - 1, 64 * 1024, 64 * 1024 + 1, 131_091];

    for (case, length) in lengths.into_iter().enumerate() {
        let source = temporary.path().join(format!("source-{case}"));
        let destination = temporary.path().join(format!("destination-{case}"));
        let content = (0..length)
            .map(|index| ((index * 31 + case) % 251) as u8)
            .collect::<Vec<_>>();
        std::fs::write(&source, &content).expect("source fixture");

        let outcome = block_on(namespace.copy_new(&source, &destination))
            .expect("strict copy succeeds");

        assert_eq!(outcome.content_byte_len(), length as u64);
        assert_eq!(std::fs::read(&destination).expect("destination"), content);
    }

    assert_eq!(directory_names(temporary.path()).len(), lengths.len() * 2);
}

#[test]
// 验证复制结果可直接通过 FileNamespace::open 与 FileIo 随机读组合消费。
fn test_copy_new_result_composes_with_public_file_io_read() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, b"public-composition").expect("source fixture");
    let namespace = LocalFileNamespace::new();

    block_on(namespace.copy_new(&source, &destination)).expect("copy");
    let file = block_on(namespace.open(&destination, FileAccessMode::Read))
        .expect("open copied target");
    let mut buffer = vec![0_u8; 18];
    block_on(file.read_exact_at(
        0,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("read copied target through FileIo");

    assert_eq!(buffer, b"public-composition");
}

#[test]
// 验证 Future 首次轮询才观察源与目标，构造 Future 本身没有文件系统副作用。
fn test_copy_new_is_lazy_and_observes_state_only_when_polled() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");

    let copy = namespace.copy_new(&source, &destination);
    std::fs::write(&source, b"created-after-future").expect("late source");
    let outcome = block_on(copy).expect("lazy future sees late source");
    assert_eq!(outcome.content_byte_len(), 20);
    assert_eq!(std::fs::read(&destination).unwrap(), b"created-after-future");

    let second_destination = temporary.path().join("second-destination");
    let copy = namespace.copy_new(&source, &second_destination);
    std::fs::write(&second_destination, b"winner").expect("late destination");
    let failure = block_on(copy).expect_err("late destination must win");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation
    );
    assert_eq!(std::fs::read(&second_destination).unwrap(), b"winner");
}

#[test]
// 验证文件、目录和最终符号链接等任意既有目标均不会被修改或替换。
fn test_copy_new_rejects_existing_destination_kinds_without_modification() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let source = temporary.path().join("source");
    std::fs::write(&source, b"source-content").expect("source fixture");

    let existing_file = temporary.path().join("existing-file");
    std::fs::write(&existing_file, b"preserve").expect("existing file");
    let failure = block_on(namespace.copy_new(&source, &existing_file))
        .expect_err("existing file must not be replaced");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(std::fs::read(&existing_file).unwrap(), b"preserve");

    let existing_directory = temporary.path().join("existing-directory");
    std::fs::create_dir(&existing_directory).expect("existing directory");
    let failure = block_on(namespace.copy_new(&source, &existing_directory))
        .expect_err("existing directory must not be replaced");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert!(existing_directory.is_dir());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let link = temporary.path().join("existing-link");
        let dangling = temporary.path().join("existing-dangling-link");
        symlink(&source, &link).expect("existing link");
        symlink(temporary.path().join("absent"), &dangling)
            .expect("existing dangling link");
        for destination in [&link, &dangling] {
            let failure = block_on(namespace.copy_new(&source, destination))
                .expect_err("every final symlink is an existing entry");
            assert_eq!(
                failure.error().current_context(),
                &ErrorKind::AlreadyExists
            );
            assert!(std::fs::symlink_metadata(destination)
                .expect("link remains")
                .file_type()
                .is_symlink());
        }
    }

    assert_eq!(std::fs::read(&source).unwrap(), b"source-content");
}

#[test]
// 验证缺失源、非文件源和缺失目标父目录在创建临时资源前准确失败。
fn test_copy_new_reports_pre_staging_source_and_parent_failures() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let missing_source = temporary.path().join("missing-source");
    let destination = temporary.path().join("destination");

    let failure = block_on(namespace.copy_new(&missing_source, &destination))
        .expect_err("missing source");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation
    );
    assert!(!destination.exists());

    let source_directory = temporary.path().join("source-directory");
    std::fs::create_dir(&source_directory).expect("source directory");
    let failure = block_on(namespace.copy_new(&source_directory, &destination))
        .expect_err("directories are not file copy sources");
    assert_eq!(failure.error().current_context(), &ErrorKind::Unsupported);
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation
    );

    let source = temporary.path().join("source");
    std::fs::write(&source, b"source").expect("source file");
    let missing_parent_destination = temporary
        .path()
        .join("missing-parent")
        .join("destination");
    let failure = block_on(namespace.copy_new(
        &source,
        &missing_parent_destination,
    ))
    .expect_err("copy does not create destination parents");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation
    );
    assert!(!missing_parent_destination.exists());
}

#[cfg(unix)]
#[test]
// 验证最终源符号链接跟随常规文件目标，而悬空源链接严格失败。
fn test_copy_new_follows_final_source_symlink_and_rejects_dangling_source() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let namespace = LocalFileNamespace::new();
    let target = temporary.path().join("target");
    let source_link = temporary.path().join("source-link");
    let destination = temporary.path().join("destination");
    std::fs::write(&target, b"linked-content").expect("link target");
    symlink(&target, &source_link).expect("source link");

    let outcome = block_on(namespace.copy_new(&source_link, &destination))
        .expect("final source symlink follows its regular-file target");
    assert_eq!(outcome.content_byte_len(), 14);
    assert_eq!(std::fs::read(&destination).unwrap(), b"linked-content");
    assert!(std::fs::symlink_metadata(&source_link)
        .unwrap()
        .file_type()
        .is_symlink());

    let dangling = temporary.path().join("dangling");
    let second_destination = temporary.path().join("second-destination");
    symlink(temporary.path().join("missing"), &dangling)
        .expect("dangling source link");
    let failure = block_on(namespace.copy_new(&dangling, &second_destination))
        .expect_err("dangling source cannot be copied");
    assert_eq!(failure.error().current_context(), &ErrorKind::NotFound);
    assert!(!second_destination.exists());
}

#[test]
// 压力验证多个任务共享一个目标时恰有一个发布完整内容，其余均无残留。
fn test_copy_new_has_exactly_one_complete_winner_for_shared_destination() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    let content = (0..(256 * 1024 + 17))
        .map(|index| (index % 239) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&source, &content).expect("source fixture");
    let namespace = LocalFileNamespace::new();

    let results = block_on(async {
        let tasks = (0..32)
            .map(|_| {
                let namespace = namespace.clone();
                let source = source.clone();
                let destination = destination.clone();
                async_global_executor::spawn(async move {
                    namespace.copy_new(&source, &destination).await
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
            &CopyTargetEvidence::NotPublishedByOperation
        );
        assert_eq!(
            failure.staging_evidence(),
            &CopyStagingEvidence::NotCreatedByOperation
        );
    }
    assert_eq!(std::fs::read(&destination).unwrap(), content);
    assert_eq!(directory_names(temporary.path()).len(), 2);
}

#[test]
// 验证发布尚未提交时取消公开 Future，最终目标不会出现且预留会被释放。
fn test_cancelled_copy_new_never_publishes_when_publish_was_not_submitted() {
    const CONTENT_LEN: usize = 64 * 1024 * 1024;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x5a; CONTENT_LEN])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();

    let first_poll = block_on(future::poll_once(
        namespace.copy_new(&source, &destination),
    ));
    assert!(
        first_poll.is_none(),
        "large copy must still be pending when its public future is cancelled"
    );

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let created = loop {
        match block_on(namespace.create_new(
            &destination,
            FileAccessMode::Read,
        )) {
            Ok(file) => break file,
            Err(failure)
                if failure.error().current_context() == &ErrorKind::Conflict =>
            {
                assert!(
                    std::time::Instant::now() < deadline,
                    "cancelled copy owner must release its target reservation"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(failure) => panic!(
                "cancelled copy unexpectedly published or failed cleanup: {failure}"
            ),
        }
    };

    assert_eq!(
        std::fs::metadata(&destination).expect("new target").len(),
        0,
        "the successful create_new, not the cancelled copy, created the target"
    );
    drop(created);
    block_on(namespace.remove_file(&destination))
        .expect("remove the proof target after its resource is released");
    assert_eq!(directory_names(temporary.path()).len(), 1);
}

#[test]
// 验证复制按开始时源长度取快照、目标在完成前隐藏，并与追加而非破坏性操作并存。
fn test_copy_new_excludes_later_append_and_keeps_target_hidden_during_transfer() {
    const CAPTURED_LEN: usize = 64 * 1024 * 1024;
    const APPENDED: &[u8] = b"later-append";

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x33; CAPTURED_LEN])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();
    let mut append_resource = block_on(
        namespace.open(&source, FileAccessMode::Append),
    )
    .expect("append resource");
    let mapping_resource = block_on(
        namespace.open(&source, FileAccessMode::ReadMmap),
    )
    .expect("mapping resource");
    let mut overwrite_resource = block_on(
        namespace.open(&source, FileAccessMode::Overwrite),
    )
    .expect("overwrite resource");
    let mut truncate_resource = block_on(
        namespace.open(&source, FileAccessMode::Truncate),
    )
    .expect("truncate resource");
    let copy_namespace = namespace.clone();
    let copy_source = source.clone();
    let copy_destination = destination.clone();
    let copy = async_global_executor::spawn(async move {
        copy_namespace
            .copy_new(&copy_source, &copy_destination)
            .await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let names = directory_names(temporary.path());
        assert!(
            !destination.exists(),
            "final target must not expose empty or partial staging content"
        );
        if names.len() > 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "observable staging entry should appear while the large copy runs"
        );
        std::thread::yield_now();
    }

    // 测试目录没有库外并发参与者；此调用用于验证同一稳定身份的受管复制
    // 读取租约会在真正建图前拒绝映射。
    let mapping_error = unsafe {
        block_on(mapping_resource.map_read_only_uncoordinated(
            MmapRange::new(0, 1).expect("one-byte range"),
        ))
    }
    .expect_err("active copy read lease must exclude every source mapping");
    assert_eq!(mapping_error.current_context(), &ErrorKind::Conflict);

    let overwrite_failure = block_on(
        overwrite_resource.overwrite_all(b"forbidden".to_vec()),
    )
    .expect_err("active copy read lease must exclude whole-file overwrite");
    let (overwrite_error, returned, progress, evidence) =
        overwrite_failure.into_parts();
    assert_eq!(overwrite_error.current_context(), &ErrorKind::Conflict);
    assert_eq!(returned, b"forbidden");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    let truncate_error = block_on(truncate_resource.truncate(0))
        .expect_err("active copy read lease must exclude truncation");
    assert_eq!(truncate_error.current_context(), &ErrorKind::Conflict);

    block_on(append_resource.append(APPENDED.to_vec()))
        .expect("strict append is allowed alongside the copy read lease");
    let outcome = block_on(copy).expect("copy completes after concurrent append");

    assert_eq!(outcome.content_byte_len(), CAPTURED_LEN as u64);
    let copied = std::fs::read(&destination).expect("copied destination");
    assert_eq!(copied.len(), CAPTURED_LEN);
    assert!(copied.iter().all(|byte| *byte == 0x33));
    let source_after = std::fs::read(&source).expect("source after append");
    assert_eq!(source_after.len(), CAPTURED_LEN + APPENDED.len());
    assert_eq!(&source_after[CAPTURED_LEN..], APPENDED);
    assert_eq!(directory_names(temporary.path()).len(), 2);
}

#[test]
// 验证库外截短造成提前 EOF 时禁止发布，并删除属于本次操作的临时文件。
fn test_copy_new_rejects_premature_source_eof_and_removes_its_staging_file() {
    const CAPTURED_LEN: u64 = 64 * 1024 * 1024;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x71; CAPTURED_LEN as usize])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();
    let copy_namespace = namespace.clone();
    let copy_source = source.clone();
    let copy_destination = destination.clone();
    let copy = async_global_executor::spawn(async move {
        copy_namespace
            .copy_new(&copy_source, &copy_destination)
            .await
    });

    let source_name = source.file_name().expect("source name").to_os_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let staging = loop {
        assert!(
            !destination.exists(),
            "destination cannot appear before complete publication"
        );
        let candidate = std::fs::read_dir(temporary.path())
            .expect("read test directory")
            .map(|entry| entry.expect("directory entry"))
            .find(|entry| entry.file_name() != source_name);
        if let Some(entry) = candidate {
            let path = entry.path();
            if let Ok(metadata) = std::fs::metadata(&path) {
                assert!(
                    metadata.len() < CAPTURED_LEN,
                    "the observed internal file must still be partial"
                );
                break path;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partial staging should become observable before transfer ends"
        );
        std::thread::yield_now();
    };

    let source_handle = std::fs::OpenOptions::new()
        .write(true)
        .open(&source)
        .expect("external uncoordinated source handle");
    source_handle.set_len(0).expect("force premature EOF");
    drop(source_handle);

    let failure = block_on(copy).expect_err("premature EOF must forbid publish");
    assert_eq!(failure.error().current_context(), &ErrorKind::Corrupted);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::CreatedThenRemovedByOperation
    );
    assert!(!destination.exists());
    assert!(!staging.exists());
    assert_eq!(directory_names(temporary.path()).len(), 1);
}

#[test]
// 验证库外参与者赢得最终目标发布竞争后，其内容不受影响且临时文件被清理。
fn test_copy_new_loses_external_publish_race_without_touching_winner() {
    const SOURCE_LEN: usize = 64 * 1024 * 1024;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x27; SOURCE_LEN])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();
    let copy_namespace = namespace.clone();
    let copy_source = source.clone();
    let copy_destination = destination.clone();
    let copy = async_global_executor::spawn(async move {
        copy_namespace
            .copy_new(&copy_source, &copy_destination)
            .await
    });

    let source_name = source.file_name().expect("source name").to_os_string();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let staging = loop {
        assert!(!destination.exists(), "copy target must still be unpublished");
        let candidate = std::fs::read_dir(temporary.path())
            .expect("read test directory")
            .map(|entry| entry.expect("directory entry"))
            .find(|entry| entry.file_name() != source_name);
        if let Some(entry) = candidate {
            let path = entry.path();
            if std::fs::metadata(&path)
                .is_ok_and(|metadata| metadata.len() < SOURCE_LEN as u64)
            {
                break path;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partial staging should be observable before publication"
        );
        std::thread::yield_now();
    };

    let mut external_winner = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&destination)
        .expect("external participant wins the no-replace race");
    use std::io::Write;
    external_winner
        .write_all(b"external-winner")
        .expect("write external winner");
    drop(external_winner);

    let failure = block_on(copy).expect_err("atomic publish must reject winner");
    assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::CreatedThenRemovedByOperation
    );
    assert_eq!(std::fs::read(&destination).unwrap(), b"external-winner");
    assert!(!staging.exists());
    assert_eq!(directory_names(temporary.path()).len(), 2);
}

#[test]
// 验证复制期间源读租约与安全删除、破坏性访问互斥，并预留最终目标名称。
fn test_copy_new_source_read_lease_blocks_safe_remove_until_completion() {
    const SOURCE_LEN: usize = 64 * 1024 * 1024;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x61; SOURCE_LEN])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();
    let copy_namespace = namespace.clone();
    let copy_source = source.clone();
    let copy_destination = destination.clone();
    let copy = async_global_executor::spawn(async move {
        copy_namespace
            .copy_new(&copy_source, &copy_destination)
            .await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(!destination.exists(), "target remains hidden before publish");
        if directory_names(temporary.path()).len() > 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "copy staging should become observable while transfer is active"
        );
        std::thread::yield_now();
    }

    let create_failure = match block_on(namespace.create_new(
        &destination,
        FileAccessMode::Read,
    )) {
        Ok(_) => panic!("copy target reservation must reject file creation"),
        Err(failure) => failure,
    };
    assert_eq!(create_failure.error().current_context(), &ErrorKind::Conflict);
    let directory_failure = block_on(namespace.create_dir(&destination))
        .expect_err("copy target reservation must reject directory creation");
    assert_eq!(
        directory_failure.error().current_context(),
        &ErrorKind::Conflict
    );
    let target_removal_failure = block_on(namespace.remove_file(&destination))
        .expect_err("reserved absent target cannot be concurrently removed");
    assert_eq!(
        target_removal_failure.error().current_context(),
        &ErrorKind::Conflict
    );
    let rename_failure = block_on(namespace.rename(&source, &destination))
        .expect_err("copy target reservation must reject rename publication");
    assert_eq!(rename_failure.error().current_context(), &ErrorKind::Conflict);
    assert!(source.exists());
    assert!(!destination.exists());

    let removal_failure = block_on(namespace.remove_file(&source))
        .expect_err("safe source removal must conflict with the copy read lease");
    assert_eq!(
        removal_failure.error().current_context(),
        &ErrorKind::Conflict
    );
    assert!(source.exists());

    block_on(copy).expect("copy completes after rejected removal");
    block_on(namespace.remove_file(&source))
        .expect("safe removal becomes admissible after copy completion");
    assert!(!source.exists());
    assert_eq!(std::fs::metadata(&destination).unwrap().len(), SOURCE_LEN as u64);
    assert_eq!(directory_names(temporary.path()).len(), 1);
}

#[cfg(target_os = "linux")]
#[test]
// 验证 Linux 上源改名后复制仍读取最初打开的底层对象，而非重新解析旧路径。
fn test_copy_new_keeps_reading_same_open_source_object_after_rename() {
    const SOURCE_LEN: usize = 64 * 1024 * 1024;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let renamed_source = temporary.path().join("renamed-source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, vec![0x42; SOURCE_LEN])
        .expect("large source fixture");
    let namespace = LocalFileNamespace::new();
    let copy_namespace = namespace.clone();
    let copy_source = source.clone();
    let copy_destination = destination.clone();
    let copy = async_global_executor::spawn(async move {
        copy_namespace
            .copy_new(&copy_source, &copy_destination)
            .await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        assert!(!destination.exists(), "target remains hidden before publish");
        if directory_names(temporary.path()).len() > 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "copy staging should become observable while transfer is active"
        );
        std::thread::yield_now();
    }

    block_on(namespace.rename(&source, &renamed_source))
        .expect("Linux permits renaming an open source while copy continues");
    assert!(!source.exists());
    assert!(renamed_source.exists());

    let outcome = block_on(copy).expect("copy remains bound to the open object");
    assert_eq!(outcome.content_byte_len(), SOURCE_LEN as u64);
    assert_eq!(std::fs::metadata(&destination).unwrap().len(), SOURCE_LEN as u64);
    assert_eq!(std::fs::metadata(&renamed_source).unwrap().len(), SOURCE_LEN as u64);
    assert!(std::fs::read(&destination)
        .unwrap()
        .into_iter()
        .all(|byte| byte == 0x42));
    assert_eq!(directory_names(temporary.path()).len(), 2);
}

#[cfg(target_os = "linux")]
#[test]
// 验证源和目标可位于不同文件系统，只要求临时文件与最终目标共享发布域。
fn test_copy_new_supports_cross_filesystem_source_and_destination() {
    use std::os::unix::fs::MetadataExt;

    let other_root = std::path::Path::new("/dev/shm");
    assert!(
        other_root.is_dir(),
        "该专项要求 Linux /dev/shm；环境缺失时不能把未执行计为通过",
    );
    let destination_parent = tempfile::tempdir().expect("destination directory");
    assert_ne!(
        std::fs::metadata(destination_parent.path())
            .expect("destination metadata")
            .dev(),
        std::fs::metadata(other_root)
            .expect("source root metadata")
            .dev(),
        "该专项要求默认临时目录与 /dev/shm 位于不同文件系统",
    );
    let source_parent = tempfile::tempdir_in(other_root)
        .expect("source directory on a distinct filesystem");
    let source = source_parent.path().join("source");
    let destination = destination_parent.path().join("destination");
    std::fs::write(&source, b"cross-filesystem-content")
        .expect("cross-filesystem source");

    let outcome = block_on(LocalFileNamespace::new().copy_new(
        &source,
        &destination,
    ))
    .expect("only staging and destination need to share a publish domain");

    assert_eq!(outcome.content_byte_len(), 24);
    assert_eq!(
        std::fs::read(&destination).unwrap(),
        b"cross-filesystem-content"
    );
    assert_eq!(directory_names(destination_parent.path()).len(), 1);
}

#[test]
// 验证硬链接别名收敛到同一源身份，任何活动映射都会阻止复制读租约。
fn test_copy_new_conflicts_with_active_source_mapping_across_hard_link_identity() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let source_alias = temporary.path().join("source-alias");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, b"mapped-source").expect("source fixture");
    std::fs::hard_link(&source, &source_alias).expect("source hard link");
    let namespace = LocalFileNamespace::new();
    let mapping_resource = block_on(
        namespace.open(&source, FileAccessMode::ReadMmap),
    )
    .expect("mapping resource");
    // 测试使用隔离目录且没有库外并发修改，满足未协调映射的外部前置条件。
    let mapping = unsafe {
        block_on(mapping_resource.map_read_only_uncoordinated(
            MmapRange::new(0, 13).expect("nonempty source range"),
        ))
    }
    .expect("source mapping");

    let failure = block_on(namespace.copy_new(&source_alias, &destination))
        .expect_err("stable source identity must conflict with any mapping");
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation
    );
    assert_eq!(
        failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation
    );
    assert!(!destination.exists());

    drop(mapping);
    block_on(namespace.copy_new(&source_alias, &destination))
        .expect("copy becomes admissible after the mapping guard is dropped");
    assert_eq!(std::fs::read(&destination).unwrap(), b"mapped-source");
}

#[cfg(unix)]
#[test]
// 验证 Unix 非 UTF-8 源、目标名称作为原生定位符全程无损处理。
fn test_copy_new_preserves_non_utf8_source_and_destination_names() {
    use std::os::unix::ffi::OsStringExt;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary
        .path()
        .join(OsString::from_vec(b"source-\xff".to_vec()));
    let destination = temporary
        .path()
        .join(OsString::from_vec(b"destination-\xfe".to_vec()));
    std::fs::write(&source, b"opaque-name-content").expect("source fixture");
    let namespace = LocalFileNamespace::new();

    let outcome = block_on(namespace.copy_new(&source, &destination))
        .expect("native non-UTF-8 names remain valid locators");

    assert_eq!(outcome.content_byte_len(), 19);
    assert_eq!(std::fs::read(&destination).unwrap(), b"opaque-name-content");
}

#[cfg(unix)]
#[test]
// 验证指向同一目标父目录的路径别名共享发布互斥，仍然只有一个赢家。
fn test_copy_new_converges_destination_parent_symlink_aliases() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary directory");
    let source = temporary.path().join("source");
    let real_parent = temporary.path().join("real-parent");
    let alias_parent = temporary.path().join("alias-parent");
    std::fs::write(&source, b"content").expect("source fixture");
    std::fs::create_dir(&real_parent).expect("real destination parent");
    symlink(&real_parent, &alias_parent).expect("destination parent alias");
    let destinations = [
        real_parent.join("destination"),
        alias_parent.join("destination"),
    ];
    let namespace = LocalFileNamespace::new();

    let first_namespace = namespace.clone();
    let second_namespace = namespace.clone();
    let first_source = source.clone();
    let second_source = source.clone();
    let first_destination = destinations[0].clone();
    let second_destination = destinations[1].clone();
    let (first, second) = block_on(async move {
        let first = async_global_executor::spawn(async move {
            first_namespace
                .copy_new(&first_source, &first_destination)
                .await
        });
        let second = async_global_executor::spawn(async move {
            second_namespace
                .copy_new(&second_source, &second_destination)
                .await
        });
        (first.await, second.await)
    });

    assert_eq!([first.is_ok(), second.is_ok()].into_iter().filter(|value| *value).count(), 1);
    assert_eq!(std::fs::read(real_parent.join("destination")).unwrap(), b"content");
    assert_eq!(directory_names(&real_parent).len(), 1);
}
