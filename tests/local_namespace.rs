//! 本地命名空间句柄、元信息、存在性和普通打开的真实集成测试。
//!
//! 覆盖线程 trait、文件／目录／符号链接、错误分类、全部访问角色以及打开后
//! 文件身份生命周期。每项使用独立临时目录，无跨进程参与者。

use futures_lite::future::block_on;
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, FileType, LocalFile,
    LocalFileNamespace,
};
use pi_result::ErrorKind;

// 编译期 helper：验证命名空间句柄同时满足 Clone、Send、Sync、'static；真实
// 跨线程移动由对应测试继续验证，不在 helper 中制造行为。
fn assert_clone_send_sync_static<T>()
where
    T: Clone + Send + Sync + 'static,
{
}

// 编译期 helper：验证文件资源的 Send、Sync、'static 类型边界，不推导 Clone
// 或具体并发操作合法性。
fn assert_send_sync_static<T>()
where
    T: Send + Sync + 'static,
{
}

// 验证 `LocalFileNamespace` 满足 Clone + Send + Sync + 'static，并把克隆跨
// 线程移动／返回；原句柄独立存活，证明共享入口不绑定创建线程。
#[test]
fn test_local_file_namespace_handle_is_cloneable_and_thread_safe() {
    assert_clone_send_sync_static::<LocalFileNamespace>();

    let namespace = LocalFileNamespace::new();
    let cloned = namespace.clone();
    let returned = std::thread::spawn(move || cloned)
        .join()
        .expect("本地命名空间句柄应能跨线程移动并返回");
    std::hint::black_box((namespace, returned));
}

// 验证 `LocalFile` 的公开类型边界为 Send + Sync + 'static；本测试刻意不要求
// Clone，具体方法的互斥性由操作集成测试覆盖。
#[test]
fn test_local_file_resource_has_required_thread_and_lifetime_bounds() {
    assert_send_sync_static::<LocalFile>();
}

// 验证 `try_exists` 对合法但缺失的路径返回成功的 false，而不是 NotFound
// 错误或父路径存在结果。
#[test]
fn test_local_namespace_try_exists_reports_missing_path() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let missing = temporary_directory.path().join("missing-entry");
    let namespace = LocalFileNamespace::new();

    let exists = block_on(namespace.try_exists(&missing))
        .expect("查询不存在的合法路径本身应成功");

    assert!(!exists);
}

// 验证现存常规文件和目录都返回 true，覆盖两种允许条目类型且不读取内容。
#[test]
fn test_local_namespace_try_exists_reports_existing_file_and_directory() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let file = temporary_directory.path().join("existing-file");
    let directory = temporary_directory.path().join("existing-directory");
    std::fs::write(&file, b"content").expect("应能建立真实测试文件");
    std::fs::create_dir(&directory).expect("应能建立真实测试子目录");
    let namespace = LocalFileNamespace::new();

    assert!(block_on(namespace.try_exists(&file)).unwrap());
    assert!(block_on(namespace.try_exists(&directory)).unwrap());
}

// 验证中间路径分量是常规文件的查询错误不得被压缩成 false，保护
// `try_exists` 只吞掉精确 NotFound 的错误边界。
#[test]
fn test_local_namespace_try_exists_preserves_non_not_found_errors() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let regular_file = temporary_directory.path().join("regular-file");
    std::fs::write(&regular_file, b"content").expect("应能建立真实测试文件");
    let impossible_child = regular_file.join("child");
    let namespace = LocalFileNamespace::new();

    let error = block_on(namespace.try_exists(&impossible_child))
        .expect_err("中间路径分量不是目录不能伪装成不存在");

    assert_eq!(error.current_context(), &ErrorKind::Io);
}

// 验证 Unix `try_exists` 跟随最终符号链接：悬空链接的目标缺失返回 false，
// 不把链接目录项本身存在解释为目标存在。
#[cfg(unix)]
#[test]
fn test_local_namespace_try_exists_follows_dangling_symbolic_link() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let missing_target = temporary_directory.path().join("missing-target");
    let link = temporary_directory.path().join("dangling-link");
    symlink(&missing_target, &link).expect("应能建立真实悬空符号链接");
    let namespace = LocalFileNamespace::new();

    assert!(!block_on(namespace.try_exists(&link)).unwrap());
}

// 验证常规文件元信息是七字节快照，权限和时间按平台能力填充，版本为空；
// Unix 额外验证可执行位，Windows 明确为未知。
#[test]
fn test_local_namespace_metadata_reports_regular_file_snapshot() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let file = temporary_directory.path().join("regular-file");
    std::fs::write(&file, b"content").expect("应能建立真实测试文件");

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = std::fs::metadata(&file).unwrap().permissions();
        permissions.set_mode(0o740);
        std::fs::set_permissions(&file, permissions).unwrap();
    }

    let namespace = LocalFileNamespace::new();
    let metadata =
        block_on(namespace.metadata(&file)).expect("常规文件元信息查询应成功");

    assert_eq!(metadata.file_type, FileType::RegularFile);
    assert_eq!(metadata.byte_len, Some(7));
    assert_eq!(metadata.permissions.read_only, Some(false));
    #[cfg(unix)]
    assert_eq!(metadata.permissions.executable, Some(true));
    #[cfg(windows)]
    assert_eq!(metadata.permissions.executable, None);
    assert!(metadata.times.modified.is_some());
    assert!(metadata.times.accessed.is_some());
    #[cfg(unix)]
    assert!(metadata.times.metadata_changed.is_some());
    #[cfg(windows)]
    assert!(metadata.times.metadata_changed.is_none());
    assert!(metadata.version.is_none());
}

// 验证目录元信息类型为 Directory、内容长度为 None、可执行语义未知且无资源
// 版本，防止把目录项大小误报为文件内容长度。
#[test]
fn test_local_namespace_metadata_reports_directory_without_content_length() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let directory = temporary_directory.path().join("directory");
    std::fs::create_dir(&directory).expect("应能建立真实测试目录");
    let namespace = LocalFileNamespace::new();

    let metadata =
        block_on(namespace.metadata(&directory)).expect("目录元信息查询应成功");

    assert_eq!(metadata.file_type, FileType::Directory);
    assert_eq!(metadata.byte_len, None);
    assert!(metadata.permissions.read_only.is_some());
    assert_eq!(metadata.permissions.executable, None);
    assert!(metadata.version.is_none());
}

// 验证 Unix `metadata` 跟随最终符号链接并返回目标常规文件的类型与 14 字节
// 内容长度，而非链接本身快照。
#[cfg(unix)]
#[test]
fn test_local_namespace_metadata_follows_final_symbolic_link() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("target");
    let link = temporary_directory.path().join("link");
    std::fs::write(&target, b"target-content").expect("应能建立链接目标");
    symlink(&target, &link).expect("应能建立真实符号链接");
    let namespace = LocalFileNamespace::new();

    let metadata =
        block_on(namespace.metadata(&link)).expect("跟随符号链接查询应成功");

    assert_eq!(metadata.file_type, FileType::RegularFile);
    assert_eq!(metadata.byte_len, Some(14));
}

// 验证元信息查询区分缺失最终目标的 NotFound 与中间分量非目录的 I/O 错误，
// 不将所有路径失败归并为同一分类。
#[test]
fn test_local_namespace_metadata_preserves_query_error_classes() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let missing = temporary_directory.path().join("missing");
    let regular_file = temporary_directory.path().join("regular-file");
    std::fs::write(&regular_file, b"content").expect("应能建立真实测试文件");
    let impossible_child = regular_file.join("child");
    let namespace = LocalFileNamespace::new();

    let missing_error = block_on(namespace.metadata(&missing)).unwrap_err();
    assert_eq!(missing_error.current_context(), &ErrorKind::NotFound);
    let invalid_path_error =
        block_on(namespace.metadata(&impossible_child)).unwrap_err();
    assert_eq!(invalid_path_error.current_context(), &ErrorKind::Io);
}

// 验证 Unix `metadata` 对悬空最终链接跟随目标并返回 NotFound，不退回
// `symlink_metadata` 语义。
#[cfg(unix)]
#[test]
fn test_local_namespace_metadata_reports_dangling_link_target_as_missing() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let missing_target = temporary_directory.path().join("missing-target");
    let link = temporary_directory.path().join("dangling-link");
    symlink(&missing_target, &link).expect("应能建立真实悬空符号链接");
    let namespace = LocalFileNamespace::new();

    let error = block_on(namespace.metadata(&link)).unwrap_err();

    assert_eq!(error.current_context(), &ErrorKind::NotFound);
}

// 验证 Unix `symlink_metadata` 对有效与悬空链接都观察链接本身：类型为
// SymbolicLink、内容长度与可执行语义未知、无版本。
#[cfg(unix)]
#[test]
fn test_local_namespace_symlink_metadata_observes_valid_and_dangling_links() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("target");
    let valid_link = temporary_directory.path().join("valid-link");
    let missing_target = temporary_directory.path().join("missing-target");
    let dangling_link = temporary_directory.path().join("dangling-link");
    std::fs::write(&target, b"content").expect("应能建立真实测试文件");
    symlink(&target, &valid_link).expect("应能建立有效符号链接");
    symlink(&missing_target, &dangling_link).expect("应能建立悬空符号链接");
    let namespace = LocalFileNamespace::new();

    for link in [&valid_link, &dangling_link] {
        let metadata = block_on(namespace.symlink_metadata(link))
            .expect("不跟随查询应观察符号链接本身");
        assert_eq!(metadata.file_type, FileType::SymbolicLink);
        assert_eq!(metadata.byte_len, None);
        assert_eq!(metadata.permissions.executable, None);
        assert!(metadata.version.is_none());
    }
}

// 验证 `symlink_metadata` 对非链接常规文件仍返回文件快照，精确区分最终目标
// 缺失的 NotFound 与中间分量非目录的 Io，覆盖正向和两类错误边界。
#[test]
fn test_local_namespace_symlink_metadata_handles_non_links_and_missing_paths() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let regular_file = temporary_directory.path().join("regular-file");
    let missing = temporary_directory.path().join("missing");
    std::fs::write(&regular_file, b"content").expect("应能建立真实测试文件");
    let impossible_child = regular_file.join("child");
    let namespace = LocalFileNamespace::new();

    let metadata = block_on(namespace.symlink_metadata(&regular_file))
        .expect("常规文件的不跟随查询应成功");
    assert_eq!(metadata.file_type, FileType::RegularFile);
    assert_eq!(metadata.byte_len, Some(7));

    let error = block_on(namespace.symlink_metadata(&missing)).unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::NotFound);

    let error = block_on(namespace.symlink_metadata(&impossible_child))
        .unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::Io);
}

// 验证六种访问角色都能打开现存常规文件，并且单纯 open + Drop 对文件内容
// 零副作用；角色不在打开时隐式截断或覆盖。
#[test]
fn test_local_namespace_open_supports_every_role_without_content_side_effects() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let file = temporary_directory.path().join("existing-file");
    let original_content = b"content-must-remain-unchanged";
    std::fs::write(&file, original_content).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ] {
        let resource = block_on(namespace.open(&file, access))
            .expect("每个本地文件角色都应能独立打开常规文件");
        drop(resource);
        assert_eq!(std::fs::read(&file).unwrap(), original_content);
    }
}

// 验证 `byte_len` 绑定已打开文件对象而非原路径：外部改名并在旧路径创建一字节
// 新文件后，资源仍报告原对象十六字节。
#[test]
fn test_local_file_byte_len_stays_bound_to_opened_object_after_rename() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let original_path = temporary_directory.path().join("original");
    let renamed_path = temporary_directory.path().join("renamed");
    std::fs::write(&original_path, b"original-content")
        .expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(namespace.open(&original_path, FileAccessMode::Read))
        .expect("常规文件应能打开");

    std::fs::rename(&original_path, &renamed_path).expect("应能改名已打开文件");
    std::fs::write(&original_path, b"x").expect("应能在旧位置建立另一文件");

    assert_eq!(block_on(resource.byte_len()).unwrap(), 16);
    assert_eq!(std::fs::metadata(&original_path).unwrap().len(), 1);
    assert_eq!(std::fs::metadata(&renamed_path).unwrap().len(), 16);
}

// 验证普通 open 对缺失目标返回 NotFound、对目录返回 Unsupported，不自动
// 创建、不把目录当普通文件，也不产生资源。
#[test]
fn test_local_namespace_open_rejects_missing_and_non_regular_targets() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let missing = temporary_directory.path().join("missing");
    let directory = temporary_directory.path().join("directory");
    std::fs::create_dir(&directory).expect("应能建立真实测试目录");
    let namespace = LocalFileNamespace::new();

    let missing_error = match block_on(namespace.open(&missing, FileAccessMode::Read)) {
        Ok(_) => panic!("不存在的目标不应打开成功"),
        Err(error) => error,
    };
    assert_eq!(missing_error.current_context(), &ErrorKind::NotFound);

    let directory_error =
        match block_on(namespace.open(&directory, FileAccessMode::Read)) {
            Ok(_) => panic!("目录不应作为普通文件打开"),
            Err(error) => error,
        };
    assert_eq!(directory_error.current_context(), &ErrorKind::Unsupported);
}

// 验证 Unix 普通 open 跟随最终符号链接并绑定其常规文件目标，返回资源观察到
// 目标十四字节长度。
#[cfg(unix)]
#[test]
fn test_local_namespace_open_follows_final_symbolic_link() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("target");
    let link = temporary_directory.path().join("link");
    std::fs::write(&target, b"target-content").expect("应能建立链接目标");
    symlink(&target, &link).expect("应能建立真实符号链接");
    let namespace = LocalFileNamespace::new();

    let resource = block_on(namespace.open(&link, FileAccessMode::Read))
        .expect("打开常规文件应跟随末级符号链接");

    assert_eq!(block_on(resource.byte_len()).unwrap(), 14);
}
