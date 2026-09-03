//! `LocalFileNamespace::available_space` 的真实本地文件系统集成测试。
//!
//! 每项使用独立临时路径；Linux tmpfs 用例依赖 `/dev/shm`，允许 64 MiB 外部
//! 容量噪声。测试覆盖懒执行、目标类型、符号链接、非法路径及与 MMAP／追加组合。

use futures_lite::future::block_on;
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace, MmapRange,
};
use pi_result::ErrorKind;

// 验证现存常规文件和目录都能定位其实际存储作用域并返回非零候选字节；该
// 断言只依赖夹具刚成功写入，不比较易受系统并发影响的精确容量。
#[test]
fn test_local_available_space_accepts_existing_directory_and_file() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let file_path = temporary_directory.path().join("target");
    std::fs::write(&file_path, b"content")
        .expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    let directory_space = block_on(
        namespace.available_space(&temporary_directory.path().to_path_buf()),
    )
    .expect("现存目录应能定位其存储作用域");
    let file_space = block_on(namespace.available_space(&file_path))
        .expect("现存文件应能定位其存储作用域");

    assert!(
        directory_space.available_bytes() > 0,
        "能够创建测试文件的真实临时文件系统应报告非零候选空间",
    );
    assert!(
        file_space.available_bytes() > 0,
        "同一真实临时文件系统上的文件应报告非零候选空间",
    );
}

// 验证查询 Future 构造时不观察路径：调用后、首次 poll 前创建目标目录应让
// 同一个 Future 成功，保护异步接口的懒执行边界。
#[test]
fn test_local_available_space_is_lazy_and_observes_target_at_first_poll() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("created-after-call");
    let namespace = LocalFileNamespace::new();

    let unpolled_query = namespace.available_space(&path);
    std::fs::create_dir(&path)
        .expect("未轮询查询不得提前观察或占用目标");

    block_on(unpolled_query)
        .expect("首次轮询时已经存在的目录应能被查询");
}

// 验证缺失最终目标精确返回 `NotFound`，不得偷偷回退到父目录空间并伪造
// 一个可用容量结果。
#[test]
fn test_local_available_space_rejects_missing_target() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("missing");
    let namespace = LocalFileNamespace::new();

    let error = block_on(namespace.available_space(&path))
        .expect_err("缺失定位符不能回退查询父目录");
    assert_eq!(error.current_context(), &ErrorKind::NotFound);
}

// 验证 Unix 悬空最终符号链接按缺失目标失败，含 NUL 的原生路径按非法输入
// 失败；两者均不得回退查询其它路径。此测试仅覆盖 Unix 参数表示。
#[cfg(unix)]
#[test]
fn test_local_available_space_rejects_dangling_symlink_and_embedded_nul() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let missing_target = temporary_directory.path().join("missing-target");
    let dangling = temporary_directory.path().join("dangling");
    symlink(&missing_target, &dangling).expect("应能建立真实悬空符号链接");
    let namespace = LocalFileNamespace::new();

    let dangling_error = block_on(namespace.available_space(&dangling))
        .expect_err("悬空最终符号链接不能回退查询链接所在目录");
    assert_eq!(dangling_error.current_context(), &ErrorKind::NotFound);

    let nul_path = std::path::PathBuf::from(OsString::from_vec(
        b"invalid\0path".to_vec(),
    ));
    let nul_error = block_on(namespace.available_space(&nul_path))
        .expect_err("带内嵌 NUL 的原生路径必须在查询前失败");
    assert_eq!(nul_error.current_context(), &ErrorKind::InvalidInput);
}

// 验证 Unix 设备节点不属于允许的常规文件／目录目标并返回 `Unsupported`，
// 不把设备所在挂载点容量当成该 API 的合法结果。
#[cfg(unix)]
#[test]
fn test_local_available_space_rejects_non_file_non_directory_target() {
    let namespace = LocalFileNamespace::new();
    let device_path = std::path::PathBuf::from("/dev/null");

    let error = block_on(namespace.available_space(&device_path))
        .expect_err("设备不是可接受的文件或目录定位符");
    assert_eq!(error.current_context(), &ErrorKind::Unsupported);
}

// 验证 Linux 最终符号链接会跟随到 tmpfs 目标的挂载空间；前后两次基准查询
// 与 64 MiB 容差吸收并发系统负载，测试必须串行且依赖可写 `/dev/shm`。
#[cfg(target_os = "linux")]
#[test]
fn test_local_available_space_follows_final_symlink_into_tmpfs() {
    use std::os::unix::fs::symlink;

    let source_directory = tempfile::tempdir()
        .expect("应能建立符号链接所在目录");
    let tmpfs_directory = tempfile::tempdir_in("/dev/shm")
        .expect("Linux 验收环境应提供可写 tmpfs");
    let link = source_directory.path().join("tmpfs-target");
    symlink(tmpfs_directory.path(), &link)
        .expect("应能建立指向 tmpfs 的最终符号链接");
    let namespace = LocalFileNamespace::new();

    let target_before = block_on(namespace.available_space(
        &tmpfs_directory.path().to_path_buf(),
    ))
    .expect("tmpfs 目录应支持容量查询")
    .available_bytes();
    let through_link = block_on(namespace.available_space(&link))
        .expect("最终符号链接应跟随到 tmpfs 存储作用域")
        .available_bytes();
    let target_after = block_on(namespace.available_space(
        &tmpfs_directory.path().to_path_buf(),
    ))
    .expect("重复 tmpfs 查询应成功")
    .available_bytes();

    const CONCURRENT_CHANGE_TOLERANCE: u64 = 64 * 1024 * 1024;
    assert!(
        through_link.abs_diff(target_before) <= CONCURRENT_CHANGE_TOLERANCE
            || through_link.abs_diff(target_after)
                <= CONCURRENT_CHANGE_TOLERANCE,
        "链接查询应落到目标 tmpfs，而不是链接所在文件系统",
    );
}

// 验证容量元信息观察可与已存在只读映射和独立 O_APPEND 资源组合：两次查询
// 均成功，映射快照稳定且追加字节完整；夹具无库外参与者。
#[test]
fn test_local_available_space_coexists_with_active_mmap_and_append() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"0123456789")
        .expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("只读映射资源应能打开");
    let mut append_resource = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("追加资源应能独立打开");

    // 测试目录不存在本库协调域以外的访问者，满足未协调映射入口要求。
    let mapping = unsafe {
        block_on(mmap_resource.map_read_only_uncoordinated(
            MmapRange::new(0, 4).expect("测试映射范围应有效"),
        ))
    }
    .expect("只读映射应能建立");

    block_on(namespace.available_space(&path))
        .expect("容量观察不应与活动映射互斥");
    block_on(append_resource.append(b"append".to_vec()))
        .expect("普通追加应允许与既有映射并存");
    block_on(namespace.available_space(&path))
        .expect("追加完成后映射存活期间仍应允许容量观察");
    assert_eq!(mapping.as_bytes(), b"0123");
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789append");
}
