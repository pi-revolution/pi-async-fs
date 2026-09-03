//! 本地 `FileIo::truncate` 缩短型语义的真实文件集成测试。
//!
//! 覆盖保留精确前缀、等长幂等、禁止扩容和角色矩阵；本文件无并发或库外参与者，
//! 每项使用独立临时目录。

use futures_lite::future::block_on;
use pi_async_fs::{FileAccessMode, FileIo, FileNamespace, LocalFileNamespace};
use pi_result::ErrorKind;

// 验证 Truncate 角色把十字节文件缩短到四字节并精确保留前缀，路径内容与
// 资源 byte_len 同时确认提交结果。
#[test]
fn test_local_file_truncate_keeps_exact_prefix() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Truncate))
        .expect("截断资源应能打开且不提前修改文件");

    block_on(file.truncate(4)).expect("缩短型截断应成功");

    assert_eq!(std::fs::read(&path).unwrap(), b"0123");
    assert_eq!(block_on(file.byte_len()).unwrap(), 4);
}

// 验证等长截断是内容幂等成功，而任何增长请求都在副作用前返回 InvalidInput，
// 不以零填充或稀疏扩容文件。
#[test]
fn test_local_file_truncate_equal_is_noop_and_growth_is_rejected() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let original = b"content";
    std::fs::write(&path, original).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Truncate))
        .expect("截断资源应能打开");

    block_on(file.truncate(7)).expect("相同长度应直接成功");
    assert_eq!(std::fs::read(&path).unwrap(), original);

    let error = block_on(file.truncate(8))
        .expect_err("截断接口绝不能扩展文件");
    assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

// 验证 Read、Append、Overwrite 和两种 Mmap 角色均不能调用 truncate；每项
// 返回 InvalidState 且真实文件内容完全不变。
#[test]
fn test_local_file_truncate_rejects_every_non_truncate_role() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let original = b"content";
    std::fs::write(&path, original).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
    ] {
        let mut file = block_on(namespace.open(&path, access))
            .expect("每种非截断角色都应能独立打开");
        let error = block_on(file.truncate(0))
            .expect_err("非截断角色必须在任何副作用前失败");
        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}
