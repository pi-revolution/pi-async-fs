//! 本地 `FileIo::overwrite_all` 的真实文件、buffer 与取消集成测试。
//!
//! 覆盖 15 种内置 buffer、空覆盖、角色拒绝、旧尾移除和取消期排它性；64 MiB
//! 用例对 I/O 与调度敏感，必须串行独占运行。

use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_lite::future::{self, block_on};
use pi_async_fs::{
    DetachableWriteBuffer, FileAccessMode, FileIo, FileNamespace, LocalFile,
    LocalFileNamespace, OverwriteTargetEvidence, TransferProgress,
};
use pi_result::ErrorKind;

// 测试 helper 仅通过公开 `overwrite_all` 执行生产逻辑，并从真实路径读取结果；
// 它不实现覆盖、截断、恢复或协调，只复用“成功后内容精确相等”的断言。
fn overwrite_and_assert<B>(
    file: &mut LocalFile,
    path: &Path,
    buffer: B,
    expected: &[u8],
)
where
    B: DetachableWriteBuffer + Send,
    B::Detached: Send,
    B::Recovery: Send,
{
    block_on(file.overwrite_all(buffer)).expect("该 buffer 形态应完整覆盖成功");
    assert_eq!(std::fs::read(path).unwrap(), expected);
}

// 验证 Vec 覆盖把文件替换为完整新内容并移除旧尾，随后 byte_len 与真实路径
// 同为 3；打开 Overwrite 资源本身不提前修改文件。
#[test]
fn test_local_file_overwrite_vec_replaces_content_and_removes_old_tail() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"old-content-with-tail")
        .expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Overwrite))
        .expect("覆盖资源应能打开且不提前修改文件");

    block_on(file.overwrite_all(b"new".to_vec()))
        .expect("完整 Vec 应覆盖成功");

    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(block_on(file.byte_len()).unwrap(), 3);
}

// 验证空 Vec 是合法的完整覆盖，目标精确变为空文件且资源长度为零，而不是
// 被当成无操作或非法输入。
#[test]
fn test_local_file_overwrite_empty_buffer_clears_file() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"must-be-removed").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Overwrite))
        .expect("覆盖资源应能打开");

    block_on(file.overwrite_all(Vec::<u8>::new()))
        .expect("空输入应把目标覆盖为空文件");

    assert_eq!(std::fs::read(&path).unwrap(), b"");
    assert_eq!(block_on(file.byte_len()).unwrap(), 0);
}

// 验证所有内置拥有／借用 buffer 形态逐项成为完整目标内容；每次从真实路径
// 复核，防止某一 DetachableWriteBuffer 分支只通过类型检查未执行生产写入。
#[test]
fn test_local_file_overwrite_supports_every_builtin_write_buffer_shape() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"initial").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Overwrite))
        .expect("覆盖资源应能打开");

    overwrite_and_assert(&mut file, &path, b"vec".to_vec(), b"vec");
    overwrite_and_assert(
        &mut file,
        &path,
        Vec::from(&b"box"[..]).into_boxed_slice(),
        b"box",
    );
    overwrite_and_assert(
        &mut file,
        &path,
        Arc::<[u8]>::from(&b"arc"[..]),
        b"arc",
    );
    let cow_owned: Cow<'static, [u8]> = Cow::Owned(b"cow-owned".to_vec());
    overwrite_and_assert(&mut file, &path, cow_owned, b"cow-owned");
    let cow_borrowed: Cow<'_, [u8]> = Cow::Borrowed(b"cow-borrowed");
    overwrite_and_assert(&mut file, &path, cow_borrowed, b"cow-borrowed");
    overwrite_and_assert(
        &mut file,
        &path,
        Bytes::from_static(b"bytes"),
        b"bytes",
    );
    overwrite_and_assert(
        &mut file,
        &path,
        BytesMut::from(&b"bytes-mut"[..]),
        b"bytes-mut",
    );

    let slice = &b"slice-ref"[..];
    overwrite_and_assert(&mut file, &path, slice, b"slice-ref");
    let vec_buffer = b"vec-ref".to_vec();
    overwrite_and_assert(&mut file, &path, &vec_buffer, b"vec-ref");
    let box_buffer = Vec::from(&b"box-ref"[..]).into_boxed_slice();
    overwrite_and_assert(&mut file, &path, &box_buffer, b"box-ref");
    let arc_buffer = Arc::<[u8]>::from(&b"arc-ref"[..]);
    overwrite_and_assert(&mut file, &path, &arc_buffer, b"arc-ref");
    let cow_buffer: Cow<'_, [u8]> = Cow::Borrowed(b"cow-ref");
    overwrite_and_assert(&mut file, &path, &cow_buffer, b"cow-ref");
    let bytes_buffer = Bytes::from_static(b"bytes-ref");
    overwrite_and_assert(&mut file, &path, &bytes_buffer, b"bytes-ref");
    let bytes_mut_buffer = BytesMut::from(&b"bytes-mut-ref"[..]);
    overwrite_and_assert(
        &mut file,
        &path,
        &bytes_mut_buffer,
        b"bytes-mut-ref",
    );
}

// 验证五种非 Overwrite 角色在副作用前拒绝调用，原 Vec 的指针／容量／内容、
// Exact(0) 进度和 Unchanged 证据全部返回，文件保持原样。
#[test]
fn test_local_file_overwrite_rejects_every_other_role_and_restores_original_vec() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let original = b"unchanged";
    std::fs::write(&path, original).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ] {
        let mut file = block_on(namespace.open(&path, access))
            .expect("每种非覆盖角色都应能独立打开");
        let mut buffer = Vec::with_capacity(32);
        buffer.extend_from_slice(b"payload");
        let pointer = buffer.as_ptr();
        let capacity = buffer.capacity();

        let failure = block_on(file.overwrite_all(buffer))
            .expect_err("非覆盖角色必须在任何副作用前失败");
        let (error, buffer, progress, evidence) = failure.into_parts();

        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
        assert_eq!(buffer.as_ptr(), pointer);
        assert_eq!(buffer.capacity(), capacity);
        assert_eq!(buffer, b"payload");
        assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
        assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

// 验证取消 64 MiB 覆盖的等待者后，生产 actor 仍持有排它租约：读、元信息、
// 追加、第二覆盖均冲突且无副作用，截止前完成后目标恰为完整 D payload。
#[test]
fn test_local_file_cancelled_overwrite_keeps_exclusive_lease_until_actor_finishes() {
    const PAYLOAD_LEN: usize = 64 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"old").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("覆盖资源应能打开");
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("读取资源应能独立打开");
    let mut append_resource = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能独立打开");
    let mut competing_overwrite = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("第二个覆盖资源应能独立打开");

    let overwrite = overwrite_resource.overwrite_all(vec![b'D'; PAYLOAD_LEN]);
    let first_poll = block_on(future::poll_once(overwrite));
    assert!(first_poll.is_none(), "大覆盖第一次轮询应仍在执行中");

    let mut read_buffer = [0xaa; 4];
    let read_error = block_on(read_resource.read_at(
        0,
        &mut read_buffer,
        pi_async_fs::ReadTargetRegion::full(),
    ))
    .expect_err("活动覆盖期间普通读取必须立即冲突");
    assert_eq!(read_error.current_context(), &ErrorKind::Conflict);
    assert_eq!(read_buffer, [0xaa; 4]);
    let length_error = block_on(read_resource.byte_len())
        .expect_err("活动覆盖期间长度查询必须立即冲突");
    assert_eq!(length_error.current_context(), &ErrorKind::Conflict);

    let append_failure = block_on(append_resource.append(b"append".to_vec()))
        .expect_err("活动覆盖期间追加必须立即冲突");
    let (append_error, append_buffer, append_progress) =
        append_failure.into_parts();
    assert_eq!(append_error.current_context(), &ErrorKind::Conflict);
    assert_eq!(append_buffer, b"append");
    assert_eq!(append_progress, TransferProgress::Exact { bytes: 0 });

    let overwrite_failure = block_on(
        competing_overwrite.overwrite_all(b"other".to_vec()),
    )
    .expect_err("活动覆盖期间其它覆盖必须立即冲突");
    let (error, buffer, progress, evidence) = overwrite_failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(buffer, b"other");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let final_len = loop {
        match block_on(overwrite_resource.byte_len()) {
            Ok(length) => break length,
            Err(error) if error.current_context() == &ErrorKind::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "覆盖内部任务应在截止前完成"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => panic!("覆盖完成后的长度查询异常失败: {error:?}"),
        }
    };
    assert_eq!(final_len, PAYLOAD_LEN as u64);
    let content = std::fs::read(&path).expect("应能读取覆盖后的文件");
    assert_eq!(content.len(), PAYLOAD_LEN);
    assert!(content.iter().all(|byte| *byte == b'D'));
}
