//! 本地 `FileIo::append` 的真实文件、buffer 与并发集成测试。
//!
//! 覆盖 15 种内置拥有／借用 buffer 形态、空输入、角色错误、独立句柄串行化
//! 和取消后完成。9／16 MiB 时序用例消耗 I/O 与 worker，必须串行独占运行。

use std::borrow::Cow;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace,
    TransferProgress,
};
use pi_result::ErrorKind;

// 验证 Vec 成功追加严格保留原前缀、把完整输入放到文件尾，并让同一资源随后
// 观察到精确新长度。
#[test]
fn test_local_file_append_vec_preserves_existing_content_and_updates_length() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base:").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    block_on(file.append(b"vec".to_vec())).expect("完整 Vec 应严格追加成功");

    assert_eq!(std::fs::read(&path).unwrap(), b"base:vec");
    assert_eq!(block_on(file.byte_len()).unwrap(), 8);
}

// 验证 Vec、Box、Arc、Cow 两分支、Bytes、BytesMut 及其全部借用形态均能
// 依调用顺序完整追加；最终单一字节串证明每种静态泛型分支真实执行。
#[test]
fn test_local_file_append_supports_every_builtin_write_buffer_shape() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"start|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    block_on(file.append(b"vec|".to_vec())).expect("owned Vec 应可追加");
    block_on(file.append(Vec::from(&b"box|"[..]).into_boxed_slice()))
        .expect("owned Box<[u8]> 应可追加");
    block_on(file.append(Arc::<[u8]>::from(&b"arc|"[..])))
        .expect("owned Arc<[u8]> 应可追加");
    let cow_owned: Cow<'static, [u8]> = Cow::Owned(b"cow-owned|".to_vec());
    block_on(file.append(cow_owned)).expect("owned Cow 分支应可追加");
    let cow_borrowed: Cow<'_, [u8]> = Cow::Borrowed(b"cow-borrowed|");
    block_on(file.append(cow_borrowed)).expect("borrowed Cow 分支应可追加");
    block_on(file.append(Bytes::from_static(b"bytes|")))
        .expect("owned Bytes 应可追加");
    block_on(file.append(BytesMut::from(&b"bytes-mut|"[..])))
        .expect("owned BytesMut 应可追加");

    let slice = &b"slice-ref|"[..];
    block_on(file.append(slice)).expect("借用切片应可追加");
    let vec_buffer = b"vec-ref|".to_vec();
    block_on(file.append(&vec_buffer)).expect("借用 Vec 应可追加");
    let box_buffer = Vec::from(&b"box-ref|"[..]).into_boxed_slice();
    block_on(file.append(&box_buffer)).expect("借用 Box<[u8]> 应可追加");
    let arc_buffer = Arc::<[u8]>::from(&b"arc-ref|"[..]);
    block_on(file.append(&arc_buffer)).expect("借用 Arc<[u8]> 应可追加");
    let cow_buffer: Cow<'_, [u8]> = Cow::Borrowed(b"cow-ref|");
    block_on(file.append(&cow_buffer)).expect("借用 Cow 应可追加");
    let bytes_buffer = Bytes::from_static(b"bytes-ref|");
    block_on(file.append(&bytes_buffer)).expect("借用 Bytes 应可追加");
    let bytes_mut_buffer = BytesMut::from(&b"bytes-mut-ref|"[..]);
    block_on(file.append(&bytes_mut_buffer)).expect("借用 BytesMut 应可追加");

    assert_eq!(
        std::fs::read(&path).unwrap(),
        b"start|vec|box|arc|cow-owned|cow-borrowed|bytes|bytes-mut|\
          slice-ref|vec-ref|box-ref|arc-ref|cow-ref|bytes-ref|bytes-mut-ref|"
    );
}

// 验证五种非 Append 角色在任何写副作用前返回 InvalidState，并原样恢复 Vec
// 的指针、容量、内容和 Exact(0) 进度，文件保持不变。
#[test]
fn test_local_file_append_rejects_every_non_append_role_and_restores_original_vec() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let original = b"unchanged";
    std::fs::write(&path, original).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ] {
        let mut file = block_on(namespace.open(&path, access))
            .expect("每种非追加角色都应能独立打开");
        let mut buffer = Vec::with_capacity(32);
        buffer.extend_from_slice(b"payload");
        let original_pointer = buffer.as_ptr();
        let original_capacity = buffer.capacity();

        let failure = block_on(file.append(buffer))
            .expect_err("非追加角色必须在任何副作用前失败");
        let (error, buffer, progress) = failure.into_parts();

        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
        assert_eq!(buffer.as_ptr(), original_pointer);
        assert_eq!(buffer.capacity(), original_capacity);
        assert_eq!(buffer, b"payload");
        assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}

// 验证所有内置拥有／借用 buffer 的空值都是成功空操作，不改变既有文件内容，
// 同时覆盖 Cow 的拥有与借用空分支。
#[test]
fn test_local_file_append_empty_builtin_buffers_leave_file_unchanged() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"unchanged").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    block_on(file.append(Vec::<u8>::new())).unwrap();
    block_on(file.append(Vec::<u8>::new().into_boxed_slice())).unwrap();
    block_on(file.append(Arc::<[u8]>::from(&b""[..]))).unwrap();
    block_on(file.append(Cow::<'static, [u8]>::Owned(Vec::new()))).unwrap();
    block_on(file.append(Cow::<'_, [u8]>::Borrowed(b""))).unwrap();
    block_on(file.append(Bytes::new())).unwrap();
    block_on(file.append(BytesMut::new())).unwrap();

    let slice = &b""[..];
    block_on(file.append(slice)).unwrap();
    let vec_buffer = Vec::<u8>::new();
    block_on(file.append(&vec_buffer)).unwrap();
    let box_buffer = Vec::<u8>::new().into_boxed_slice();
    block_on(file.append(&box_buffer)).unwrap();
    let arc_buffer = Arc::<[u8]>::from(&b""[..]);
    block_on(file.append(&arc_buffer)).unwrap();
    let cow_buffer: Cow<'_, [u8]> = Cow::Borrowed(b"");
    block_on(file.append(&cow_buffer)).unwrap();
    let bytes_buffer = Bytes::new();
    block_on(file.append(&bytes_buffer)).unwrap();
    let bytes_mut_buffer = BytesMut::new();
    block_on(file.append(&bytes_mut_buffer)).unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"unchanged");
}

// 验证两个独立 Append 资源在不同线程同时追加 9 MiB 时按完整调用串行化；
// 最终长度和 A/B 两种允许顺序的精确模型排除任何字节穿插。
#[test]
fn test_local_file_independent_append_resources_serialize_complete_calls() {
    const PAYLOAD_LEN: usize = 9 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"start|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut first = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("第一个追加资源应能独立打开");
    let mut second = block_on(namespace.clone().open(&path, FileAccessMode::Append))
        .expect("第二个追加资源应能独立打开");
    let barrier = Arc::new(std::sync::Barrier::new(3));

    let first_barrier = Arc::clone(&barrier);
    let first_thread = std::thread::spawn(move || {
        let payload = vec![b'A'; PAYLOAD_LEN];
        first_barrier.wait();
        block_on(first.append(payload)).expect("第一个完整追加应成功");
    });
    let second_barrier = Arc::clone(&barrier);
    let second_thread = std::thread::spawn(move || {
        let payload = vec![b'B'; PAYLOAD_LEN];
        second_barrier.wait();
        block_on(second.append(payload)).expect("第二个完整追加应成功");
    });
    barrier.wait();
    first_thread.join().expect("第一个追加线程不应 panic");
    second_thread.join().expect("第二个追加线程不应 panic");

    let content = std::fs::read(&path).expect("应能读取最终文件");
    assert_eq!(content.len(), 6 + 2 * PAYLOAD_LEN);
    assert_eq!(&content[..6], b"start|");
    let tail = &content[6..];
    let first_then_second = tail[..PAYLOAD_LEN].iter().all(|byte| *byte == b'A')
        && tail[PAYLOAD_LEN..].iter().all(|byte| *byte == b'B');
    let second_then_first = tail[..PAYLOAD_LEN].iter().all(|byte| *byte == b'B')
        && tail[PAYLOAD_LEN..].iter().all(|byte| *byte == b'A');
    assert!(
        first_then_second || second_then_first,
        "同一稳定文件身份的公开追加调用不得相互穿插"
    );
}

// 验证丢弃已提交 16 MiB 追加 Future 不取消拥有型 actor：原资源等待归还后
// 可用，后续独立追加排在完整 payload 之后，最终字节与长度均精确。
#[test]
fn test_local_file_cancelled_append_keeps_actor_lease_and_restores_resource() {
    const PAYLOAD_LEN: usize = 16 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"start|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut cancelled_resource = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("被取消调用使用的追加资源应能打开");
    let mut following_resource = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("后续追加资源应能独立打开");

    let append = cancelled_resource.append(vec![b'C'; PAYLOAD_LEN]);
    let first_poll = block_on(future::poll_once(append));
    assert!(first_poll.is_none(), "大追加第一次轮询应仍在执行中");

    let observed_len = block_on(cancelled_resource.byte_len())
        .expect("取消后原资源应等待内部任务归还句柄并继续可用");
    assert_eq!(observed_len, 6 + PAYLOAD_LEN as u64);
    block_on(following_resource.append(b"tail".to_vec()))
        .expect("后续追加应等待先前内部任务终态后成功");

    let content = std::fs::read(&path).expect("应能读取最终文件");
    assert_eq!(content.len(), 6 + PAYLOAD_LEN + 4);
    assert_eq!(&content[..6], b"start|");
    assert!(content[6..6 + PAYLOAD_LEN]
        .iter()
        .all(|byte| *byte == b'C'));
    assert_eq!(&content[6 + PAYLOAD_LEN..], b"tail");
}
