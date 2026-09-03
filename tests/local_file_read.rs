//! 本地固定／增长、定位／顺序读取的真实 buffer 与取消集成测试。
//!
//! 覆盖数组、非定长切片、Vec、Box、BytesMut，目标窗口、EOF、溢出、游标和
//! 六类取消时序。32／64 MiB 用例使用真实 I/O 与截止，必须串行独占运行。

use bytes::BytesMut;
use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace,
    OverwriteTargetEvidence, ReadGrowthLimit, ReadGrowthOutcome,
    ReadTargetRegion, TransferProgress,
};
use pi_result::ErrorKind;

// 测试 helper 对同一四字节 `AsMut<[u8]>` 目标依次执行四个公开固定读取入口，
// 并逐次重置哨兵、验证完整字节；它不在测试侧实现读取或游标逻辑。
fn assert_local_fixed_read_success_matrix<B>(buffer: &mut B)
where
    B: AsMut<[u8]> + Send + ?Sized,
{
    assert_eq!(buffer.as_mut().len(), 4, "矩阵目标必须恰好为四字节");
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");

    buffer.as_mut().fill(0xaa);
    let read = block_on(file.read_at(
        1,
        buffer,
        ReadTargetRegion::full(),
    ))
    .expect("定位读取应支持该 buffer 类型");
    assert_eq!(read, 4);
    assert_eq!(buffer.as_mut(), b"1234");

    buffer.as_mut().fill(0xaa);
    block_on(file.read_exact_at(
        2,
        buffer,
        ReadTargetRegion::full(),
    ))
    .expect("精确定位读取应支持该 buffer 类型");
    assert_eq!(buffer.as_mut(), b"2345");

    buffer.as_mut().fill(0xaa);
    let read = block_on(file.read(buffer, ReadTargetRegion::full()))
        .expect("顺序读取应支持该 buffer 类型");
    assert_eq!(read, 4);
    assert_eq!(buffer.as_mut(), b"0123");

    buffer.as_mut().fill(0xaa);
    block_on(file.read_exact(buffer, ReadTargetRegion::full()))
        .expect("精确顺序读取应支持该 buffer 类型");
    assert_eq!(buffer.as_mut(), b"4567");
}

// 验证固定长度数组可组合用于定位／顺序及普通／精确四种读取，保护数组泛型
// 分支的成功行为和字节覆盖范围。
#[test]
fn test_local_fixed_read_success_matrix_for_array() {
    let mut buffer = [0xaa; 4];
    assert_local_fixed_read_success_matrix(&mut buffer);
}

// 验证非定长 `&mut [u8]` 可穿过 `?Sized` 泛型边界并完成四种固定读取，
// 防止实现意外要求 Sized 或拥有型 buffer。
#[test]
fn test_local_fixed_read_success_matrix_for_unsized_slice() {
    let mut storage = [0xaa; 4];
    assert_local_fixed_read_success_matrix(&mut storage[..]);
}

// 验证已初始化 Vec 作为固定读取目标时只覆盖其现有长度，不采用增长式语义，
// 并完成四种固定读取组合。
#[test]
fn test_local_fixed_read_success_matrix_for_vec() {
    let mut buffer = vec![0xaa; 4];
    assert_local_fixed_read_success_matrix(&mut buffer);
}

// 验证拥有型 `Box<[u8]>` 的现有切片可用于四种固定读取，覆盖无容量概念的
// 连续可变目标分支。
#[test]
fn test_local_fixed_read_success_matrix_for_boxed_slice() {
    let mut buffer: Box<[u8]> = vec![0xaa; 4].into_boxed_slice();
    assert_local_fixed_read_success_matrix(&mut buffer);
}

// 验证 `BytesMut` 的已初始化视图可用于四种固定读取，且该测试不依赖其备用
// 容量或增长接口。
#[test]
fn test_local_fixed_read_success_matrix_for_bytes_mut() {
    let mut buffer = BytesMut::from(&[0xaa; 4][..]);
    assert_local_fixed_read_success_matrix(&mut buffer);
}

// 验证取消等待 64 MiB `read_exact_at` 后，读取租约持续到后端静止：覆盖在此
// 前冲突并原样归还输入，截止前恢复后覆盖成功且被取消 buffer 不再被触碰。
#[test]
fn test_cancelled_local_file_read_keeps_lease_until_backend_quiesces() {
    const PAYLOAD_LEN: usize = 64 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, vec![b'R'; PAYLOAD_LEN])
        .expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut read_buffer = vec![0x55; PAYLOAD_LEN];

    let read = read_resource.read_exact_at(
        0,
        &mut read_buffer,
        ReadTargetRegion::full(),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大读取第一次轮询应仍在执行中");

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("取消后的底层读取停止前，竞争覆盖必须冲突");
    let (error, mut payload, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(payload, b"replacement");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        match block_on(overwrite_resource.overwrite_all(payload)) {
            Ok(()) => break,
            Err(failure) => {
                let (error, returned, progress, evidence) = failure.into_parts();
                assert_eq!(error.current_context(), &ErrorKind::Conflict);
                assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
                assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);
                assert!(
                    std::time::Instant::now() < deadline,
                    "取消后的读取完成所有者应在截止前停止底层读取"
                );
                payload = returned;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    assert!(read_buffer.iter().all(|byte| *byte == 0x55));
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证取消 32 MiB 非精确定位读取后，原资源的后续观察会等待预读停止，目标
// buffer 保持哨兵，随后覆盖可成功，证明后台不再持有调用方借用。
#[test]
fn test_cancelled_local_file_read_at_stops_touching_buffer_and_recovers() {
    const PAYLOAD_LEN: usize = 32 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, vec![b'R'; PAYLOAD_LEN])
        .expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut read_buffer = vec![0x55; PAYLOAD_LEN];

    let read = read_resource.read_at(
        0,
        &mut read_buffer,
        ReadTargetRegion::full(),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大定位读取第一次轮询应仍在执行中");

    block_on(read_resource.byte_len())
        .expect("原读取资源应等待内部预读停止后恢复可用");
    block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect("读取资源恢复后覆盖应成功");

    assert!(read_buffer.iter().all(|byte| *byte == 0x55));
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证取消 32 MiB 定位增长读取后，竞争覆盖在后端停止前冲突；资源恢复后可
// 重试覆盖，增长 buffer 至少保留原前缀且不会在函数返回后继续变化。
#[test]
fn test_cancelled_local_file_read_to_end_at_keeps_lease_until_backend_quiesces() {
    const PAYLOAD_LEN: usize = 32 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, vec![b'R'; PAYLOAD_LEN])
        .expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut growth_buffer = b"prefix|".to_vec();

    let read = read_resource.read_to_end_at(
        0,
        &mut growth_buffer,
        ReadGrowthLimit::new(PAYLOAD_LEN),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大定位增长读取第一次轮询应仍在执行中");

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("取消后的定位增长读取停止前，竞争覆盖必须冲突");
    let (error, payload, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    block_on(read_resource.byte_len())
        .expect("原读取资源应等待内部预读停止后恢复可用");
    block_on(overwrite_resource.overwrite_all(payload))
        .expect("内部预读停止后覆盖应可重试");

    assert!(growth_buffer.starts_with(b"prefix|"));
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证取消 32 MiB 普通顺序读取不会提交游标或改写调用方 buffer；资源恢复后
// 从起点重读 RRRR，再允许竞争覆盖。
#[test]
fn test_cancelled_local_file_sequential_read_keeps_cursor_and_buffer_safe() {
    const PAYLOAD_LEN: usize = 32 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, vec![b'R'; PAYLOAD_LEN])
        .expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let mut read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut read_buffer = vec![0x55; PAYLOAD_LEN];

    let read = read_resource.read(
        &mut read_buffer,
        ReadTargetRegion::full(),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大顺序读取第一次轮询应仍在执行中");

    block_on(read_resource.byte_len())
        .expect("原读取资源应等待内部预读停止后恢复可用");
    let mut retry = [0xaa; 4];
    let retried = block_on(read_resource.read(
        &mut retry,
        ReadTargetRegion::full(),
    ))
    .expect("取消后的顺序游标应仍能从起点重试");
    assert_eq!(retried, 4);
    assert_eq!(&retry, b"RRRR");

    block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect("读取资源恢复后覆盖应成功");
    assert!(read_buffer.iter().all(|byte| *byte == 0x55));
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证取消 64 MiB 精确顺序读取期间排它覆盖冲突，后端静止后游标仍在起点、
// buffer 保持哨兵，重读和覆盖按顺序成功。
#[test]
fn test_cancelled_local_file_read_exact_keeps_lease_and_cursor() {
    const PAYLOAD_LEN: usize = 64 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, vec![b'R'; PAYLOAD_LEN])
        .expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let mut read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut read_buffer = vec![0x55; PAYLOAD_LEN];

    let read = read_resource.read_exact(
        &mut read_buffer,
        ReadTargetRegion::full(),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大顺序精确读取第一次轮询应仍在执行中");

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("取消后的顺序精确读取停止前，竞争覆盖必须冲突");
    let (error, payload, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    block_on(read_resource.byte_len())
        .expect("原读取资源应等待内部预读停止后恢复可用");
    let mut retry = [0xaa; 4];
    block_on(read_resource.read_exact(
        &mut retry,
        ReadTargetRegion::full(),
    ))
    .expect("取消后的顺序精确读取应从原游标重试");
    assert_eq!(&retry, b"RRRR");

    block_on(overwrite_resource.overwrite_all(payload))
        .expect("内部预读停止后覆盖应可重试");
    assert!(read_buffer.iter().all(|byte| *byte == 0x55));
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证取消 32 MiB 顺序增长读取只成对提交已追加 buffer 字节与游标；恢复后
// 下一次读取精确接续 `appended` 位置，不能出现二者进度分离。
#[test]
fn test_cancelled_local_file_read_to_end_keeps_lease_and_paired_cursor() {
    const PAYLOAD_LEN: usize = 32 * 1024 * 1024;
    const PREFIX: &[u8] = b"prefix|";

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    let source: Vec<u8> = (0..PAYLOAD_LEN)
        .map(|index| (index % 251) as u8)
        .collect();
    std::fs::write(&path, &source).expect("应能建立真实大文件");
    let namespace = LocalFileNamespace::new();
    let mut read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut growth_buffer = PREFIX.to_vec();

    let read = read_resource.read_to_end(
        &mut growth_buffer,
        ReadGrowthLimit::new(PAYLOAD_LEN),
    );
    let first_poll = block_on(future::poll_once(read));
    assert!(first_poll.is_none(), "大顺序增长读取第一次轮询应仍在执行中");

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("取消后的顺序增长读取停止前，竞争覆盖必须冲突");
    let (error, payload, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    block_on(read_resource.byte_len())
        .expect("原读取资源应等待内部预读停止后恢复可用");
    assert!(growth_buffer.starts_with(PREFIX));
    let appended = growth_buffer.len() - PREFIX.len();
    assert!(appended < PAYLOAD_LEN, "被取消调用不应已经读完整个大文件");
    let mut retry = [0xaa; 4];
    let retried = block_on(read_resource.read(
        &mut retry,
        ReadTargetRegion::full(),
    ))
    .expect("取消后的顺序增长读取应从成对提交位置继续");
    assert_eq!(retried, 4);
    assert_eq!(&retry, &source[appended..appended + 4]);

    block_on(overwrite_resource.overwrite_all(payload))
        .expect("内部预读停止后覆盖应可重试");
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
}

// 验证 `read_at` 从绝对偏移 3 只覆盖八字节数组的 `[2, 6)` 目标窗口，返回
// 四字节并保持窗口外哨兵不变。
#[test]
fn test_local_file_read_at_reads_absolute_offset_into_only_target_window() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = [0xaa; 8];
    let target = ReadTargetRegion::try_from(2..6).expect("目标窗口应合法");

    let read = block_on(file.read_at(3, &mut buffer, target))
        .expect("定位读取应成功");

    assert_eq!(read, 4);
    assert_eq!(buffer, [0xaa, 0xaa, b'3', b'4', b'5', b'6', 0xaa, 0xaa]);
}

// 验证 Append、Overwrite、两种 Mmap、Truncate 五种角色在修改 Vec 前拒绝
// `read_at`，全部返回 InvalidState 且哨兵内容不变。
#[test]
fn test_local_file_read_at_rejects_every_non_read_role_before_modifying_vec() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"content").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ] {
        let file = block_on(namespace.open(&path, access))
            .expect("非读取角色资源也应能独立打开");
        let mut buffer = vec![0xaa; 7];
        let error = block_on(file.read_at(
            0,
            &mut buffer,
            ReadTargetRegion::full(),
        ))
        .expect_err("非读取角色不得执行定位读取");

        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
        assert_eq!(buffer, vec![0xaa; 7]);
    }
}

// 验证 `BytesMut` 目标窗口超过已初始化长度时在切片／I/O 前返回 InvalidInput，
// 原五字节内容保持完整。
#[test]
fn test_local_file_read_at_rejects_out_of_bounds_bytes_mut_before_modification() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = BytesMut::from(&b"guard"[..]);
    let target = ReadTargetRegion::try_from(2..6).expect("范围结构应合法");

    let error = block_on(file.read_at(0, &mut buffer, target))
        .expect_err("超出已初始化视图的窗口必须失败");

    assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
    assert_eq!(&buffer[..], b"guard");
}

// 验证 Box 的合法空目标窗口即使源偏移为 `u64::MAX` 也成功返回零且不定位
// 文件，全部哨兵保持不变。
#[test]
fn test_local_file_read_at_empty_boxed_window_succeeds_without_seeking() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"content").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer: Box<[u8]> = vec![0xaa; 5].into_boxed_slice();
    let target = ReadTargetRegion::try_from(3..3).expect("空窗口应合法");

    let read = block_on(file.read_at(u64::MAX, &mut buffer, target))
        .expect("空窗口不得访问或定位底层文件");

    assert_eq!(read, 0);
    assert_eq!(&*buffer, &[0xaa; 5]);
}

// 验证源偏移 `u64::MAX` 加非空切片窗口发生逻辑溢出时返回 InvalidInput，
// 且在任何文件访问和 buffer 修改前失败。
#[test]
fn test_local_file_read_at_rejects_source_range_overflow_for_slice() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"content").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut storage = [0xaa; 4];
    let buffer: &mut [u8] = &mut storage;
    let target = ReadTargetRegion::try_from(1..3).expect("目标窗口应合法");

    let error = block_on(file.read_at(u64::MAX, buffer, target))
        .expect_err("源偏移加目标长度溢出必须失败");

    assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
    assert_eq!(storage, [0xaa; 4]);
}

// 验证允许短读的定位读取在文件尾和尾后均成功返回零，并分别保持 BytesMut
// 目标不变，不把 EOF 当作普通错误。
#[test]
fn test_local_file_read_at_returns_zero_at_and_after_end_without_modification() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"four").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");

    for offset in [4, 9] {
        let mut buffer = BytesMut::from(&b"guard"[..]);
        let read = block_on(file.read_at(
            offset,
            &mut buffer,
            ReadTargetRegion::full(),
        ))
        .expect("文件尾及其后定位读取应成功");

        assert_eq!(read, 0);
        assert_eq!(&buffer[..], b"guard");
    }
}

// 验证非精确定位读取靠近 EOF 时只写入实际两字节，返回 2，并保持目标窗口
// 未填充后缀与窗口外字节为原哨兵。
#[test]
fn test_local_file_read_at_short_read_preserves_unfilled_window_suffix() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"abc").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer: Box<[u8]> = vec![0xaa; 7].into_boxed_slice();
    let target = ReadTargetRegion::try_from(1..6).expect("目标窗口应合法");

    let read = block_on(file.read_at(1, &mut buffer, target))
        .expect("允许短读的定位读取应成功");

    assert_eq!(read, 2);
    assert_eq!(&*buffer, &[0xaa, b'b', b'c', 0xaa, 0xaa, 0xaa, 0xaa]);
}

// 验证 `read_exact_at` 从偏移 3 填满 Vec 的 `[2, 7)` 五字节窗口，同时保持
// 头部和尾部窗口外哨兵不变。
#[test]
fn test_local_file_read_exact_at_fills_entire_vec_target_window() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = vec![0xaa; 8];
    let target = ReadTargetRegion::try_from(2..7).expect("目标窗口应合法");

    block_on(file.read_exact_at(3, &mut buffer, target))
        .expect("精确定位读取应填满目标窗口");

    assert_eq!(
        buffer,
        vec![0xaa, 0xaa, b'3', b'4', b'5', b'6', b'7', 0xaa]
    );
}

// 验证精确定位读取遇到 EOF 返回 I/O 错误，且明确保护目标窗口之外首尾 guard；
// 窗口内可能已有提交前缀，不错误断言事务回滚。
#[test]
fn test_local_file_read_exact_at_reports_end_of_file_without_touching_outside_window() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"abc").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = BytesMut::from(&b"guards!"[..]);
    let target = ReadTargetRegion::try_from(1..6).expect("目标窗口应合法");

    let error = block_on(file.read_exact_at(1, &mut buffer, target))
        .expect_err("文件剩余内容不足时精确读取必须失败");

    assert_eq!(error.current_context(), &ErrorKind::Io);
    assert_eq!(buffer[0], b'g');
    assert_eq!(buffer[6], b'!');
}

// 验证 Vec 增长读取从偏移 2 只追加四字节上限，保留既有前缀并返回
// `LimitReached { appended_bytes: 4 }`。
#[test]
fn test_local_file_read_to_end_at_appends_to_vec_until_limit() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = b"prefix:".to_vec();

    let outcome = block_on(file.read_to_end_at(
        2,
        &mut buffer,
        ReadGrowthLimit::new(4),
    ))
    .expect("定位增长读取应成功");

    assert_eq!(
        outcome,
        ReadGrowthOutcome::LimitReached { appended_bytes: 4 }
    );
    assert_eq!(buffer, b"prefix:2345");
}

// 验证 BytesMut 增长读取从偏移 4 追加剩余两字节并在上限前观察 EOF，返回
// `EndOfFile { appended_bytes: 2 }` 且保留原前缀。
#[test]
fn test_local_file_read_to_end_at_appends_to_bytes_mut_until_end_of_file() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"abcdef").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = BytesMut::from(&b"prefix:"[..]);

    let outcome = block_on(file.read_to_end_at(
        4,
        &mut buffer,
        ReadGrowthLimit::new(8),
    ))
    .expect("定位增长读取应在限额内观察到文件尾");

    assert_eq!(
        outcome,
        ReadGrowthOutcome::EndOfFile { appended_bytes: 2 }
    );
    assert_eq!(&buffer[..], b"prefix:ef");
}

// 验证增长上限为零时即使偏移 `u64::MAX` 也不执行定位、I/O 或扩容，Vec 的
// 指针语义由容量不变侧面保护，结果为零字节 LimitReached。
#[test]
fn test_local_file_read_to_end_at_zero_limit_is_no_io_no_growth() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"content").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = Vec::with_capacity(7);
    buffer.extend_from_slice(b"prefix:");
    let initial_capacity = buffer.capacity();

    let outcome = block_on(file.read_to_end_at(
        u64::MAX,
        &mut buffer,
        ReadGrowthLimit::new(0),
    ))
    .expect("零上限不得定位或读取底层文件");

    assert_eq!(
        outcome,
        ReadGrowthOutcome::LimitReached { appended_bytes: 0 }
    );
    assert_eq!(buffer, b"prefix:");
    assert_eq!(buffer.capacity(), initial_capacity);
}

// 验证 `usize::MAX` 只是允许增长的逻辑上限，不要求预先分配等量内存；两字节
// 文件正常读到 EOF，实际 buffer 仅增长两字节。
#[test]
fn test_local_file_read_to_end_at_maximum_limit_does_not_require_maximum_allocation() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"xy").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut buffer = Vec::new();

    let outcome = block_on(file.read_to_end_at(
        0,
        &mut buffer,
        ReadGrowthLimit::new(usize::MAX),
    ))
    .expect("最大逻辑上限配合短文件不应要求等量预分配");

    assert_eq!(
        outcome,
        ReadGrowthOutcome::EndOfFile { appended_bytes: 2 }
    );
    assert_eq!(buffer, b"xy");
}

// 验证绝对定位读取不改变顺序游标：先读取偏移 7 的 78，随后两次顺序读取仍
// 从 0 开始并依次得到 012、345。
#[test]
fn test_local_file_positional_and_sequential_reads_keep_independent_positions() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");

    let mut positional = [0xaa; 2];
    let positional_read = block_on(file.read_at(
        7,
        &mut positional,
        ReadTargetRegion::full(),
    ))
    .expect("定位读取应成功");
    let mut first = [0xaa; 3];
    let first_read = block_on(file.read(
        &mut first,
        ReadTargetRegion::full(),
    ))
    .expect("第一次顺序读取应成功");
    let mut second = vec![0xaa; 3];
    let second_read = block_on(file.read(
        &mut second,
        ReadTargetRegion::full(),
    ))
    .expect("第二次顺序读取应成功");

    assert_eq!(positional_read, 2);
    assert_eq!(positional, *b"78");
    assert_eq!(first_read, 3);
    assert_eq!(first, *b"012");
    assert_eq!(second_read, 3);
    assert_eq!(second, b"345");
}

// 验证精确顺序读取因 EOF 失败时不提交任何顺序游标进度；下一次普通读取从
// 原起点得到 ab，窗口外／失败中间内容不被当作已提交位置。
#[test]
fn test_local_file_read_exact_failure_keeps_sequential_position() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"abcd").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut oversized = BytesMut::from(&b"guards"[..]);

    let error = block_on(file.read_exact(
        &mut oversized,
        ReadTargetRegion::full(),
    ))
    .expect_err("精确顺序读取遇到文件尾必须失败");
    let mut retry = [0xaa; 2];
    let read = block_on(file.read(
        &mut retry,
        ReadTargetRegion::full(),
    ))
    .expect("失败后的顺序读取应从原游标重试");

    assert_eq!(error.current_context(), &ErrorKind::Io);
    assert_eq!(read, 2);
    assert_eq!(retry, *b"ab");
}

// 验证两次顺序增长读取把 buffer 追加与游标推进成对提交：第一次限两字节得
// 01，第二次从位置 2 读到 EOF 得 2345，分别支持 Vec 与 BytesMut。
#[test]
fn test_local_file_sequential_growth_pairs_buffer_and_position_progress() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"012345").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("只读资源应能打开");
    let mut first = b"first:".to_vec();
    let first_outcome = block_on(file.read_to_end(
        &mut first,
        ReadGrowthLimit::new(2),
    ))
    .expect("第一次顺序增长读取应成功");
    let mut second = BytesMut::from(&b"second:"[..]);
    let second_outcome = block_on(file.read_to_end(
        &mut second,
        ReadGrowthLimit::new(8),
    ))
    .expect("第二次顺序增长读取应从已提交游标继续");

    assert_eq!(
        first_outcome,
        ReadGrowthOutcome::LimitReached { appended_bytes: 2 }
    );
    assert_eq!(first, b"first:01");
    assert_eq!(
        second_outcome,
        ReadGrowthOutcome::EndOfFile { appended_bytes: 4 }
    );
    assert_eq!(&second[..], b"second:2345");
}
