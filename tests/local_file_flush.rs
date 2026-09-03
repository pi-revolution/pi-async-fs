//! 本地 `FileIo::flush` 的角色、顺序、取消与强制刷新集成测试。
//!
//! 覆盖 Data／DataAndMetadata、允许／拒绝角色、懒执行、重复调用和取消期互斥。
//! 16／128 MiB 时序用例依赖真实 I/O 与 worker，完整文件必须串行独占运行。

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace,
    LocalFile, LocalFileNamespace, OverwriteTargetEvidence,
    ReadTargetRegion, TransferProgress,
};
use pi_result::ErrorKind;

#[derive(Clone, Copy)]
enum AllowedFlushRole {
    Append,
    Overwrite,
    Truncate,
}

// 测试 helper 通过公开 API 为三种允许角色制造真实修改，再执行指定刷新模式并
// 从路径复核内容；它不模拟刷新或持久化，仅减少六个正向组合的夹具重复。
fn assert_flush_succeeds(
    role: AllowedFlushRole,
    mode: FileFlushMode,
) {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"initial").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let access = match role {
        AllowedFlushRole::Append => FileAccessMode::Append,
        AllowedFlushRole::Overwrite => FileAccessMode::Overwrite,
        AllowedFlushRole::Truncate => FileAccessMode::Truncate,
    };
    let mut file: LocalFile = block_on(namespace.open(&path, access))
        .expect("允许强刷新的资源应能打开");

    let expected = match role {
        AllowedFlushRole::Append => {
            block_on(file.append(b"|append".to_vec()))
                .expect("刷新前追加应成功");
            &b"initial|append"[..]
        }
        AllowedFlushRole::Overwrite => {
            block_on(file.overwrite_all(b"overwrite".to_vec()))
                .expect("刷新前覆盖应成功");
            &b"overwrite"[..]
        }
        AllowedFlushRole::Truncate => {
            block_on(file.truncate(3)).expect("刷新前截断应成功");
            &b"ini"[..]
        }
    };

    block_on(file.flush(mode)).expect("所选角色与刷新等级应成功");
    assert_eq!(std::fs::read(&path).unwrap(), expected);
}

// 验证 Data 刷新形成等待完成的顺序屏障，但不会关闭 Append 资源；同一资源
// 可继续追加，最终字节严格为刷新前后两段的调用顺序。
#[test]
fn test_local_file_flush_data_after_append_keeps_resource_usable() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    block_on(file.append(b"before|".to_vec())).expect("屏障前追加应成功");
    block_on(file.flush(FileFlushMode::Data))
        .expect("数据刷新屏障应等待真实完成");
    block_on(file.append(b"after".to_vec()))
        .expect("刷新完成后同一资源应继续可用");

    assert_eq!(std::fs::read(&path).unwrap(), b"base|before|after");
}

// 验证 Append、Overwrite、Truncate 与 Data、DataAndMetadata 的六个允许
// 组合全部通过真实修改和真实刷新，不遗漏任一静态角色分支。
#[test]
fn test_local_file_flush_supports_both_modes_for_every_allowed_role() {
    for role in [
        AllowedFlushRole::Append,
        AllowedFlushRole::Overwrite,
        AllowedFlushRole::Truncate,
    ] {
        assert_flush_succeeds(role, FileFlushMode::Data);
        assert_flush_succeeds(role, FileFlushMode::DataAndMetadata);
    }
}

// 验证 Read 与两种 Mmap 角色对两种刷新模式均在副作用前返回 InvalidState，
// 真实文件内容在全部六个拒绝组合后保持不变。
#[test]
fn test_local_file_flush_rejects_every_disallowed_role_before_side_effects() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let original = b"unchanged";
    std::fs::write(&path, original).expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
    ] {
        let mut file = block_on(namespace.open(&path, access))
            .expect("每种禁止刷新角色都应能独立打开");

        for mode in [FileFlushMode::Data, FileFlushMode::DataAndMetadata] {
            let error = block_on(file.flush(mode))
                .expect_err("禁止角色必须在强刷新前失败");
            assert_eq!(error.current_context(), &ErrorKind::InvalidState);
            assert_eq!(std::fs::read(&path).unwrap(), original);
        }
    }
}

// 验证一个独立 Append 资源完成的刷新能为同一文件另一个追加资源建立顺序
// 屏障：before 已进入屏障，after 仅在屏障返回后提交。
#[test]
fn test_local_file_flush_orders_independently_opened_append_resources() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut writer = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");
    let mut barrier = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("独立刷新资源应能打开");

    block_on(writer.append(b"before|".to_vec()))
        .expect("线性化点之前的追加应完成");
    block_on(barrier.flush(FileFlushMode::DataAndMetadata))
        .expect("独立句柄应能为同一稳定文件建立强刷新屏障");
    block_on(writer.append(b"after".to_vec()))
        .expect("屏障之后的追加应继续成功");

    assert_eq!(std::fs::read(&path).unwrap(), b"base|before|after");
}

// 验证构造后从未 poll 的 flush Future 不取得租约也不产生 I/O 副作用；丢弃
// 后同一资源的追加立即成功，保护异步懒执行合同。
#[test]
fn test_local_file_unpolled_flush_has_no_lease_or_side_effect() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    let unpolled = file.flush(FileFlushMode::Data);
    drop(unpolled);
    block_on(file.append(b"append".to_vec()))
        .expect("未轮询的刷新不得阻挡后续追加");

    assert_eq!(std::fs::read(&path).unwrap(), b"base|append");
}

// 验证没有新写入时重复 Data 和 DataAndMetadata 刷新都可安全成功且不改变
// 文件内容；只断言内容幂等，不声称系统调用次数或耗时幂等。
#[test]
fn test_local_file_repeated_flush_without_new_writes_is_safe() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"unchanged").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut file = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");

    block_on(file.flush(FileFlushMode::Data)).expect("首次数据刷新应成功");
    block_on(file.flush(FileFlushMode::Data)).expect("重复数据刷新应成功");
    block_on(file.flush(FileFlushMode::DataAndMetadata))
        .expect("首次完整刷新应成功");
    block_on(file.flush(FileFlushMode::DataAndMetadata))
        .expect("重复完整刷新应成功");

    assert_eq!(std::fs::read(&path).unwrap(), b"unchanged");
}

// 验证 16 MiB 追加 Future 被取消等待后，追加 actor 完成前刷新立即冲突；
// 原资源确认 actor 完成后，同一刷新可重试成功。
#[test]
fn test_local_file_flush_conflicts_with_cancelled_append_until_actor_finishes() {
    const PAYLOAD_LEN: usize = 16 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut writer = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("追加资源应能打开");
    let mut barrier = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("刷新资源应能独立打开");

    let append = writer.append(vec![b'A'; PAYLOAD_LEN]);
    let first_poll = block_on(future::poll_once(append));
    assert!(first_poll.is_none(), "大追加第一次轮询应仍在执行中");

    let error = block_on(barrier.flush(FileFlushMode::Data))
        .expect_err("追加 actor 收尾前强刷新必须立即冲突");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);

    assert_eq!(
        block_on(writer.byte_len()).expect("原资源应等待追加 actor 归还文件句柄"),
        5 + PAYLOAD_LEN as u64,
    );
    block_on(barrier.flush(FileFlushMode::Data))
        .expect("追加 actor 完成后强刷新应可重试");
}

// 验证取消 128 MiB 文件的 DataAndMetadata 刷新等待者不会提前释放排它租约：
// 读、长度、追加、覆盖、截断和第二刷新均冲突，截止前完成后操作恢复。
#[test]
fn test_cancelled_local_file_flush_keeps_exclusive_lease_until_sync_finishes() {
    const PAYLOAD_LEN: usize = 128 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut dirty_writer = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("准备脏数据的追加资源应能打开");
    block_on(dirty_writer.append(vec![b'D'; PAYLOAD_LEN]))
        .expect("强刷新前的大追加应完成写管线排空");

    let mut flush_resource = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("强刷新资源应能打开");
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("读取资源应能独立打开");
    let mut append_resource = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("竞争追加资源应能独立打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("竞争覆盖资源应能独立打开");
    let mut truncate_resource = block_on(namespace.open(&path, FileAccessMode::Truncate))
        .expect("竞争截断资源应能独立打开");
    let mut competing_flush = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("竞争刷新资源应能独立打开");

    let flush = flush_resource.flush(FileFlushMode::DataAndMetadata);
    let first_poll = block_on(future::poll_once(flush));
    assert!(first_poll.is_none(), "大文件强刷新第一次轮询应仍在执行中");

    let mut read_buffer = [0x55; 4];
    let read_error = block_on(read_resource.read_at(
        0,
        &mut read_buffer,
        ReadTargetRegion::full(),
    ))
    .expect_err("强刷新 actor 活动期间普通读取必须冲突");
    assert_eq!(read_error.current_context(), &ErrorKind::Conflict);
    assert_eq!(read_buffer, [0x55; 4]);

    let length_error = block_on(read_resource.byte_len())
        .expect_err("强刷新 actor 活动期间长度查询必须冲突");
    assert_eq!(length_error.current_context(), &ErrorKind::Conflict);

    let append_failure = block_on(append_resource.append(b"append".to_vec()))
        .expect_err("强刷新 actor 活动期间追加必须冲突");
    let (append_error, append_buffer, append_progress) =
        append_failure.into_parts();
    assert_eq!(append_error.current_context(), &ErrorKind::Conflict);
    assert_eq!(append_buffer, b"append");
    assert_eq!(append_progress, TransferProgress::Exact { bytes: 0 });

    let overwrite_failure = block_on(
        overwrite_resource.overwrite_all(b"overwrite".to_vec()),
    )
    .expect_err("强刷新 actor 活动期间覆盖必须冲突");
    let (error, buffer, progress, evidence) = overwrite_failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(buffer, b"overwrite");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    let truncate_error = block_on(truncate_resource.truncate(0))
        .expect_err("强刷新 actor 活动期间截断必须冲突");
    assert_eq!(truncate_error.current_context(), &ErrorKind::Conflict);

    let flush_error = block_on(competing_flush.flush(FileFlushMode::Data))
        .expect_err("强刷新 actor 活动期间第二次刷新必须冲突");
    assert_eq!(flush_error.current_context(), &ErrorKind::Conflict);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let final_len = loop {
        match block_on(flush_resource.byte_len()) {
            Ok(length) => break length,
            Err(error) if error.current_context() == &ErrorKind::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "强刷新内部任务应在截止前完成"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => panic!("强刷新完成后的长度查询异常失败: {error:?}"),
        }
    };
    assert_eq!(final_len, 5 + PAYLOAD_LEN as u64);
    block_on(append_resource.append(b"tail".to_vec()))
        .expect("强刷新 actor 完成后竞争追加应可重试");
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        5 + PAYLOAD_LEN as u64 + 4,
    );
}
