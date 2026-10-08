//! 本地 `FileIo::flush` 的角色、顺序、取消与强制刷新集成测试。
//!
//! 覆盖 Data／DataAndMetadata、允许／拒绝角色、懒执行、重复调用和取消期互斥。
//! 追加取消用例依赖真实 I/O；刷新取消使用隔离进程中的真实单 worker 执行池
//! 控制任务排队。完整文件必须串行独占运行，不能与基准或其它压力任务并发。

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace,
    LocalFile, LocalFileNamespace, MmapRange, OverwriteTargetEvidence,
    ReadTargetRegion, TransferProgress,
};
use pi_result::ErrorKind;

#[derive(Clone, Copy)]
enum AllowedFlushRole {
    Append,
    Overwrite,
    Truncate,
    ReadWriteMmap,
}

// 测试 helper 通过公开 API 为四种允许角色制造真实修改，再执行指定刷新模式并
// 从路径复核内容；它不模拟刷新或持久化，仅减少八个正向组合的夹具重复。
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
        AllowedFlushRole::ReadWriteMmap => FileAccessMode::ReadWriteMmap,
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
        AllowedFlushRole::ReadWriteMmap => {
            // SAFETY: 夹具独占目标文件，修改严格局限于已有七字节范围。
            let mut mapping = unsafe {
                block_on(file.map_read_write_uncoordinated(MmapRange::new(0, 7).unwrap()))
            }
            .expect("刷新前应能建立真实可写映射");
            mapping.as_bytes_mut().copy_from_slice(b"mapped!");
            block_on(mapping.flush()).expect("映射修改须取得自己的完成证据");
            &b"mapped!"[..]
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

// B2H-DES-001：只读映射及其克隆保活期间，独立追加资源完成追加后能够等待
// 两种普通强刷新；刷新不改旧视图、不解除映射，也不使资源失去后续追加能力。
#[test]
fn test_local_file_flush_coexists_with_live_read_only_mapping_after_append() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mapper = block_on(namespace.open(&path, FileAccessMode::ReadMmap))
        .expect("只读映射资源应能打开");
    let mut writer = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("独立追加资源应能打开");
    // SAFETY: 独占临时文件；所有内容修改仅通过本库受管追加，不截断映射范围。
    let mapping = unsafe {
        block_on(mapper.map_read_only_uncoordinated(
            MmapRange::new(2, 7).expect("固定映射范围应有效"),
        ))
    }
    .expect("只读映射应能建立");
    let clone = mapping.clone();
    drop(mapping);

    block_on(writer.append(b"XY".to_vec())).expect("映射期间应允许严格追加");
    block_on(writer.flush(FileFlushMode::Data))
        .expect("活动只读映射不能阻止追加内容的数据刷新");
    block_on(writer.flush(FileFlushMode::DataAndMetadata))
        .expect("活动只读映射不能阻止数据及元信息刷新");
    assert_eq!(clone.as_bytes(), b"23456");
    block_on(writer.append(b"Z".to_vec())).expect("刷新后应能继续追加");
    block_on(writer.flush(FileFlushMode::DataAndMetadata))
        .expect("再次追加后可再次取得同步完成证据");
    assert_eq!(clone.as_bytes(), b"23456");
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789XYZ");
}

// B2H-DES-001：可写映射修改后，其 ReadWriteMmap 文件资源可以执行普通文件
// 刷新；映射专属完成证据仍通过映射 flush 取得，不从文件刷新成功推导它。
#[test]
fn test_read_write_mmap_resource_supports_file_flush_with_live_mapping() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap))
        .expect("读写映射资源应能打开");
    // SAFETY: 测试独占目标文件，仅通过返回的独占透明句柄修改映射范围。
    let mut mapping = unsafe {
        block_on(mapper.map_read_write_uncoordinated(
            MmapRange::new(2, 7).expect("固定映射范围应有效"),
        ))
    }
    .expect("读写映射应能建立");
    mapping.as_bytes_mut().copy_from_slice(b"ABCDE");
    block_on(mapper.flush(FileFlushMode::Data))
        .expect("映射写后应允许通过读写文件资源调用普通数据刷新");
    assert_eq!(mapping.as_bytes(), b"ABCDE");
    block_on(mapping.flush()).expect("映射自身应能取得同步完成证据");
    block_on(mapper.flush(FileFlushMode::DataAndMetadata))
        .expect("映射存活时也应允许文件数据及元信息刷新");
    assert_eq!(mapping.as_bytes(), b"ABCDE");
    assert_eq!(std::fs::read(&path).unwrap(), b"01ABCDE789");
}

// 验证 Append、Overwrite、Truncate、ReadWriteMmap 与两种刷新等级的八个允许
// 组合全部通过真实修改和真实刷新，不遗漏任一静态角色分支。
#[test]
fn test_local_file_flush_supports_both_modes_for_every_allowed_role() {
    for role in [
        AllowedFlushRole::Append,
        AllowedFlushRole::Overwrite,
        AllowedFlushRole::Truncate,
        AllowedFlushRole::ReadWriteMmap,
    ] {
        assert_flush_succeeds(role, FileFlushMode::Data);
        assert_flush_succeeds(role, FileFlushMode::DataAndMetadata);
    }
}

// 验证两个只读角色对两种刷新模式均在副作用前返回 InvalidState；允许活动
// 映射与其它可写资源刷新共存，不等于给只读文件资源增加刷新权限。
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

// B2H-DES-001：取消一个独立资源的追加等待后，另一资源仍可刷新；该次刷新
// 不替尚未完成的追加提供完成证据，等待追加资源归位后应再刷新并核验精确字节。
#[test]
fn test_local_file_flush_coexists_with_cancelled_append_then_can_flush_completion() {
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

    block_on(barrier.flush(FileFlushMode::Data))
        .expect("独立资源尚有追加进行时不应自动拒绝普通刷新");

    assert_eq!(
        block_on(writer.byte_len()).expect("原资源应等待追加 actor 归还文件句柄"),
        5 + PAYLOAD_LEN as u64,
    );
    block_on(barrier.flush(FileFlushMode::Data))
        .expect("追加完成后必须再次刷新以取得该追加的同步完成证据");
    let bytes = std::fs::read(&path).expect("应能独立核验最终追加内容");
    assert_eq!(&bytes[..5], b"base|");
    assert_eq!(bytes.len(), 5 + PAYLOAD_LEN);
    assert!(bytes[5..].iter().all(|byte| *byte == b'A'));
}

// B2H-DES-001：真实单 worker 池暂时忙碌时，刷新已经移交后台但未结束；取消
// 等待不能提前放行覆盖/截断。该场景不依赖文件大小或磁盘速度猜测在途状态。
#[test]
fn test_cancelled_local_file_flush_protects_whole_file_operations_until_sync_finishes() {
    const CHILD_MODE: &str = "PI_ASYNC_FS_FLUSH_CANCELLATION_CHILD";
    if std::env::var(CHILD_MODE).as_deref() != Ok("active") {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("test_cancelled_local_file_flush_protects_whole_file_operations_until_sync_finishes")
            .arg("--exact")
            .arg("--nocapture")
            .env(CHILD_MODE, "active")
            .env("BLOCKING_MAX_THREADS", "1")
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("应能启动隔离的真实异步执行环境");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if child.try_wait().expect("应能读取子进程状态").is_some() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().expect("超时后必须终止子进程");
                let output = child.wait_with_output().expect("必须回收超时子进程");
                panic!("刷新取消专项超时: {}", String::from_utf8_lossy(&output.stderr));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().expect("应能回收隔离子进程");
        assert!(
            output.status.success(),
            "刷新取消专项失败\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }
    const PAYLOAD_LEN: usize = 4096;

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
    let mapping_resource = block_on(namespace.open(&path, FileAccessMode::ReadMmap))
        .expect("建图资源应能独立打开");

    // 仅配置隔离进程的真实执行资源：单 worker 暂时忙碌，生产 FileIo 刷新
    // 照常提交真实同步任务并持有自己的资源；没有模拟或重写任何文件功能。
    let (started_sender, started_receiver) = std::sync::mpsc::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::channel();
    let occupied_worker = blocking::unblock(move || {
        started_sender.send(()).expect("应能报告 worker 已被占用");
        release_receiver.recv_timeout(std::time::Duration::from_secs(15))
            .expect("必须在截止内恢复真实执行池");
    });
    started_receiver.recv_timeout(std::time::Duration::from_secs(15))
        .expect("真实阻塞任务应在截止内开始");

    let flush = flush_resource.flush(FileFlushMode::DataAndMetadata);
    let first_poll = block_on(future::poll_once(flush));
    assert!(first_poll.is_none(), "单 worker 未恢复前刷新不可能已经完成");

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

    // 真实 worker 尚未恢复，这些操作应获准等待执行，而非因刷新立即返回
    // Conflict。建图先于读取/追加探测，以免引入它们原有的建图互斥关系。
    // SAFETY: 测试独占文件，没有库外修改；映射只覆盖已经存在的旧范围。
    let mapping = unsafe {
        mapping_resource.map_read_only_uncoordinated(MmapRange::new(0, 4).unwrap())
    };
    assert!(
        block_on(future::poll_once(mapping)).is_none(),
        "刷新活动期间合法建图应获准，而不是立即冲突",
    );
    let mut pending_read_buffer = [0_u8; 4];
    let read = read_resource.read_exact_at(
        0,
        &mut pending_read_buffer,
        ReadTargetRegion::full(),
    );
    assert!(
        block_on(future::poll_once(read)).is_none(),
        "刷新活动期间普通读取应获准，而不是立即冲突",
    );
    let append = append_resource.append(b"append".to_vec());
    assert!(
        block_on(future::poll_once(append)).is_none(),
        "刷新活动期间独立追加应获准，而不是立即冲突",
    );
    let independent_flush = competing_flush.flush(FileFlushMode::Data);
    let admitted = block_on(future::poll_once(independent_flush));
    assert!(admitted.is_none(), "独立刷新应获准排队，而非因已有刷新返回冲突");
    release_sender.send(()).expect("应能恢复真实 worker");
    block_on(occupied_worker);

    let mut read_buffer = [0x55; 4];
    block_on(read_resource.read_exact_at(
        0,
        &mut read_buffer,
        ReadTargetRegion::full(),
    ))
    .expect("普通刷新不应阻止独立内容读取");
    assert_eq!(&read_buffer, b"base");
    assert_eq!(
        block_on(append_resource.byte_len()).expect("取消等待后的追加必须真实收敛"),
        11 + PAYLOAD_LEN as u64,
    );
    block_on(competing_flush.flush(FileFlushMode::Data))
        .expect("独立普通刷新应可并存");
    assert_eq!(
        block_on(flush_resource.byte_len()).expect("原资源应等待后台完成者归还句柄"),
        11 + PAYLOAD_LEN as u64,
    );
    block_on(append_resource.append(b"tail".to_vec()))
        .expect("强刷新 actor 完成后竞争追加应可重试");
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        15 + PAYLOAD_LEN as u64,
    );
    block_on(overwrite_resource.overwrite_all(b"restored".to_vec()))
        .expect("刷新完成后覆盖应恢复准入");
    block_on(truncate_resource.truncate(3))
        .expect("刷新完成后截断应恢复准入");
    assert_eq!(std::fs::read(&path).unwrap(), b"res");
}

// 两线程通过硬链接别名握住同一稳定文件：一个持续修改/刷新可写映射，另一个
// 追加/刷新。映射 guard 在全部追加结束前一直保活，最终旧范围与每条尾部记录精确。
#[test]
fn test_mmap_write_append_and_file_flush_coexist_across_threads_and_hard_links() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("target");
    let alias = temporary_directory.path().join("alias");
    std::fs::write(&path, vec![b'0'; 4096]).expect("应能建立真实测试文件");
    std::fs::hard_link(&path, &alias).expect("本地测试文件系统必须支持硬链接");
    let namespace = LocalFileNamespace::new();
    let mut mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap))
        .expect("读写映射资源应能打开");
    let mut appender = block_on(namespace.open(&alias, FileAccessMode::Append))
        .expect("硬链接别名上的独立追加资源应能打开");
    // SAFETY: 目标由测试独占；只通过映射句柄修改旧范围，通过独立资源追加尾部。
    let mut mapping = unsafe {
        block_on(mapper.map_read_write_uncoordinated(
            MmapRange::new(0, 4096).expect("固定映射范围应有效"),
        ))
    }
    .expect("可写映射应能建立");
    let (control_sender, control_receiver) = std::sync::mpsc::channel();
    let (completed_sender, completed_receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        control_receiver.recv_timeout(std::time::Duration::from_secs(15))
            .expect("调用线程必须在截止内发出启动信号");
        for byte in 0_u8..16 {
            mapping.as_bytes_mut()[..4].fill(byte);
            block_on(mapping.flush()).expect("线程中的映射刷新应成功");
            block_on(mapper.flush(FileFlushMode::Data))
                .expect("可写映射文件资源刷新应与独立追加并存");
        }
        completed_sender.send(()).expect("应能报告映射修改与刷新已经完成");
        control_receiver.recv_timeout(std::time::Duration::from_secs(15))
            .expect("调用线程必须在全部追加结束后释放映射");
        assert_eq!(&mapping.as_bytes()[..4], &[15; 4]);
    });
    control_sender.send(()).expect("应能启动映射线程");
    for _ in 0..16 {
        block_on(appender.append(b"TAIL".to_vec()))
            .expect("严格追加应与映射修改和刷新共存");
        block_on(appender.flush(FileFlushMode::DataAndMetadata))
            .expect("每次追加后都应允许普通文件强刷新");
    }
    completed_receiver.recv_timeout(std::time::Duration::from_secs(15))
        .expect("映射线程必须在截止内完成全部调用");
    control_sender.send(()).expect("应能释放映射线程持有的 guard");
    worker.join().expect("映射线程应完整结束且释放 guard");
    let bytes = std::fs::read(&path).expect("应能独立读取最终权威内容");
    assert_eq!(&bytes[..4], &[15; 4]);
    assert!(bytes[4..4096].iter().all(|byte| *byte == b'0'));
    assert_eq!(bytes.len(), 4160);
    assert_eq!(&bytes[4096..], b"TAIL".repeat(16));
}
