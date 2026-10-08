//! 跨真实操作系统进程的本地协调协议集成测试。
//!
//! 父测试复用当前测试二进制启动受控子进程，以真实文件、锁与协议记录验证
//! 映射冲突、追加记录完整性、排他创建和自定义协调根。测试共享 CPU、磁盘和
//! 进程调度资源，必须串行独占执行；环境变量仅是本测试二进制的私有控制面。

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    set_cross_process_coordination_root, CrossProcessCoordinationRoot,
    FileAccessMode, FileFlushMode, FileIo, FileNamespace, LocalFileNamespace,
    MmapRange, OverwriteTargetEvidence, ReadTargetRegion, RemoveTargetEvidence,
    TransferProgress,
};
use pi_result::ErrorKind;

// 父子进程协议字段：用路径文件作为就绪/释放信号，避免依赖特定异步运行时。
const CHILD_CASE: &str = "PI_ASYNC_FS_CROSS_PROCESS_CASE";
const TARGET_PATH: &str = "PI_ASYNC_FS_CROSS_PROCESS_TARGET";
const READY_PATH: &str = "PI_ASYNC_FS_CROSS_PROCESS_READY";
const RELEASE_PATH: &str = "PI_ASYNC_FS_CROSS_PROCESS_RELEASE";
const APPEND_WORKER: &str = "PI_ASYNC_FS_APPEND_WORKER";
const RESULT_PATH: &str = "PI_ASYNC_FS_CROSS_PROCESS_RESULT";
const COORDINATION_ROOT_PATH: &str = "PI_ASYNC_FS_COORDINATION_ROOT_PATH";
// 每个追加进程写入固定数量、固定宽度且可独立校验的唯一记录。
const APPEND_RECORDS: usize = 96;
const APPEND_RECORD_BYTES: usize = 64;

// 读取父进程传入的必需原生路径；缺失代表测试装配错误而非生产错误。
fn required_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var_os(name).expect("子进程路径参数应存在"))
}

// 有界等待跨进程文件信号，超时会携带阶段和路径立即终止测试。
fn wait_for_path(path: &Path, description: &str) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "等待{description}超时: {}",
            path.display(),
        );
        thread::sleep(Duration::from_millis(10));
    }
}

// 只保护本轮映射/刷新子进程夹具的生命周期，不介入生产文件访问逻辑。
// 成功退出最多等待 30 秒；超时或父进程断言展开时终止并回收子进程，避免
// 后台仍持有真实文件锁影响下一项测试。受控子进程没有继承输出管道的后代。
struct ManagedChild {
    child: Option<Child>,
}

impl ManagedChild {
    fn wait_with_output(mut self) -> std::io::Result<Output> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let child = self.child.as_mut().expect("待回收的子进程必须存在");
            if child.try_wait()?.is_some() {
                return self.child.take().unwrap().wait_with_output();
            }
            if Instant::now() >= deadline {
                child.kill()?;
                let output = self.child.take().unwrap().wait_with_output()?;
                panic!(
                    "子进程退出超时\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // 展开期清理不能再次 panic；失败保留诊断，不伪造产品成功结果。
            if let Err(error) = child.kill() {
                eprintln!("终止测试子进程失败: {error}");
            }
            if let Err(error) = child.wait() {
                eprintln!("回收测试子进程失败: {error}");
            }
        }
    }
}

// 以精确过滤器重新启动当前测试二进制，运行指定通用子进程场景。
fn spawn_current_test(
    test_name: &str,
    case: &str,
    target: &Path,
    ready: &Path,
    release: &Path,
) -> ManagedChild {
    let mut command = Command::new(std::env::current_exe().expect("应能定位当前测试程序"));
    command
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_CASE, case)
        .env(TARGET_PATH, target)
        .env(READY_PATH, ready)
        .env(RELEASE_PATH, release)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if case == "hold-file-flush" {
        command.env("BLOCKING_MAX_THREADS", "1");
    }
    ManagedChild {
        child: Some(command.spawn().expect("应能启动隔离测试子进程")),
    }
}

// 启动持有独立文件句柄的追加工作进程，并传递可验证的工作者编号。
fn spawn_append_worker(
    test_name: &str,
    target: &Path,
    ready: &Path,
    release: &Path,
    worker: char,
) -> Child {
    Command::new(std::env::current_exe().expect("应能定位当前测试程序"))
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_CASE, "append-worker")
        .env(TARGET_PATH, target)
        .env(READY_PATH, ready)
        .env(RELEASE_PATH, release)
        .env(APPEND_WORKER, worker.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("应能启动追加工作子进程")
}

// 启动协调式排他创建竞争者，并用独立结果文件回传提交证据。
fn spawn_create_worker(
    test_name: &str,
    target: &Path,
    ready: &Path,
    release: &Path,
    result: &Path,
) -> Child {
    Command::new(std::env::current_exe().expect("应能定位当前测试程序"))
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_CASE, "create-worker")
        .env(TARGET_PATH, target)
        .env(READY_PATH, ready)
        .env(RELEASE_PATH, release)
        .env(RESULT_PATH, result)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("应能启动协调式创建子进程")
}

// 生成固定宽度唯一记录；记录边界被最终 chunks_exact 作为参考模型校验。
fn append_record(worker: u8, index: usize) -> Vec<u8> {
    let prefix = format!("{}:{index:04}:", char::from(worker));
    let mut record = vec![worker; APPEND_RECORD_BYTES];
    record[..prefix.len()].copy_from_slice(prefix.as_bytes());
    record
}

#[test]
// 子进程夹具：持有协调式只读或可写映射直到父进程释放；可写分支同时通过
// 真实映射与文件刷新建立完成证据，全部等待有截止，不模拟协调或刷新逻辑。
fn test_helper_hold_coordinated_mapping_in_child_process() {
    let writable = match std::env::var(CHILD_CASE).as_deref() {
        Ok("hold-mapping") => false,
        Ok("hold-write-mapping") => true,
        _ => return,
    };
    let target = required_path(TARGET_PATH);
    let ready = required_path(READY_PATH);
    let release = required_path(RELEASE_PATH);
    let namespace = LocalFileNamespace::new();

    // SAFETY: 父测试创建并独占目标目录；父子进程均只使用本库同版本协调入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("子进程应加入既有协调域");
    let mut resource = block_on(namespace.open_with_cross_process_authority(
        &authority,
        if writable { FileAccessMode::ReadWriteMmap } else { FileAccessMode::ReadMmap },
    ))
    .expect("子进程应打开协调式映射资源");
    if writable {
        let mut mapping = block_on(resource.map_read_write(
            MmapRange::new(2, 7).expect("测试范围应有效"),
        ))
        .expect("子进程应建立活动可写映射");
        mapping.as_bytes_mut().copy_from_slice(b"ABCDE");
        block_on(mapping.flush()).expect("映射修改应取得同步完成证据");
        block_on(resource.flush(FileFlushMode::DataAndMetadata))
            .expect("子进程读写映射资源也应支持普通文件刷新");
        std::fs::write(&ready, b"ready").expect("应能发布可写映射就绪信号");
        wait_for_path(&release, "父进程释放信号");
        assert_eq!(mapping.as_bytes(), b"ABCDE");
        return;
    }
    let mapping = block_on(resource.map_read_only(
        MmapRange::new(2, 7).expect("测试范围应有效"),
    ))
    .expect("子进程应建立活动只读映射");
    assert_eq!(mapping.as_bytes(), b"23456");
    std::fs::write(&ready, b"ready").expect("应能发布子进程就绪信号");
    wait_for_path(&release, "父进程释放信号");
    assert_eq!(mapping.as_bytes(), b"23456");
}

// B2H-DES-001：仅调控隔离进程的真实单 worker 队列，使生产刷新在取得跨
// 进程许可后保持未完成；取消等待后仍由生产完成者保有许可。不触碰协议文件、
// 不模拟锁/刷新，父进程的公开拒绝与恢复结果才是协调正确性的判断依据。
#[test]
fn test_helper_hold_cancelled_file_flush_in_child_process() {
    if std::env::var(CHILD_CASE).as_deref() != Ok("hold-file-flush") {
        return;
    }
    let target = required_path(TARGET_PATH);
    let ready = required_path(READY_PATH);
    let release = required_path(RELEASE_PATH);
    let namespace = LocalFileNamespace::new();
    // SAFETY: 父子进程独占目录并使用同版本、同物理根的公开协调入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("子进程应加入既有协调域");
    let mut file = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Append,
    ))
    .expect("子进程应打开独立刷新资源");

    let (first_started_sender, first_started_receiver) = std::sync::mpsc::channel();
    let (first_release_sender, first_release_receiver) = std::sync::mpsc::channel();
    let first_worker = blocking::unblock(move || {
        first_started_sender.send(()).unwrap();
        first_release_receiver.recv_timeout(Duration::from_secs(15))
            .expect("首个执行资源门必须在截止内释放");
    });
    first_started_receiver.recv_timeout(Duration::from_secs(15)).unwrap();
    let mut flush = Box::pin(file.flush(FileFlushMode::DataAndMetadata));
    assert!(block_on(future::poll_once(flush.as_mut())).is_none());

    // 在公开 Future 尚未继续轮询时，把第二个执行资源门排在已提交工作之后。
    // 第一门释放后第二门保留 worker；再轮询公开 Future 后其真实同步仍排队，
    // 不能靠磁盘快慢猜测该操作是否仍在进行。任务队列不处理任何文件副作用。
    let (second_started_sender, second_started_receiver) = std::sync::mpsc::channel();
    let (second_release_sender, second_release_receiver) = std::sync::mpsc::channel();
    let second_worker = blocking::unblock(move || {
        second_started_sender.send(()).unwrap();
        second_release_receiver.recv_timeout(Duration::from_secs(15))
            .expect("刷新执行资源门必须在截止内释放");
    });
    first_release_sender.send(()).unwrap();
    block_on(first_worker);
    second_started_receiver.recv_timeout(Duration::from_secs(15)).unwrap();
    assert!(
        block_on(future::poll_once(flush.as_mut())).is_none(),
        "同步 worker 被占用时刷新不得已完成",
    );
    drop(flush);
    std::fs::write(&ready, b"ready").expect("应能报告已取消等待的在途刷新");
    wait_for_path(&release, "父进程完成冲突与共存断言");
    second_release_sender.send(()).unwrap();
    block_on(second_worker);
    assert_eq!(block_on(file.byte_len()).unwrap(), 8);
    block_on(file.flush(FileFlushMode::DataAndMetadata))
        .expect("后台取消请求完成后资源应可再次成功刷新");
}

#[test]
// 子进程夹具：用独立协调式 O_APPEND 句柄连续写入唯一固定宽度记录。
fn test_helper_append_records_in_child_process() {
    if std::env::var(CHILD_CASE).as_deref() != Ok("append-worker") {
        return;
    }
    let target = required_path(TARGET_PATH);
    let ready = required_path(READY_PATH);
    let release = required_path(RELEASE_PATH);
    let worker_text = std::env::var(APPEND_WORKER).expect("工作编号应存在");
    let worker = *worker_text.as_bytes().first().expect("工作编号不能为空");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 父测试独占目录；所有工作进程只通过同一版本协调入口访问目标。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("追加工作进程应加入协调域");
    let mut appender = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Append,
    ))
    .expect("追加工作进程应打开独立 O_APPEND 资源");
    std::fs::write(&ready, b"ready").expect("应能发布追加工作进程就绪信号");
    wait_for_path(&release, "并发追加开始信号");

    for index in 0..APPEND_RECORDS {
        block_on(appender.append(append_record(worker, index)))
            .expect("每个完整记录都应原子地追加成功");
    }
}

#[test]
// 子进程夹具：参与同一不存在目标的协调式排他创建，并回传是否提交。
fn test_helper_race_coordinated_create_in_child_process() {
    if std::env::var(CHILD_CASE).as_deref() != Ok("create-worker") {
        return;
    }
    let target = required_path(TARGET_PATH);
    let ready = required_path(READY_PATH);
    let release = required_path(RELEASE_PATH);
    let result_path = required_path(RESULT_PATH);
    let namespace = LocalFileNamespace::new();
    std::fs::write(&ready, b"ready").expect("应能发布创建工作进程就绪信号");
    wait_for_path(&release, "协调式创建开始信号");

    // SAFETY: 父测试独占目标目录；两个竞争进程使用相同协议和默认协调根。
    let result = unsafe {
        block_on(namespace.create_new_coordinated(
            target,
            FileAccessMode::Read,
        ))
    };
    let text = match result {
        Ok(created) => {
            assert_eq!(block_on(created.file().byte_len()).unwrap(), 0);
            "success"
        }
        Err(failure) => {
            assert_eq!(
                failure.target_evidence(),
                &pi_async_fs::CreateTargetEvidence::NotCreatedByOperation,
            );
            "not-created"
        }
    };
    std::fs::write(result_path, text).expect("应能发布协调式创建结果");
}

#[test]
// 验证跨进程相交映射、读取、截断和删除冲突，端点相接范围及追加可并存。
fn test_cross_process_mapping_conflict_matrix_and_lifecycle_removal() {
    const HELPER: &str = "test_helper_hold_coordinated_mapping_in_child_process";
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("managed-file");
    let ready = temporary_directory.path().join("child-ready");
    let release = temporary_directory.path().join("child-release");
    std::fs::write(&target, b"0123456789").expect("应能建立真实测试文件");

    let namespace = LocalFileNamespace::new();
    // SAFETY: 测试独占目录，并在启动子进程前建立双方共同加入的协调域。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("父进程应建立协调域");
    let child = spawn_current_test(
        HELPER,
        "hold-mapping",
        &target,
        &ready,
        &release,
    );
    wait_for_path(&ready, "子进程活动映射");

    let mmap_resource = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::ReadMmap,
    ))
    .expect("父进程应能打开独立映射资源");
    let overlap = block_on(mmap_resource.map_read_only(
        MmapRange::new(6, 9).expect("测试范围应有效"),
    ))
    .expect_err("不同进程的相交半开范围必须冲突");
    assert_eq!(overlap.current_context(), &ErrorKind::Conflict);
    let touching = block_on(mmap_resource.map_read_only(
        MmapRange::new(7, 10).expect("测试范围应有效"),
    ))
    .expect("只接触端点而不相交的范围应能并存");
    assert_eq!(touching.as_bytes(), b"789");
    drop(touching);
    drop(mmap_resource);

    let reader = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Read,
    ))
    .expect("映射存活时仍允许取得独立读取资源");
    let mut bytes = [0_u8; 2];
    let read_error = block_on(reader.read_exact_at(
        0,
        &mut bytes,
        ReadTargetRegion::full(),
    ))
    .expect_err("任何活动映射都必须排斥普通内容读取");
    assert_eq!(read_error.current_context(), &ErrorKind::Conflict);
    drop(reader);

    let mut appender = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Append,
    ))
    .expect("映射存活时应允许打开独立追加资源");
    block_on(appender.append(Vec::from(&b"A"[..])))
        .expect("既有映射与严格 O_APPEND 追加允许并存");
    block_on(appender.flush(FileFlushMode::Data))
        .expect("其它进程只读映射存活时必须允许追加数据刷新");
    block_on(appender.flush(FileFlushMode::DataAndMetadata))
        .expect("其它进程只读映射存活时必须允许数据及元信息刷新");
    drop(appender);
    assert_eq!(std::fs::read(&target).unwrap(), b"0123456789A");

    let mut truncater = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Truncate,
    ))
    .expect("映射存活时可以取得角色资源，但操作仍须动态准入");
    let truncate_error = block_on(truncater.truncate(5))
        .expect_err("不同进程的活动映射必须排斥截断");
    assert_eq!(truncate_error.current_context(), &ErrorKind::Conflict);
    drop(truncater);
    assert_eq!(std::fs::metadata(&target).unwrap().len(), 11);

    let removal = block_on(
        namespace.remove_file_with_cross_process_authority(&authority),
    )
    .expect_err("其它进程仍有活动资源和映射时删除必须立即冲突");
    assert_eq!(removal.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        removal.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation,
    );

    std::fs::write(&release, b"release").expect("应能释放子进程");
    let output = child.wait_with_output().expect("应能回收子进程");
    assert!(
        output.status.success(),
        "映射子进程失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    block_on(namespace.remove_file_with_cross_process_authority(&authority))
        .expect("其它进程释放最后一个活动所有者后删除应成功");
    assert!(!target.exists());
}

// B2H-DES-001：子进程持续握住可写映射及独立文件资源时，父进程的完整追加与
// 两种强刷新均可成功；子进程旧范围与父进程尾部内容严格核验，跨进程锁不误互斥。
#[test]
fn test_cross_process_writable_mapping_coexists_with_append_and_file_flush() {
    const HELPER: &str = "test_helper_hold_coordinated_mapping_in_child_process";
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("managed-file");
    let ready = temporary_directory.path().join("child-ready");
    let release = temporary_directory.path().join("child-release");
    std::fs::write(&target, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    // SAFETY: 父子进程独占目录并只通过本库同版本、同物理根的协调入口访问文件。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("父进程应建立协调域");
    let child = spawn_current_test(HELPER, "hold-write-mapping", &target, &ready, &release);
    wait_for_path(&ready, "子进程活动可写映射及刷新完成");
    let mut appender = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Append,
    ))
    .expect("父进程应能独立打开追加资源");
    block_on(appender.append(b"XY".to_vec()))
        .expect("其它进程可写映射存在时应允许严格追加");
    block_on(appender.flush(FileFlushMode::Data))
        .expect("其它进程可写映射不能阻止追加内容的数据刷新");
    block_on(appender.flush(FileFlushMode::DataAndMetadata))
        .expect("其它进程可写映射不能阻止数据及元信息刷新");
    assert_eq!(std::fs::read(&target).unwrap(), b"01ABCDE789XY");
    std::fs::write(&release, b"release").expect("应能释放子进程映射");
    let output = child.wait_with_output().expect("应能回收子进程");
    assert!(
        output.status.success(),
        "可写映射子进程失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"01ABCDE789XY");
}

// B2H-DES-001：另一真实进程取消刷新等待但同步尚未完成时，覆盖/截断必须
// 明确冲突并无副作用，元信息、追加及另一刷新可并存；真实完成后破坏性操作
// 恢复。全部目标访问来自公开协调式生产 API，子进程由作用域 guard 回收。
#[test]
fn test_cross_process_in_flight_file_flush_protects_whole_file_operations() {
    const HELPER: &str = "test_helper_hold_cancelled_file_flush_in_child_process";
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("managed-file");
    let ready = temporary_directory.path().join("child-ready");
    let release = temporary_directory.path().join("child-release");
    std::fs::write(&target, b"012345").unwrap();
    let namespace = LocalFileNamespace::new();
    // SAFETY: 父子进程独占目标目录且只使用同版本、同物理根的协调式入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("父进程应建立协调域");
    let mut overwrite = block_on(namespace.open_with_cross_process_authority(
        &authority, FileAccessMode::Overwrite,
    )).unwrap();
    let mut truncate = block_on(namespace.open_with_cross_process_authority(
        &authority, FileAccessMode::Truncate,
    )).unwrap();
    let mut appender = block_on(namespace.open_with_cross_process_authority(
        &authority, FileAccessMode::Append,
    )).unwrap();
    // 负面控制：普通持有这些资源并不使破坏性操作永久失败。
    block_on(overwrite.overwrite_all(b"012345".to_vec())).unwrap();
    block_on(truncate.truncate(6)).unwrap();
    let child = spawn_current_test(HELPER, "hold-file-flush", &target, &ready, &release);
    wait_for_path(&ready, "子进程在途文件刷新");

    let failure = block_on(overwrite.overwrite_all(b"destroy".to_vec()))
        .expect_err("另一进程同步未结束时覆盖必须冲突");
    let (error, buffer, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(buffer, b"destroy");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);
    let error = block_on(truncate.truncate(0))
        .expect_err("另一进程同步未结束时截断必须冲突");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(std::fs::read(&target).unwrap(), b"012345");
    assert_eq!(block_on(appender.byte_len()).unwrap(), 6);
    block_on(appender.append(b"XY".to_vec())).unwrap();
    block_on(appender.flush(FileFlushMode::Data)).unwrap();
    block_on(appender.flush(FileFlushMode::DataAndMetadata)).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"012345XY");

    std::fs::write(&release, b"release").unwrap();
    let output = child.wait_with_output().expect("应在截止内回收刷新子进程");
    assert!(
        output.status.success(),
        "刷新子进程失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    block_on(overwrite.overwrite_all(b"done".to_vec()))
        .expect("刷新完成后覆盖必须恢复准入");
    block_on(truncate.truncate(2)).expect("刷新完成后截断必须恢复准入");
    assert_eq!(std::fs::read(&target).unwrap(), b"do");
}

#[test]
// 验证两个进程的不同追加句柄被串行化，每次成功调用的记录都不撕裂且不重复。
fn test_cross_process_append_serializes_distinct_handles_without_record_tearing() {
    const HELPER: &str = "test_helper_append_records_in_child_process";
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("append-target");
    let ready_a = temporary_directory.path().join("append-a-ready");
    let ready_b = temporary_directory.path().join("append-b-ready");
    let release = temporary_directory.path().join("append-release");
    std::fs::write(&target, []).expect("应能建立空测试文件");

    let namespace = LocalFileNamespace::new();
    // SAFETY: 测试独占目标目录，两个子进程均使用同版本、同根位置的协调协议。
    let _authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("父进程应先发布协调域");
    let worker_a = spawn_append_worker(
        HELPER,
        &target,
        &ready_a,
        &release,
        'A',
    );
    wait_for_path(&ready_a, "追加工作进程 A");
    let worker_b = spawn_append_worker(
        HELPER,
        &target,
        &ready_b,
        &release,
        'B',
    );
    wait_for_path(&ready_b, "追加工作进程 B");
    std::fs::write(&release, b"start").expect("应能同时释放追加工作进程");

    for (name, child) in [("A", worker_a), ("B", worker_b)] {
        let output = child.wait_with_output().expect("应能回收追加子进程");
        assert!(
            output.status.success(),
            "追加子进程 {name} 失败\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    let bytes = std::fs::read(&target).expect("应能读取最终追加结果");
    assert_eq!(
        bytes.len(),
        2 * APPEND_RECORDS * APPEND_RECORD_BYTES,
        "成功次数和最终字节数必须精确一致",
    );
    let mut expected = HashSet::with_capacity(2 * APPEND_RECORDS);
    for worker in *b"AB" {
        for index in 0..APPEND_RECORDS {
            assert!(expected.insert(append_record(worker, index)));
        }
    }
    for record in bytes.chunks_exact(APPEND_RECORD_BYTES) {
        assert!(
            expected.remove(record),
            "每个物理记录必须完整属于一个调用，且不得重复或字节交错",
        );
    }
    assert!(expected.is_empty(), "所有预期记录都必须恰好出现一次");
}

#[test]
// 验证两个真实进程竞争协调式创建时恰有一个提交者和一个明确未创建者。
fn test_cross_process_coordinated_create_has_exactly_one_committer() {
    const HELPER: &str = "test_helper_race_coordinated_create_in_child_process";
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("create-target");
    let ready_a = temporary_directory.path().join("create-a-ready");
    let ready_b = temporary_directory.path().join("create-b-ready");
    let result_a = temporary_directory.path().join("create-a-result");
    let result_b = temporary_directory.path().join("create-b-result");
    let release = temporary_directory.path().join("create-release");

    let worker_a = spawn_create_worker(
        HELPER,
        &target,
        &ready_a,
        &release,
        &result_a,
    );
    let worker_b = spawn_create_worker(
        HELPER,
        &target,
        &ready_b,
        &release,
        &result_b,
    );
    wait_for_path(&ready_a, "协调式创建工作进程 A");
    wait_for_path(&ready_b, "协调式创建工作进程 B");
    std::fs::write(&release, b"start").expect("应能同时释放创建工作进程");

    for (name, child) in [("A", worker_a), ("B", worker_b)] {
        let output = child.wait_with_output().expect("应能回收创建子进程");
        assert!(
            output.status.success(),
            "创建子进程 {name} 失败\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let results = [
        std::fs::read_to_string(&result_a).expect("A 应发布创建结果"),
        std::fs::read_to_string(&result_b).expect("B 应发布创建结果"),
    ];
    assert_eq!(
        results.iter().filter(|value| value.as_str() == "success").count(),
        1,
        "原生目标和协议域必须只有一个创建提交者",
    );
    assert_eq!(
        results
            .iter()
            .filter(|value| value.as_str() == "not-created")
            .count(),
        1,
        "失败参与者必须明确证明自己没有创建目标",
    );
    assert_eq!(std::fs::metadata(&target).unwrap().len(), 0);

    let namespace = LocalFileNamespace::new();
    // SAFETY: 两个工作进程已经退出，父进程继续独占目录并加入其发布的协议。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("竞争完成后应存在一个完整可加入的 Published 协调域");
    block_on(namespace.remove_file_with_cross_process_authority(&authority))
        .expect("没有活动资源时应能清理唯一创建结果");
}

#[test]
// 在隔离子进程中验证全局自定义协调根真正承载协议数据且目标同目录无旁产物。
fn test_custom_coordination_root_drives_real_protocol_placement() {
    const TEST_NAME: &str =
        "test_custom_coordination_root_drives_real_protocol_placement";
    if std::env::var(CHILD_CASE).as_deref() != Ok("custom-root") {
        let temporary_directory =
            tempfile::tempdir().expect("应能建立真实临时目录");
        let target_parent = temporary_directory.path().join("targets");
        let coordination_root = temporary_directory.path().join("coordination");
        std::fs::create_dir(&target_parent).expect("应能建立目标父目录");
        std::fs::create_dir(&coordination_root).expect("应能建立协调根目录");
        let target = target_parent.join("managed-file");

        let output = Command::new(std::env::current_exe().expect("应能定位测试程序"))
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--nocapture")
            .env(CHILD_CASE, "custom-root")
            .env(TARGET_PATH, &target)
            .env(COORDINATION_ROOT_PATH, &coordination_root)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("应能启动隔离的自定义根测试进程");
        assert!(
            output.status.success(),
            "自定义协调根子进程失败\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }

    let target = required_path(TARGET_PATH);
    let coordination_root = required_path(COORDINATION_ROOT_PATH);
    set_cross_process_coordination_root(
        CrossProcessCoordinationRoot::Custom(coordination_root.clone()),
    )
    .expect("首次实际协调前应能冻结自定义根");
    std::fs::write(&target, b"rooted").expect("应能建立真实目标文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 隔离子进程独占目标父目录和协调根，并且整个生命周期只使用
    // 同一库版本、同一自定义根与协调式公开入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }
    .expect("自定义根下应能建立真实协调域");
    let mut appender = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Append,
    ))
    .expect("自定义根授权应能打开协调式资源");
    block_on(appender.append(Vec::from(&b"!"[..])))
        .expect("自定义根下的协调式追加应成功");
    drop(appender);

    assert!(
        std::fs::read_dir(&coordination_root)
            .expect("应能枚举自定义协调根")
            .next()
            .is_some(),
        "实际协议数据必须位于已冻结的自定义根内",
    );
    let target_parent = target.parent().expect("目标应有父目录");
    let target_entries = std::fs::read_dir(target_parent)
        .expect("应能枚举目标父目录")
        .map(|entry| entry.expect("目录项应可读取").path())
        .collect::<Vec<_>>();
    assert_eq!(target_entries, vec![target.clone()]);
    assert_eq!(std::fs::read(&target).unwrap(), b"rooted!");

    block_on(namespace.remove_file_with_cross_process_authority(&authority))
        .expect("自定义根下的协调式删除应成功");
}
