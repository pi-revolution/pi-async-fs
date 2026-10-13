//! 授权资源交接的真实跨进程生命周期回归，保护LIF-INHERITED-R01。
//!
//! 只驱动公开打开、正常Drop和合作式安全删除，不代理锁或修改内部状态。
//! 默认运行有界正常交接；独立GDB脚本仅改变同一场景的线程顺序，精确覆盖旧竞争。
//! 必须串行执行。调试器路径敏感的控制证据不冒充Windows等价调度证明。

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use futures_lite::future::block_on;
use pi_async_fs::{
    set_cross_process_coordination_root, CrossProcessCoordinationRoot,
    FileAccessMode, FileNamespace, LocalFileNamespace,
};

const TEST_NAME: &str = "test_coordinated_resource_handoff_keeps_removal_blocked_until_last_drop";
const ROLE: &str = "PI_ASYNC_FS_HANDOFF_ROLE";
const ROOT: &str = "PI_ASYNC_FS_HANDOFF_ROOT";
const TARGET: &str = "PI_ASYNC_FS_HANDOFF_TARGET";
const DIAGNOSTIC_DIR: &str = "PI_ASYNC_FS_HANDOFF_DIR";
const INTERLEAVE: &str = "PI_ASYNC_FS_HANDOFF_INTERLEAVE";

// 信号文件仅编排真实参与者；不承担生产互斥或状态维护。
fn wait_for(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !path.exists() {
        assert!(Instant::now() < deadline, "handoff signal timeout: {}", path.display());
        thread::sleep(Duration::from_millis(1));
    }
}

fn configure(root: &Path) {
    set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(root.to_path_buf()))
        .expect("trusted shared coordination root");
}

// 使用实际合作进程的公开安全删除；绝不直接打开、加锁或改写协议载体。
fn remove_in_peer() {
    let root = PathBuf::from(std::env::var_os(ROOT).expect("peer root"));
    let target = PathBuf::from(std::env::var_os(TARGET).expect("peer target"));
    configure(&root);
    let namespace = LocalFileNamespace::new();
    // SAFETY: 测试控制全部参与者与目录，所有访问者遵守同一公开协调协议。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }.expect("peer joins the real published protocol");
    match block_on(namespace.remove_file_with_cross_process_authority(&authority)) {
        Ok(()) => println!("\nR01_PEER_RESULT=removed"),
        Err(failure) => println!("\nR01_PEER_RESULT={:?}", failure.error().current_context()),
    }
}

fn peer_remove(root: &Path, target: &Path) -> String {
    let mut child = Command::new(std::env::current_exe().expect("test executable"))
        .arg(TEST_NAME).arg("--exact").arg("--nocapture").arg("--test-threads=1")
        .env(ROLE, "peer").env(ROOT, root).env(TARGET, target)
        .env_remove(INTERLEAVE).env_remove(DIAGNOSTIC_DIR)
        .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().expect("real removal peer");
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if child.try_wait().expect("peer wait").is_some() { break; }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("handoff peer timeout");
        }
        thread::sleep(Duration::from_millis(1));
    }
    let output = child.wait_with_output().expect("peer output");
    assert!(output.status.success(), "handoff peer failed: {output:?}");
    let stdout = String::from_utf8(output.stdout).expect("peer UTF-8 output");
    let results: Vec<_> = stdout.lines()
        .filter_map(|line| line.strip_prefix("R01_PEER_RESULT="))
        .collect();
    assert_eq!(results.len(), 1, "exactly one peer verdict: {stdout}");
    results[0].to_owned()
}

// Windows小夹具位于原生项目target；Linux用小型ext4临时目录并自动回收。
fn fixture() -> tempfile::TempDir {
    #[cfg(windows)]
    {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("authority-handoff-fixtures");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new().prefix("handoff-").tempdir_in(root).unwrap()
    }
    #[cfg(not(windows))]
    { tempfile::Builder::new().prefix("pi-async-fs-handoff-").tempdir().unwrap() }
}

#[test]
// 公共接缝：授权打开→线程交接→另一进程安全删除→最后Drop→删除成功。
// 精确调度模式和普通模式采用相同的Conflict/真实名称/字节断言，不能跳过故障注入。
fn test_coordinated_resource_handoff_keeps_removal_blocked_until_last_drop() {
    if std::env::var(ROLE).as_deref() == Ok("peer") {
        remove_in_peer();
        return;
    }
    let injected = std::env::var(INTERLEAVE).as_deref() == Ok("1");
    let temporary = if injected { None } else { Some(fixture()) };
    let directory = if injected {
        PathBuf::from(std::env::var_os(DIAGNOSTIC_DIR).expect("explicit debugger fixture"))
    } else {
        temporary.as_ref().unwrap().path().to_path_buf()
    };
    let root = directory.join("coordination");
    std::fs::create_dir(&root).expect("new private coordination root");
    let target = directory.join("data.bin");
    std::fs::write(&target, b"R01-real-file").unwrap();
    configure(&root);
    let namespace = LocalFileNamespace::new();
    // SAFETY: 建立前文件/目录/参与者均由测试控制，之后只用公开协调访问。
    let authority = Arc::new(unsafe {
        block_on(namespace.establish_cross_process_authority(&target))
    }.expect("real authority"));
    let first = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read))
        .expect("first real resource");
    assert_eq!(peer_remove(&root, &target), "Conflict", "baseline protection must exist");
    assert_eq!(std::fs::read(&target).unwrap(), b"R01-real-file");
    println!("R01_BASELINE=Conflict");

    let release = directory.join("release-a");
    let released = directory.join("released-a");
    let first_release = release.clone();
    let first_released = released.clone();
    let first_thread = thread::spawn(move || {
        wait_for(&first_release);
        drop(first);
        std::fs::write(first_released, b"dropped").unwrap();
    });
    if injected { std::fs::write(directory.join("armed"), b"armed").unwrap(); }
    let second_namespace = namespace.clone();
    let second_authority = Arc::clone(&authority);
    let second = thread::spawn(move || {
        block_on(second_namespace.open_with_cross_process_authority(&second_authority, FileAccessMode::Read))
            .expect("second real resource")
    }).join().expect("second thread");
    if injected {
        assert!(directory.join("injection-complete").exists(), "required debugger interleave missing");
    } else {
        std::fs::write(&release, b"release").unwrap();
    }
    wait_for(&released);
    first_thread.join().unwrap();
    let observed = peer_remove(&root, &target);
    let exists = target.exists();
    let bytes_match = exists && std::fs::read(&target).unwrap() == b"R01-real-file";
    println!("R01_HELD_RESULT={observed}\nR01_TARGET_EXISTS_WHILE_HELD={exists}");
    // 必须让真实second资源活到独立删除结果之后，最后释放再独立核验恢复。
    drop(second);
    let after_drop = if exists { peer_remove(&root, &target) } else { "already-removed-while-held".to_owned() };
    println!("R01_AFTER_DROP={after_drop}");
    assert_eq!(observed, "Conflict", "held resource must block cooperative removal");
    assert!(bytes_match, "held target must retain original bytes");
    assert_eq!(after_drop, "removed", "final release must admit cooperative removal");
    assert!(!target.exists(), "successful removal must remove the name");
}
