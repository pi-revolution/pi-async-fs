//! EXO-STA-M01：每真实文件对象的打开资格状态。冻结范围/两个内部出口/三图：
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_OPEN_ADMISSION_STATE_LAYER_2026-10-12.md。
//! 已经由local冷取得链消费；本层及公开装配仅限域验收，不代替最终整体签发。

use crate::file_identity::StableFileIdentity;
use crate::open_admission_native::{CoordinationRootAnchor, OpenAdmissionKind, OwnedOpenAdmission};
use pi_result::InteropResultExt;
use std::sync::Arc;

#[derive(Debug, pi_result::thiserror::Error)]
enum AdmissionStateError {
    #[error("file opening conflicts with an active admission")]
    RoleConflict,
    #[error("admission identity differs from the active file object")]
    IdentityConflict,
    #[error("admission root differs from the active coordination domain")]
    RootConflict,
    #[error("identity pin does not refer to a regular file")]
    UnsupportedTarget,
    #[error("admission state is unsupported on this platform")]
    UnsupportedPlatform,
}

impl pi_result::ClassifyErrorKind for AdmissionStateError {
    fn classify_error_kind(&self) -> pi_result::ErrorKind {
        match self {
            Self::RoleConflict | Self::IdentityConflict | Self::RootConflict => pi_result::ErrorKind::Conflict,
            Self::UnsupportedTarget | Self::UnsupportedPlatform => pi_result::ErrorKind::Unsupported,
        }
    }
}

/// STA-T01/F01~02：槽是本模块唯一权威状态；拥有型初始化守卫交给实际
/// 阻塞完成者，外层取消不能提前释放仍在执行的初始化。无外部可写出口。
pub(crate) struct OpenAdmissionState {
    identity: StableFileIdentity,
    admission: Arc<async_lock::Mutex<Option<OwnedOpenAdmission>>>,
}

impl OpenAdmissionState {
    /// 只建立尚未取得资格的本地槽，不查配置、不执行系统I/O。
    #[must_use]
    pub(crate) fn new(identity: StableFileIdentity) -> Self {
        Self { identity, admission: Arc::new(async_lock::Mutex::new(None)) }
    }

    /// 只在建立资源时调用；真实数据操作不查询本槽。初始化门只覆盖取得，
    /// 不随业务资源长期锁住，也不由公开调用方自行安排锁序或发布字段。
    pub(crate) async fn ensure(
        &self,
        root: Arc<CoordinationRootAnchor>,
        identity_pin: std::fs::File,
        kind: OpenAdmissionKind,
    ) -> pi_result::Result<()> {
        let mut admission = self.admission.lock_arc().await;
        let expected_identity = self.identity.clone();
        crate::bounded_blocking::unblock_result(move || {
            if let Some(existing) = admission.as_ref() {
                if existing.key().root_identity() != root.snapshot().identity() {
                    return Err(AdmissionStateError::RootConflict).into_classified_error();
                }
                // 资格类别不能因同进程/同状态而被隐式升级或当作EX派生能力。
                if existing.is_exclusive() || matches!(kind, OpenAdmissionKind::Exclusive) {
                    return Err(AdmissionStateError::RoleConflict).into_classified_error();
                }
                // 复用也必须校验本次拥有型来源，不能凭预期编号或路径混入新对象。
                // L02查询只在拥有输入的真实同步完成者内借用，Windows不悬空HANDLE。
                let metadata = identity_pin.metadata().into_classified_error()?;
                if !metadata.is_file() {
                    return Err(AdmissionStateError::UnsupportedTarget).into_classified_error();
                }
                #[cfg(unix)]
                let actual = crate::file_identity_query::identity_from_metadata(&metadata)?;
                #[cfg(windows)]
                let actual = {
                    use std::os::windows::io::AsHandle;
                    crate::file_identity_query::identity_from_handle(identity_pin.as_handle())?
                };
                #[cfg(not(any(unix, windows)))]
                let actual: StableFileIdentity = {
                    return Err(AdmissionStateError::UnsupportedPlatform).into_classified_error();
                };
                if actual != expected_identity {
                    return Err(AdmissionStateError::IdentityConflict).into_classified_error();
                }
            } else {
                let acquired = OwnedOpenAdmission::try_acquire(root, identity_pin, kind)?;
                if acquired.key().file_identity() != &expected_identity {
                    return Err(AdmissionStateError::IdentityConflict).into_classified_error();
                }
                // 唯一发布点：真实取得且身份配对之后。错误分支自动关闭临时资格，
                // 不缓存失败、不修改既有槽，固定空载体继续保留供后来仲裁。
                *admission = Some(acquired);
            }
            Ok(())
        }).await
    }
}

// 诊断绝不等待初始化门或触碰原生资源；即使工作已排队/正在执行也能打印。
impl core::fmt::Debug for OpenAdmissionState {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("OpenAdmissionState").finish_non_exhaustive()
    }
}

impl core::fmt::Display for OpenAdmissionState {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("OpenAdmissionState")
    }
}

#[cfg(all(test, any(target_os = "linux", windows)))]
mod open_admission_state_tests {
    use super::OpenAdmissionState;
    use crate::file_identity::StableFileIdentity;
    use crate::open_admission_native::{CoordinationRootAnchor, OpenAdmissionKind, OwnedOpenAdmission};
    use futures_lite::future::block_on;
    use std::path::Path;
    use std::sync::Arc;

    // 小型真实夹具只准备/清理文件，不在测试侧实现资格、状态或锁协议。
    fn fixture() -> tempfile::TempDir {
        #[cfg(target_os = "linux")]
        { tempfile::Builder::new().prefix("pi-async-fs-state-").tempdir().expect("fixture") }
        #[cfg(windows)]
        {
            let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target").join("admission-state-fixtures");
            std::fs::create_dir_all(&directory).expect("fixture directory");
            tempfile::Builder::new().prefix("state-").tempdir_in(directory).expect("fixture")
        }
    }

    fn root(path: &Path) -> Arc<CoordinationRootAnchor> {
        Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(path).expect("root path")).expect("root"))
    }

    // 实际来源由已验收L02查询；不编造身份或访问生产槽的内部表示。
    fn identity(path: &Path) -> StableFileIdentity {
        let file = std::fs::File::open(path).expect("identity file");
        #[cfg(unix)]
        { crate::file_identity_query::identity_from_metadata(&file.metadata().expect("metadata")).expect("identity") }
        #[cfg(windows)]
        {
            use std::os::windows::io::AsHandle;
            crate::file_identity_query::identity_from_handle(file.as_handle()).expect("identity")
        }
    }

    // STA-T01：通过两个生产出口共享同一真实对象资格；用独立原生仲裁者
    // 证明仍被持有及最后Drop释放，不检查槽/锁计数、不用测试代码补产品功能。
    #[test]
    fn test_admission_state_reuses_shared_until_last_state_drop() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"state-original").expect("prepare");
        let root = root(temporary.path());
        let state = OpenAdmissionState::new(identity(&path));
        for _ in 0..2 {
            block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
                .expect("shared acquisition and reuse");
        }
        let competing = || OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        );
        assert_eq!(competing().unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(state);
        drop(competing().expect("last state drop releases shared admission"));
        assert_eq!(std::fs::read(&path).expect("content"), b"state-original");
    }

    // STA-T01：共享不能升级；已取得独占后，即使来自同一状态的新独立
    // SH/EX申请也必须失败。每次失败后原资格仍在，最后Drop才真正放行。
    #[test]
    fn test_admission_state_rejects_upgrades_and_independent_exclusive_reentry() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"role-original").unwrap();
        let root = root(temporary.path());
        let acquire = |state: &OpenAdmissionState, kind| {
            block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), kind))
        };
        let shared = OpenAdmissionState::new(identity(&path));
        acquire(&shared, OpenAdmissionKind::Shared).expect("shared");
        assert_eq!(acquire(&shared, OpenAdmissionKind::Exclusive).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        acquire(&shared, OpenAdmissionKind::Shared).expect("failed upgrade preserves shared");
        drop(shared);
        let exclusive = OpenAdmissionState::new(identity(&path));
        acquire(&exclusive, OpenAdmissionKind::Exclusive).expect("exclusive after shared release");
        for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
            assert_eq!(acquire(&exclusive, kind).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        }
        let other = OpenAdmissionState::new(identity(&path));
        assert_eq!(acquire(&other, OpenAdmissionKind::Shared).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(exclusive);
        acquire(&other, OpenAdmissionKind::Shared).expect("failed acquisition remains retryable");
        assert_eq!(std::fs::read(&path).unwrap(), b"role-original");
    }

    // STA-T02：真实错误文件不能首次发布或借已有SH混入，同文件不同物理根
    // 不能复用；失败前后用独立原生资格和已知字节证明未损坏原状态/文件。
    #[test]
    fn test_admission_state_validates_actual_identity_and_root_before_reuse_or_publish() {
        let temporary = fixture();
        let path = temporary.path().join("expected.bin");
        let other = temporary.path().join("other.bin");
        let alias = temporary.path().join("alias.bin");
        std::fs::write(&path, b"expected").unwrap();
        std::fs::write(&other, b"other").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        let root = root(temporary.path());
        let state = OpenAdmissionState::new(identity(&path));
        let mismatch = block_on(state.ensure(
            Arc::clone(&root), std::fs::File::open(&other).unwrap(), OpenAdmissionKind::Shared,
        )).unwrap_err();
        assert_eq!(mismatch.current_context(), &pi_result::ErrorKind::Conflict);
        drop(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&other).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("wrong object was not retained"));
        block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
            .expect("correct source can retry");
        block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&alias).unwrap(), OpenAdmissionKind::Shared))
            .expect("real alias has the same identity");
        assert_eq!(block_on(state.ensure(
            Arc::clone(&root), std::fs::File::open(&other).unwrap(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        let second_root_path = temporary.path().join("different-root");
        std::fs::create_dir(&second_root_path).unwrap();
        let different_root = self::root(&second_root_path);
        assert_eq!(block_on(state.ensure(
            different_root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::read(&path).unwrap(), b"expected");
        assert_eq!(std::fs::read(&other).unwrap(), b"other");
    }

    // STA-T05：自动线程能力和诊断是调用方合同；未poll的取得Future不能
    // 创建载体或取得资格。诊断不泄漏身份/路径，也不采样可变资格状态。
    #[test]
    fn test_admission_state_is_send_sync_and_lazy_with_redacted_diagnostics() {
        fn traits<T: Send + Sync + 'static>() {}
        fn send_future<T: Send>(_: &T) {}
        traits::<OpenAdmissionState>();
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"lazy-original").unwrap();
        let root = root(temporary.path());
        let state = OpenAdmissionState::new(identity(&path));
        assert_eq!(format!("{state:?}"), "OpenAdmissionState { .. }");
        assert_eq!(format!("{state}"), "OpenAdmissionState");
        let future = state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive);
        send_future(&future);
        drop(future);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("unpolled future has no admission"));
        assert_eq!(std::fs::read(&path).unwrap(), b"lazy-original");
    }

    // STA-T03/T08：下层真实冲突/非空载体不能永久缓存失败。仅在没有任何
    // 持有者时离线修复同一个测试载体；目录来源也不能借已有SH冒充普通文件。
    #[test]
    fn test_admission_state_retries_real_failures_without_losing_existing_admission() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"failure-original").unwrap();
        let root = root(temporary.path());
        let state = OpenAdmissionState::new(identity(&path));
        let external = OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap();
        assert_eq!(block_on(state.ensure(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(external);
        let carriers = std::fs::read_dir(temporary.path()).unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.file_name().unwrap().to_string_lossy().starts_with(".pi_async_fs-open-v1-"))
            .collect::<Vec<_>>();
        assert_eq!(carriers.len(), 1);
        std::fs::write(&carriers[0], b"invalid-carrier").unwrap();
        assert_eq!(block_on(state.ensure(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Corrupted);
        assert_eq!(std::fs::read(&carriers[0]).unwrap(), b"invalid-carrier");
        let directory_pin = || {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS);
            }
            options.open(temporary.path()).expect("real directory pin")
        };
        assert_eq!(block_on(state.ensure(
            Arc::clone(&root), directory_pin(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        std::fs::write(&carriers[0], []).unwrap();
        block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
            .expect("same state retries after real repair");
        assert_eq!(block_on(state.ensure(
            Arc::clone(&root), directory_pin(), OpenAdmissionKind::Shared,
        )).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
            .expect("bad reuse input preserves existing shared qualification");
        drop(state);
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("no retained failed or final qualification"));
        assert_eq!(std::fs::read(&path).unwrap(), b"failure-original");
        assert_eq!(std::fs::metadata(&carriers[0]).unwrap().len(), 0);
    }

    // STA-T04：两轮各八个真实线程仅验证一轮初始化竞争，不测试负载或吞吐。
    // 同状态保持到全部线程结束：SH精确八成功；EX精确一成功/七冲突/零其它错误。
    #[test]
    fn test_admission_state_concurrent_shared_and_exclusive_have_exact_outcomes() {
        for exclusive in [false, true] {
            let temporary = fixture();
            let path = temporary.path().join("data.bin");
            std::fs::write(&path, b"concurrent-original").unwrap();
            let root = root(temporary.path());
            let state = Arc::new(OpenAdmissionState::new(identity(&path)));
            let start = Arc::new(std::sync::Barrier::new(9));
            let mut workers = Vec::new();
            for _ in 0..8 {
                let state = Arc::clone(&state);
                let root = Arc::clone(&root);
                let start = Arc::clone(&start);
                let pin = std::fs::File::open(&path).unwrap();
                workers.push(std::thread::spawn(move || {
                    start.wait();
                    block_on(state.ensure(root, pin, if exclusive { OpenAdmissionKind::Exclusive } else { OpenAdmissionKind::Shared }))
                }));
            }
            start.wait();
            let (mut successes, mut conflicts, mut other_errors) = (0, 0, 0);
            for worker in workers {
                match worker.join().expect("thread completes") {
                    Ok(()) => successes += 1,
                    Err(error) if error.current_context() == &pi_result::ErrorKind::Conflict => conflicts += 1,
                    Err(_) => other_errors += 1,
                }
            }
            assert_eq!((successes, conflicts, other_errors), if exclusive { (1, 7, 0) } else { (8, 0, 0) });
            assert_eq!(OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
            let weak_state = Arc::downgrade(&state);
            drop(state);
            assert!(weak_state.upgrade().is_none());
            drop(OwnedOpenAdmission::try_acquire(
                root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).expect("all temporary threads and final state release qualification"));
            assert_eq!(std::fs::read(&path).unwrap(), b"concurrent-original");
        }
    }

    // 隔离子进程只配置真实阻塞池和全局容量，避免修改主测试进程环境。
    // 30秒看门狗终止并回收挂起子进程；成功还须有实际子测试完成标记。
    fn run_isolated_child(name: &str) -> bool {
        const ENVIRONMENT: &str = "PI_ASYNC_FS_ADMISSION_STATE_CHILD";
        if std::env::var(ENVIRONMENT).as_deref() == Ok(name) { return false; }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(name).arg("--exact").arg("--nocapture")
            .env(ENVIRONMENT, name).env("BLOCKING_MAX_THREADS", "1")
            .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped())
            .spawn().expect("isolated child");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if child.try_wait().unwrap().is_some() { break; }
            if std::time::Instant::now() >= deadline {
                child.kill().expect("kill timed-out child");
                let output = child.wait_with_output().expect("reap timed-out child");
                panic!("admission state child timed out: {}", String::from_utf8_lossy(&output.stderr));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().expect("reap child");
        assert!(output.status.success(), "child failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("STATE_CHILD_VERIFIED"));
        assert!(stdout.contains("1 passed; 0 failed; 0 ignored"));
        true
    }

    // STA-T06：空槽和已有SH各一次：单真实worker忙碌时取消已排队申请和
    // 另一个等门申请，再Drop状态。空槽未执行就取消；已有SH仍须排斥EX，
    // 直到执行器实际完成取消收尾。无假执行器/状态/生产测试钩子。
    #[test]
    fn test_admission_state_cancellation_keeps_queued_initialization_owned_until_completion() {
        if run_isolated_child("open_admission_state::open_admission_state_tests::test_admission_state_cancellation_keeps_queued_initialization_owned_until_completion") { return; }
        for published in [false, true] {
            let temporary = fixture();
            let path = temporary.path().join("data.bin");
            std::fs::write(&path, b"cancel-original").unwrap();
            let root = self::root(temporary.path());
            let weak_root = Arc::downgrade(&root);
            let state = OpenAdmissionState::new(identity(&path));
            if published {
                block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
                    .expect("published admission before queued reuse");
            }
            let (started_sender, started_receiver) = std::sync::mpsc::channel();
            let (release_sender, release_receiver) = std::sync::mpsc::channel();
            let occupied = blocking::unblock(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv_timeout(std::time::Duration::from_secs(15)).expect("release occupied worker");
            });
            started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            let mut first = Box::pin(state.ensure(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared,
            ));
            assert!(block_on(futures_lite::future::poll_once(first.as_mut())).is_none());
            let mut waiting = Box::pin(state.ensure(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ));
            assert!(block_on(futures_lite::future::poll_once(waiting.as_mut())).is_none());
            assert_eq!(format!("{state:?}"), "OpenAdmissionState { .. }");
            assert_eq!(format!("{state}"), "OpenAdmissionState");
            assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), if published { 2 } else { 1 });
            drop(waiting);
            drop(first);
            drop(state);
            drop(root);
            assert!(weak_root.upgrade().is_some(), "queued task still owns native inputs");
            if published {
                assert_eq!(OwnedOpenAdmission::try_acquire(
                    weak_root.upgrade().unwrap(), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
                ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
            }
            release_sender.send(()).unwrap();
            block_on(occupied);
            // 当前解析blocking 1.7.0队列为FIFO，单worker后提交的真实任务完成意味着
            // 此前排队任务已执行或被执行器销毁。不能冒称取消任务必然执行闭包。
            block_on(blocking::unblock(|| ()));
            assert!(weak_root.upgrade().is_none(), "actual cleanup releases final root owner");
            assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), if published { 2 } else { 1 });
            let root = self::root(temporary.path());
            drop(OwnedOpenAdmission::try_acquire(
                root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).expect("qualification is available after real cleanup"));
            assert_eq!(std::fs::read(&path).unwrap(), b"cancel-original");
            assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 2);
        }
        println!("STATE_CHILD_VERIFIED cancellation");
    }

    // STA-T07：真实全局容量一分别拒绝空槽取得和已有SH复用，错误必须是
    // ResourceExhausted；未提交任务不能发布/损坏资格，释放容量后同状态可重试。
    // 仅子进程设置一次容量，不修改主进程全局设施或用测试模型代替准入。
    #[test]
    fn test_admission_state_capacity_rejection_is_retryable_and_preserves_published_admission() {
        if run_isolated_child("open_admission_state::open_admission_state_tests::test_admission_state_capacity_rejection_is_retryable_and_preserves_published_admission") { return; }
        crate::set_local_blocking_capacity(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
        for published in [false, true] {
            let temporary = fixture();
            let path = temporary.path().join("data.bin");
            std::fs::write(&path, b"capacity-original").unwrap();
            let root = root(temporary.path());
            let state = OpenAdmissionState::new(identity(&path));
            if published {
                block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
                    .expect("published shared admission");
            }
            let (started_sender, started_receiver) = std::sync::mpsc::channel();
            let (release_sender, release_receiver) = std::sync::mpsc::channel();
            let occupied = async_global_executor::spawn(crate::bounded_blocking::unblock(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv_timeout(std::time::Duration::from_secs(15)).expect("release bounded worker");
            }));
            started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            assert_eq!(block_on(state.ensure(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared,
            )).unwrap_err().current_context(), &pi_result::ErrorKind::ResourceExhausted);
            let independent = OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            );
            if published {
                assert_eq!(independent.unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
            } else {
                drop(independent.expect("rejected first acquisition did not publish qualification"));
            }
            release_sender.send(()).unwrap();
            block_on(occupied).expect("bounded task completes and releases capacity");
            block_on(state.ensure(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared))
                .expect("same state retries after capacity returns");
            drop(state);
            drop(OwnedOpenAdmission::try_acquire(
                root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).expect("last state drop releases qualification"));
            assert_eq!(std::fs::read(&path).unwrap(), b"capacity-original");
        }
        println!("STATE_CHILD_VERIFIED capacity");
    }
}
