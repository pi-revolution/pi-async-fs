//! 公开独占入口的真实文件系统组合与生命周期测试。
//!
//! 只通过FileNamespace/FileIo驱动生产能力；标准文件接口仅建立夹具、独立
//! 核对数据或观察不合作访问的明确边界。每项在独立子进程配置真实共同根，
//! 不重置生产全局状态。仅有界功能竞争，不进行压力或性能测量，必须串行。
//! 对应SI-071 PUB-A01~06；已有内部完成链证据为SI-070 CMP-A01~05。

use std::path::{Path, PathBuf};
use std::borrow::Cow;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_lite::future::block_on;
use pi_async_fs::{
    set_cross_process_coordination_root, CopyStagingEvidence, CopyTargetEvidence,
    CrossProcessCoordinationRoot,
    DetachableWriteBuffer, ExclusiveFileOwner, FileAccessMode, FileFlushMode, FileIo, FileNamespace,
    LocalFile, LocalFileNamespace, MmapRange, ReadGrowthLimit, ReadTargetRegion, TransferProgress,
};
use pi_result::ErrorKind;

// Windows夹具放项目target，Linux只用可自动清理的小型真实磁盘目录。
fn fixture() -> tempfile::TempDir {
    #[cfg(windows)]
    {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("exclusive-public-fixtures");
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new().prefix("public-").tempdir_in(root).unwrap()
    }
    #[cfg(not(windows))]
    { tempfile::Builder::new().prefix("pi-async-fs-public-").tempdir().unwrap() }
}

// 只隔离一次性配置，不包装或代实现任何生产任务；超时先结束子进程再收输出。
fn isolated(test_name: &str) -> bool {
    const CHILD: &str = "PI_ASYNC_FS_EXCLUSIVE_PUBLIC_CHILD";
    if std::env::var(CHILD).as_deref() == Ok(test_name) { return false; }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([test_name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CHILD, test_name).env("BLOCKING_MAX_THREADS", "1")
        .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("public exclusive child timed out: {}", String::from_utf8_lossy(&output.stderr));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "child failed\n{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("1 passed; 0 failed; 0 ignored"));
    assert!(stdout.contains("PUBLIC_EXCLUSIVE_VERIFIED"));
    print!("{stdout}");
    true
}

// 共同物理根由公开配置入口建立；目录由夹具创建，生产自己创建其协议资产。
fn configure(directory: &Path) -> PathBuf {
    let root = directory.join("common-root");
    std::fs::create_dir(&root).unwrap();
    set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(root.clone())).unwrap();
    root
}

#[test]
// PUB-F01：真实SH先/EX先、重复EX、六角色及跨目录硬链接；结果拥有Send/Sync，
// 可离开namespace借用栈在线程内释放；独占不改变内容，也不阻止只读元信息查询。
fn test_public_exclusive_open_arbitrates_all_roles_and_owns_its_lifetime() {
    if isolated("test_public_exclusive_open_arbitrates_all_roles_and_owns_its_lifetime") { return; }
    fn assert_owner_traits<T: Send + Sync + 'static>() {}
    assert_owner_traits::<ExclusiveFileOwner>();
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("source-中文");
    let other_directory = temporary.path().join("alias-directory");
    std::fs::create_dir(&other_directory).unwrap();
    let alias = other_directory.join("another-name");
    std::fs::write(&path, b"original-content").unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    let namespace = LocalFileNamespace::new();
    let ordinary = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    assert_eq!(block_on(namespace.open_exclusive(&alias)).unwrap_err().current_context(), &ErrorKind::Conflict);
    drop(ordinary);
    let owner = block_on(namespace.open_exclusive(&path)).unwrap();
    assert_eq!(format!("{owner:?}"), "ExclusiveFileOwner { .. }");
    assert_eq!(owner.to_string(), "ExclusiveFileOwner(<redacted>)");
    assert_eq!(std::fs::read(&path).unwrap(), b"original-content");
    for location in [&path, &alias] {
        for access in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
            FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap] {
            let error = match block_on(namespace.clone().open(location, access)) {
                Ok(_) => panic!("ordinary acquisition bypassed exclusive owner"), Err(error) => error,
            };
            assert_eq!(error.current_context(), &ErrorKind::Conflict);
        }
        assert_eq!(block_on(namespace.open_exclusive(location)).unwrap_err().current_context(), &ErrorKind::Conflict);
        assert!(block_on(namespace.try_exists(location)).unwrap());
    }
    std::thread::spawn(move || drop(owner)).join().unwrap();
    let reopened = block_on(namespace.open(&alias, FileAccessMode::Read)).unwrap();
    drop(reopened);
    assert_eq!(std::fs::read(&path).unwrap(), b"original-content");
    println!("PUBLIC_EXCLUSIVE_VERIFIED acquisition");
}

#[test]
// PUB-F03/A02~03：显式六角色派生、实际不相交RO/RW映射、两个独立追加通道
// 及两类文件刷新共存；相交/普通读/覆盖/截断/同文件写源仍拒绝。最后RO克隆
// 在线程中保留所有者资格，不能用外部owner或原File的Drop冒充最终释放。
fn test_public_exclusive_derivation_preserves_roles_and_last_mapping_lifetime() {
    if isolated("test_public_exclusive_derivation_preserves_roles_and_last_mapping_lifetime") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("source");
    let alias = temporary.path().join("alias");
    let wrong = temporary.path().join("wrong");
    std::fs::write(&path, b"0123456789abcdef").unwrap();
    std::fs::write(&wrong, b"untouched").unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    let namespace = LocalFileNamespace::new();
    let owner = block_on(namespace.open_exclusive(&path)).unwrap();
    let error = match block_on(namespace.open_with_exclusive_owner(&owner, &wrong, FileAccessMode::Read)) {
        Ok(_) => panic!("wrong file inherited owner qualification"), Err(error) => error,
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    let derive = |access| block_on(namespace.open_with_exclusive_owner(&owner, &alias, access)).unwrap();
    let reader = derive(FileAccessMode::Read);
    let mut append = derive(FileAccessMode::Append);
    let mut second_append = derive(FileAccessMode::Append);
    let mut overwrite = derive(FileAccessMode::Overwrite);
    let mut truncate = derive(FileAccessMode::Truncate);
    let read_mapping = derive(FileAccessMode::ReadMmap);
    let mut write_mapping = derive(FileAccessMode::ReadWriteMmap);
    let mut original = [0_u8; 16];
    block_on(reader.read_exact_at(0, &mut original, ReadTargetRegion::full())).unwrap();
    assert_eq!(&original, b"0123456789abcdef");
    // SAFETY: 独立夹具仅由本测试使用本库访问；无人缩短、替换或旁路修改映射范围。
    let readonly = unsafe { block_on(read_mapping.map_read_only_uncoordinated(MmapRange::new(0, 4).unwrap())) }.unwrap();
    // SAFETY: 与RO逻辑范围不相交，后续文件写入仅用本库受管尾部追加。
    let mut writable = unsafe { block_on(write_mapping.map_read_write_uncoordinated(MmapRange::new(4, 8).unwrap())) }.unwrap();
    writable.as_bytes_mut().copy_from_slice(b"WXYZ");
    block_on(writable.flush()).unwrap();
    block_on(append.append(Vec::from(&b"+"[..]))).unwrap();
    block_on(second_append.append(bytes::Bytes::from_static(b"+"))).unwrap();
    block_on(append.flush(FileFlushMode::Data)).unwrap();
    block_on(write_mapping.flush(FileFlushMode::DataAndMetadata)).unwrap();
    assert_eq!(readonly.as_bytes(), b"0123");
    assert_eq!(writable.as_bytes(), b"WXYZ");
    // SAFETY: 仅申请已占用范围；生产应在原生建图前拒绝，仍遵守外部稳定性前提。
    let error = unsafe { block_on(read_mapping.map_read_only_uncoordinated(MmapRange::new(3, 5).unwrap())) }.unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(block_on(reader.read_exact_at(0, &mut original, ReadTargetRegion::full())).unwrap_err().current_context(), &ErrorKind::Conflict);
    assert_eq!(block_on(overwrite.overwrite_all(Vec::from(&b"blocked"[..]))).unwrap_err().error().current_context(), &ErrorKind::Conflict);
    assert_eq!(block_on(truncate.truncate(0)).unwrap_err().current_context(), &ErrorKind::Conflict);
    let failure = block_on(append.append(readonly.clone())).unwrap_err();
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(failure.buffer().as_bytes(), b"0123");
    drop(failure);
    assert_eq!(block_on(namespace.remove_file(&path)).unwrap_err().error().current_context(), &ErrorKind::Conflict);
    let last_mapping = readonly.clone();
    drop(readonly);
    drop(writable);
    drop(owner);
    drop((reader, append, second_append, overwrite, truncate, read_mapping, write_mapping));
    let error = match block_on(namespace.open(&path, FileAccessMode::Read)) {
        Ok(_) => panic!("last mapping lost owner qualification"), Err(error) => error,
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    std::thread::spawn(move || {
        assert_eq!(last_mapping.as_bytes(), b"0123");
        drop(last_mapping);
    }).join().unwrap();
    drop(block_on(namespace.open(&alias, FileAccessMode::Read)).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"0123WXYZ89abcdef++");
    assert_eq!(std::fs::read(&wrong).unwrap(), b"untouched");
    println!("PUBLIC_EXCLUSIVE_VERIFIED derived-lifetime");
}

#[test]
// PUB-F02：原凭据闲置不占打开资格，受管普通资源先到排斥EX；凭据不允许
// 绕过既有EX。显式派生可以安全映射/追加/刷新，最后映射阻止安全删除，
// 旧绑定不能删除或独占打开同名新对象；不同硬链接名不绕过原内容授权限制。
fn test_public_authorized_exclusive_preserves_binding_and_content_protocol() {
    if isolated("test_public_authorized_exclusive_preserves_binding_and_content_protocol") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("managed");
    let alias = temporary.path().join("managed-alias");
    std::fs::write(&path, b"managed-original").unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    let namespace = LocalFileNamespace::new();
    // SAFETY: 独立夹具的全部访问都遵守本库同一物理根/名称协议，不存在库外写者。
    let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
    let ordinary = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read)).unwrap();
    assert_eq!(block_on(namespace.open_exclusive_with_cross_process_authority(&authority)).unwrap_err().current_context(), &ErrorKind::Conflict);
    drop(ordinary);
    let owner = block_on(namespace.open_exclusive_with_cross_process_authority(&authority)).unwrap();
    let error = match block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Append)) {
        Ok(_) => panic!("authority alone bypassed EX"), Err(error) => error,
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert!(block_on(namespace.open_with_exclusive_owner(&owner, &alias, FileAccessMode::Read)).is_err());
    let mapper = block_on(namespace.open_with_exclusive_owner(&owner, &path, FileAccessMode::ReadMmap)).unwrap();
    let mut append = block_on(namespace.open_with_exclusive_owner(&owner, &path, FileAccessMode::Append)).unwrap();
    let mapping = block_on(mapper.map_read_only(MmapRange::new(0, 7).unwrap())).unwrap();
    block_on(append.append(Vec::from(&b"+"[..]))).unwrap();
    block_on(append.flush(FileFlushMode::DataAndMetadata)).unwrap();
    assert_eq!(mapping.as_bytes(), b"managed");
    assert_eq!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap_err().error().current_context(), &ErrorKind::Conflict);
    drop((owner, mapper, append));
    assert_eq!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap_err().error().current_context(), &ErrorKind::Conflict);
    drop(mapping);
    block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap();
    assert!(!path.exists());
    std::fs::write(&path, b"replacement").unwrap();
    assert!(block_on(namespace.open_exclusive_with_cross_process_authority(&authority)).is_err());
    assert!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    // 原别名仍指向被解除名称的旧对象，内容完整并含已完成追加。
    assert_eq!(std::fs::read(&alias).unwrap(), b"managed-original+");
    println!("PUBLIC_EXCLUSIVE_VERIFIED authorized-owner");
}

#[test]
// PUB-F04/A02/A04：普通/受管源×空/单字节/窗口两侧，公开owner复制的精确内容
// 及结果长度；位置型复制不能绕过独占，错源/既有目标/活动映射失败保留两项
// 独立证据与原资产。正常复制仍可与公共读取组合，不把API返回Ok当唯一证据。
fn test_public_exclusive_copy_preserves_source_checks_and_both_evidence_values() {
    if isolated("test_public_exclusive_copy_preserves_source_checks_and_both_evidence_values") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let namespace = LocalFileNamespace::new();
    for managed in [false, true] {
        for length in [0, 1, 65_535, 65_536, 65_537] {
            let source = temporary.path().join(format!("source-{managed}-{length}"));
            let destination = temporary.path().join(format!("destination-{managed}-{length}"));
            let wrong = temporary.path().join(format!("wrong-{managed}-{length}"));
            let blocked = temporary.path().join(format!("blocked-{managed}-{length}"));
            let content = vec![b'Q'; length];
            std::fs::write(&source, &content).unwrap();
            std::fs::write(&wrong, b"unrelated").unwrap();
            let authority = if managed {
                // SAFETY: 独立受管夹具无旁路内容访问或名称变更，所有参与者使用本库。
                Some(unsafe { block_on(namespace.establish_cross_process_authority(&source)) }.unwrap())
            } else { None };
            let owner = match authority.as_ref() {
                Some(authority) => block_on(namespace.open_exclusive_with_cross_process_authority(authority)),
                None => block_on(namespace.open_exclusive(&source)),
            }.unwrap();
            let ordinary_failure = block_on(namespace.copy_new(&source, &blocked)).unwrap_err();
            assert_eq!(ordinary_failure.error().current_context(), &ErrorKind::Conflict);
            assert_eq!(ordinary_failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(ordinary_failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            let wrong_failure = block_on(namespace.copy_new_with_exclusive_owner(&owner, &wrong, &blocked)).unwrap_err();
            assert!(matches!(wrong_failure.error().current_context(), ErrorKind::Conflict | ErrorKind::NotFound));
            assert_eq!(wrong_failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(wrong_failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            assert!(!blocked.exists());
            let outcome = block_on(namespace.copy_new_with_exclusive_owner(&owner, &source, &destination)).unwrap();
            assert_eq!(outcome.content_byte_len(), length as u64);
            assert_eq!(std::fs::read(&destination).unwrap(), content);
            let target_reader = block_on(namespace.open(&destination, FileAccessMode::Read)).unwrap();
            let mut read_back = vec![0; length];
            block_on(target_reader.read_exact_at(0, &mut read_back, ReadTargetRegion::full())).unwrap();
            assert_eq!(read_back, content);
            let failure = block_on(namespace.copy_new_with_exclusive_owner(&owner, &source, &destination)).unwrap_err();
            assert_eq!(failure.error().current_context(), &ErrorKind::AlreadyExists);
            assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            if length > 0 {
                let mapper = block_on(namespace.open_with_exclusive_owner(&owner, &source, FileAccessMode::ReadMmap)).unwrap();
                let mapping = match authority.as_ref() {
                    Some(_) => block_on(mapper.map_read_only(MmapRange::new(0, 1).unwrap())),
                    // SAFETY: 独立普通夹具无库外写者，映射生存期内仅本库读取复制申请。
                    None => unsafe { block_on(mapper.map_read_only_uncoordinated(MmapRange::new(0, 1).unwrap())) },
                }.unwrap();
                let failure = block_on(namespace.copy_new_with_exclusive_owner(&owner, &source, &blocked)).unwrap_err();
                assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
                assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
                assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
                assert_eq!(mapping.as_bytes(), b"Q");
            }
            assert_eq!(std::fs::read(&source).unwrap(), content);
            assert_eq!(std::fs::read(&wrong).unwrap(), b"unrelated");
            assert!(!blocked.exists());
            assert!(!std::fs::read_dir(temporary.path()).unwrap().any(|entry| entry.unwrap().file_name().to_string_lossy().ends_with(".copy")));
        }
    }
    println!("PUBLIC_EXCLUSIVE_VERIFIED owner-copy");
}

#[test]
// PUB-F01/A04：构造或丢弃未poll的Future不冻结配置、不打开文件；首poll之后
// 即使失败也固定配置。目标在构造后创建可被观察，错误后不存在幽灵资格。
fn test_public_exclusive_open_is_lazy_and_first_poll_freezes_configuration() {
    if isolated("test_public_exclusive_open_is_lazy_and_first_poll_freezes_configuration") { return; }
    let temporary = fixture();
    let namespace = LocalFileNamespace::new();
    let path = temporary.path().join("late-source");
    drop(namespace.open_exclusive(&path));
    let pending = namespace.open_exclusive(&path);
    configure(temporary.path());
    assert_eq!(block_on(pending).unwrap_err().current_context(), &ErrorKind::NotFound);
    assert_eq!(set_cross_process_coordination_root(CrossProcessCoordinationRoot::FileParent).unwrap_err().current_context(), &ErrorKind::Conflict);
    let pending = namespace.open_exclusive(&path);
    std::fs::write(&path, b"created-later").unwrap();
    let owner = block_on(pending).unwrap();
    drop(owner);
    drop(block_on(namespace.open(&path, FileAccessMode::Read)).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"created-later");
    println!("PUBLIC_EXCLUSIVE_VERIFIED laziness");
}

#[test]
// PUB-F01/A04：默认根明确不支持完整独占，普通打开仍有效；尝试失败不新增
// 协调资产、不修改数据，首poll固定默认策略后不能再切换到另一域。
fn test_public_exclusive_open_default_root_is_unsupported_without_assets() {
    if isolated("test_public_exclusive_open_default_root_is_unsupported_without_assets") { return; }
    let temporary = fixture();
    let path = temporary.path().join("source");
    std::fs::write(&path, b"default-original").unwrap();
    let namespace = LocalFileNamespace::new();
    assert_eq!(block_on(namespace.open_exclusive(&path)).unwrap_err().current_context(), &ErrorKind::Unsupported);
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
    assert_eq!(set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(temporary.path().to_path_buf())).unwrap_err().current_context(), &ErrorKind::Conflict);
    drop(block_on(namespace.open(&path, FileAccessMode::Read)).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"default-original");
    println!("PUBLIC_EXCLUSIVE_VERIFIED default-root");
}

#[test]
// PUB-F01/A04：不存在、目录及共同根失效均明确失败，不自动创建/降级；
// 非文件目标在取得资格前拒绝，未留下资产，后续合法打开仍能成功。
fn test_public_exclusive_open_rejects_invalid_targets_and_unavailable_root() {
    if isolated("test_public_exclusive_open_rejects_invalid_targets_and_unavailable_root") { return; }
    let temporary = fixture();
    let root = configure(temporary.path());
    let namespace = LocalFileNamespace::new();
    let missing = temporary.path().join("missing");
    assert_eq!(block_on(namespace.open_exclusive(&missing)).unwrap_err().current_context(), &ErrorKind::NotFound);
    assert_eq!(block_on(namespace.open_exclusive(&root)).unwrap_err().current_context(), &ErrorKind::Unsupported);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    assert!(!missing.exists());
    std::fs::write(&missing, b"valid").unwrap();
    std::fs::remove_dir(&root).unwrap();
    assert_eq!(block_on(namespace.open_exclusive(&missing)).unwrap_err().current_context(), &ErrorKind::NotFound);
    assert!(!root.exists(), "unavailable configured root must not be silently recreated");
    std::fs::create_dir(&root).unwrap();
    drop(block_on(namespace.open_exclusive(&missing)).unwrap());
    assert_eq!(std::fs::read(&missing).unwrap(), b"valid");
    println!("PUBLIC_EXCLUSIVE_VERIFIED invalid-targets");
}

#[cfg(target_os = "linux")]
#[test]
// PUB-F01：真实只读权限文件必须拒绝默认读写独占，不能仅用read权限伪造
// 成功。普通只读仍可读；恢复权限后能取得EX，错误没有遗留资格。非root环境。
fn test_public_exclusive_open_requires_real_read_write_permission() {
    if isolated("test_public_exclusive_open_requires_real_read_write_permission") { return; }
    use std::os::unix::fs::PermissionsExt;
    // SAFETY：geteuid无指针参数，只读取当前进程有效用户号；不修改权限或状态。
    assert_ne!(unsafe { libc::geteuid() }, 0, "permission evidence requires non-root user");
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("read-only");
    std::fs::write(&path, b"readonly").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
    let namespace = LocalFileNamespace::new();
    let result = block_on(namespace.open_exclusive(&path));
    let ordinary = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    let mut bytes = [0; 8];
    block_on(ordinary.read_exact_at(0, &mut bytes, ReadTargetRegion::full())).unwrap();
    drop(ordinary);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(result.unwrap_err().current_context(), &ErrorKind::PermissionDenied);
    assert_eq!(&bytes, b"readonly");
    drop(block_on(namespace.open_exclusive(&path)).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"readonly");
    println!("PUBLIC_EXCLUSIVE_VERIFIED read-write-permission");
}

#[test]
// PUB-A03/E-T03：两线程在不同硬链接位置同时发起SH/EX或EX/EX，获胜资源
// 一直握到双方结果都已产生，严格断言1成功、1Conflict、0其它错误；只一轮
// 最小竞争，不做重复高负载。最终释放后普通/独占均可重新取得，数据不变。
fn test_public_exclusive_thread_races_have_exactly_one_winner() {
    if isolated("test_public_exclusive_thread_races_have_exactly_one_winner") { return; }
    use std::sync::{Arc, Barrier};
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("source");
    let alias = temporary.path().join("alias");
    std::fs::write(&path, b"race-original").unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    for both_exclusive in [false, true] {
        let start = Arc::new(Barrier::new(2));
        let acquired = Arc::new(Barrier::new(2));
        let threads = [path.clone(), alias.clone()].into_iter().enumerate().map(|(index, locator)| {
            let start = Arc::clone(&start);
            let acquired = Arc::clone(&acquired);
            std::thread::spawn(move || {
                let namespace = LocalFileNamespace::new();
                start.wait();
                if index == 0 || both_exclusive {
                    let result = block_on(namespace.open_exclusive(&locator));
                    let category = result.as_ref().map(|_| ()).map_err(|error| *error.current_context());
                    acquired.wait();
                    drop(result);
                    category
                } else {
                    let result = block_on(namespace.open(&locator, FileAccessMode::Append));
                    let category = result.as_ref().map(|_| ()).map_err(|error| *error.current_context());
                    acquired.wait();
                    drop(result);
                    category
                }
            })
        }).collect::<Vec<_>>();
        let results = threads.into_iter().map(|thread| thread.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| matches!(result, Err(ErrorKind::Conflict))).count(), 1);
        assert_eq!(results.iter().filter(|result| matches!(result, Err(error) if *error != ErrorKind::Conflict)).count(), 0);
        let namespace = LocalFileNamespace::new();
        drop(block_on(namespace.open_exclusive(&path)).unwrap());
        drop(block_on(namespace.open(&alias, FileAccessMode::Read)).unwrap());
    }
    assert_eq!(std::fs::read(&path).unwrap(), b"race-original");
    println!("PUBLIC_EXCLUSIVE_VERIFIED thread-races");
}

// 只把同一真实buffer依次交给错误角色和合法派生追加，核验错误资产保持原
// 地址/字节/精确零进度；不代理生产detach/recover、串行化或错误处理。
fn append_after_role_error<B>(append: &mut LocalFile, reader: &mut LocalFile, buffer: B)
where
    B: DetachableWriteBuffer + Send,
    B::Detached: Send,
    B::Recovery: Send,
{
    let address = buffer.as_ref().as_ptr();
    let failure = block_on(reader.append(buffer)).unwrap_err();
    assert_eq!(failure.error().current_context(), &ErrorKind::InvalidState);
    assert_eq!(failure.buffer().as_ref(), b"Q");
    assert_eq!(failure.buffer().as_ref().as_ptr(), address);
    assert_eq!(failure.progress(), &TransferProgress::Exact { bytes: 0 });
    let (_, buffer, _) = failure.into_parts();
    block_on(append.append(buffer)).unwrap();
}

// 固定读取通过公开派生资源验证所有合法AsMut载体；期望值为独立已知前缀。
fn check_fixed_read<B: AsMut<[u8]> + Send + ?Sized>(reader: &LocalFile, buffer: &mut B) {
    assert_eq!(block_on(reader.read_at(0, buffer, ReadTargetRegion::full())).unwrap(), 4);
    assert_eq!(buffer.as_mut(), b"base");
    block_on(reader.read_exact_at(0, buffer, ReadTargetRegion::full())).unwrap();
    assert_eq!(buffer.as_mut(), b"base");
}

#[test]
// E-T04/PUB-A02：15个常用普通写buffer形态实际走错误返还→合法追加，固定读
// 的Vec/Box/BytesMut/数组/切片/异文件RW映射，增长读的未初始化备用容量；
// 同步/异步角色组合不改变原buffer合同，外层所有者可在通道存活时先释放。
fn test_public_exclusive_derived_resources_accept_all_builtin_buffer_shapes() {
    if isolated("test_public_exclusive_derived_resources_accept_all_builtin_buffer_shapes") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("source");
    let read_target = temporary.path().join("read-target");
    std::fs::write(&path, b"base:").unwrap();
    std::fs::write(&read_target, b"----").unwrap();
    let namespace = LocalFileNamespace::new();
    let owner = block_on(namespace.open_exclusive(&path)).unwrap();
    let mut reader = block_on(namespace.open_with_exclusive_owner(&owner, &path, FileAccessMode::Read)).unwrap();
    let mut append = block_on(namespace.open_with_exclusive_owner(&owner, &path, FileAccessMode::Append)).unwrap();
    drop(owner);
    macro_rules! check { ($buffer:expr) => { append_after_role_error(&mut append, &mut reader, $buffer) }; }
    check!(Vec::from(&b"Q"[..]));
    check!(Vec::from(&b"Q"[..]).into_boxed_slice());
    check!(Arc::<[u8]>::from(&b"Q"[..]));
    check!(Cow::<'_, [u8]>::Owned(Vec::from(&b"Q"[..])));
    check!(Cow::<'_, [u8]>::Borrowed(b"Q"));
    check!(bytes::Bytes::from_static(b"Q"));
    check!(bytes::BytesMut::from(&b"Q"[..]));
    check!(&b"Q"[..]);
    let vector = Vec::from(&b"Q"[..]); check!(&vector);
    let boxed = vector.clone().into_boxed_slice(); check!(&boxed);
    let shared = Arc::<[u8]>::from(&b"Q"[..]); check!(&shared);
    let borrowed_cow: Cow<'_, [u8]> = Cow::Borrowed(b"Q"); check!(&borrowed_cow);
    let owned_cow: Cow<'_, [u8]> = Cow::Owned(vector.clone()); check!(&owned_cow);
    let bytes = bytes::Bytes::from_static(b"Q"); check!(&bytes);
    let bytes_mut = bytes::BytesMut::from(&b"Q"[..]); check!(&bytes_mut);
    block_on(append.append(Vec::<u8>::new())).unwrap();
    block_on(append.flush(FileFlushMode::Data)).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"base:QQQQQQQQQQQQQQQ");
    check_fixed_read(&reader, &mut vec![0; 4]);
    check_fixed_read(&reader, &mut vec![0; 4].into_boxed_slice());
    check_fixed_read(&reader, &mut bytes::BytesMut::from(&b"----"[..]));
    check_fixed_read(&reader, &mut [0; 4]);
    let mut stack = [0; 4]; check_fixed_read(&reader, &mut stack[..]);
    let destination = block_on(namespace.open(&read_target, FileAccessMode::ReadWriteMmap)).unwrap();
    // SAFETY: 映射另一独立文件且无人缩短/替换，固定读只写此可写目标范围。
    let mut target_mapping = unsafe { block_on(destination.map_read_write_uncoordinated(MmapRange::new(0, 4).unwrap())) }.unwrap();
    check_fixed_read(&reader, &mut target_mapping);
    block_on(target_mapping.flush()).unwrap();
    drop(target_mapping);
    assert_eq!(std::fs::read(&read_target).unwrap(), b"base");
    let mut vector_spare = Vec::<u8>::with_capacity(64);
    let outcome = block_on(reader.read_to_end_at(0, &mut vector_spare, ReadGrowthLimit::new(100))).unwrap();
    assert_eq!(outcome.appended_bytes(), 20);
    assert!(outcome.is_end_of_file());
    assert_eq!(vector_spare, b"base:QQQQQQQQQQQQQQQ");
    let mut bytes_spare = bytes::BytesMut::with_capacity(64);
    let outcome = block_on(reader.read_to_end_at(0, &mut bytes_spare, ReadGrowthLimit::new(100))).unwrap();
    assert_eq!(outcome.appended_bytes(), 20);
    assert!(outcome.is_end_of_file());
    assert_eq!(&bytes_spare[..], b"base:QQQQQQQQQQQQQQQ");
    let mut uninitialized = Vec::<u8>::with_capacity(64);
    let nonempty = ReadTargetRegion::new(std::ops::Bound::Included(0), std::ops::Bound::Included(0)).unwrap();
    assert_eq!(block_on(reader.read_at(0, &mut uninitialized, nonempty)).unwrap_err().current_context(), &ErrorKind::InvalidInput);
    assert!(uninitialized.is_empty());
    assert_eq!(block_on(reader.read_at(u64::MAX, &mut uninitialized, ReadTargetRegion::full())).unwrap(), 0);
    drop((reader, append));
    drop(block_on(namespace.open_exclusive(&path)).unwrap());
    println!("PUBLIC_EXCLUSIVE_VERIFIED builtin-buffers");
}

#[test]
// E-T04/E-T06/PUB-A02~03：只读映射作为另一独占文件的拥有/借用/派生切片
// 追加源，错误返还的最后映射保持原文件独占，最后成功消费才释放；源与目标
// 是不同真实文件，不以资格相同或同线程绕过同文件映射写源禁令。
fn test_public_exclusive_mapped_write_sources_keep_the_source_owner_alive() {
    if isolated("test_public_exclusive_mapped_write_sources_keep_the_source_owner_alive") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    std::fs::write(&source, b"mapped").unwrap();
    std::fs::write(&destination, b"base:").unwrap();
    let namespace = LocalFileNamespace::new();
    let source_owner = block_on(namespace.open_exclusive(&source)).unwrap();
    let destination_owner = block_on(namespace.open_exclusive(&destination)).unwrap();
    let mapper = block_on(namespace.open_with_exclusive_owner(&source_owner, &source, FileAccessMode::ReadMmap)).unwrap();
    let mut append = block_on(namespace.open_with_exclusive_owner(&destination_owner, &destination, FileAccessMode::Append)).unwrap();
    let mut wrong_role = block_on(namespace.open_with_exclusive_owner(&destination_owner, &destination, FileAccessMode::Read)).unwrap();
    // SAFETY: 两文件相异，源映射期间没有截断/替换/库外修改；目标只受管追加。
    let mapping = unsafe { block_on(mapper.map_read_only_uncoordinated(MmapRange::new(0, 6).unwrap())) }.unwrap();
    block_on(append.append(mapping.clone())).unwrap();
    block_on(append.append(&mapping)).unwrap();
    block_on(append.append(mapping.as_bytes())).unwrap();
    let failure = block_on(wrong_role.append(mapping.clone())).unwrap_err();
    assert_eq!(failure.error().current_context(), &ErrorKind::InvalidState);
    assert_eq!(failure.progress(), &TransferProgress::Exact { bytes: 0 });
    assert_eq!(failure.buffer().as_bytes(), b"mapped");
    drop((source_owner, destination_owner, mapper, mapping));
    assert_eq!(block_on(namespace.open_exclusive(&source)).unwrap_err().current_context(), &ErrorKind::Conflict);
    let (_, last_mapping, _) = failure.into_parts();
    block_on(append.append(last_mapping)).unwrap();
    block_on(append.flush(FileFlushMode::DataAndMetadata)).unwrap();
    drop(block_on(namespace.open_exclusive(&source)).unwrap());
    assert_eq!(std::fs::read(&source).unwrap(), b"mapped");
    assert_eq!(std::fs::read(&destination).unwrap(), b"base:mappedmappedmappedmapped");
    drop((append, wrong_role));
    drop(block_on(namespace.open_exclusive(&destination)).unwrap());
    println!("PUBLIC_EXCLUSIVE_VERIFIED mapped-sources");
}

#[test]
// E-T07/PUB-A02：默认改名不因独占新增禁令；改名后显式给出新位置仍可派生，
// 旧位置新建是另一对象，不转移独占。安全删除保持活动资源保护，读取和复制
// 使用原对象的精确数据。这里无活动映射，不强行要求OS允许映射期间改名。
fn test_public_exclusive_rename_preserves_object_and_allows_new_name_derivation() {
    if isolated("test_public_exclusive_rename_preserves_object_and_allows_new_name_derivation") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let source = temporary.path().join("source");
    let renamed = temporary.path().join("renamed");
    let copied = temporary.path().join("copied");
    std::fs::write(&source, b"original").unwrap();
    let namespace = LocalFileNamespace::new();
    let owner = block_on(namespace.open_exclusive(&source)).unwrap();
    block_on(namespace.rename(&source, &renamed)).unwrap();
    assert!(!source.exists());
    assert_eq!(std::fs::read(&renamed).unwrap(), b"original");
    assert_eq!(block_on(namespace.open_exclusive(&renamed)).unwrap_err().current_context(), &ErrorKind::Conflict);
    let reader = block_on(namespace.open_with_exclusive_owner(&owner, &renamed, FileAccessMode::Read)).unwrap();
    let mut original = [0; 8];
    block_on(reader.read_exact_at(0, &mut original, ReadTargetRegion::full())).unwrap();
    assert_eq!(&original, b"original");
    block_on(namespace.copy_new_with_exclusive_owner(&owner, &renamed, &copied)).unwrap();
    assert_eq!(std::fs::read(&copied).unwrap(), b"original");
    std::fs::write(&source, b"replacement").unwrap();
    let replacement_owner = block_on(namespace.open_exclusive(&source)).unwrap();
    let wrong = match block_on(namespace.open_with_exclusive_owner(&owner, &source, FileAccessMode::Read)) {
        Ok(_) => panic!("old owner followed a reused name"), Err(error) => error,
    };
    assert_eq!(wrong.current_context(), &ErrorKind::Conflict);
    assert_eq!(block_on(namespace.remove_file(&renamed)).unwrap_err().error().current_context(), &ErrorKind::Conflict);
    drop((owner, reader));
    block_on(namespace.remove_file(&renamed)).unwrap();
    assert_eq!(std::fs::read(&source).unwrap(), b"replacement");
    drop(replacement_owner);
    println!("PUBLIC_EXCLUSIVE_VERIFIED rename-identity");
}

#[cfg(target_os = "linux")]
#[test]
// E-T07/PUB-A02：Linux非协调名称删除不会把打开期独占升级为路径永久占有。
// 原对象仍被映射保活，同名重建可独立独占；原owner不得授权新对象。仅此
// Linux解除名称行为，不外推Windows的延后删除和同名重建时机。
fn test_public_exclusive_unlink_and_recreate_keeps_distinct_object_qualifications() {
    if isolated("test_public_exclusive_unlink_and_recreate_keeps_distinct_object_qualifications") { return; }
    let temporary = fixture();
    configure(temporary.path());
    let path = temporary.path().join("source");
    std::fs::write(&path, b"old-object").unwrap();
    let namespace = LocalFileNamespace::new();
    let old_owner = block_on(namespace.open_exclusive(&path)).unwrap();
    let mapper = block_on(namespace.open_with_exclusive_owner(&old_owner, &path, FileAccessMode::ReadMmap)).unwrap();
    // SAFETY: Linux仅解除名称，旧对象不缩短，映射访问由guard保活，无库外写者。
    let mapping = unsafe { block_on(mapper.map_read_only_uncoordinated(MmapRange::new(0, 10).unwrap())) }.unwrap();
    block_on(namespace.remove_file_uncoordinated(&path)).unwrap();
    assert!(!path.exists());
    std::fs::write(&path, b"new-object").unwrap();
    let new_owner = block_on(namespace.open_exclusive(&path)).unwrap();
    let error = match block_on(namespace.open_with_exclusive_owner(&old_owner, &path, FileAccessMode::Read)) {
        Ok(_) => panic!("old owner authorized a recreated object"), Err(error) => error,
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(mapping.as_bytes(), b"old-object");
    assert_eq!(std::fs::read(&path).unwrap(), b"new-object");
    drop((old_owner, mapper, mapping, new_owner));
    drop(block_on(namespace.open(&path, FileAccessMode::Read)).unwrap());
    println!("PUBLIC_EXCLUSIVE_VERIFIED unlink-recreate");
}

// 子进程由夹具拥有；失败或超时先结束真实进程，不能让其资格/句柄泄漏到后续用例。
struct Peer { child: Option<std::process::Child> }

impl Peer {
    fn finish(mut self) {
        let child = self.child.as_mut().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        let output = self.child.take().unwrap().wait_with_output().unwrap();
        assert!(output.status.success(), "peer failed\n{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("PUBLIC_EXCLUSIVE_PEER_VERIFIED"));
        print!("{stdout}");
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// 只用文件作为测试时序通知，不把这些通知当生产资格载体或释放证据。
fn wait_marker(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(Instant::now() < deadline, "timed out waiting for fixture marker: {}", path.display());
        std::thread::sleep(Duration::from_millis(2));
    }
}

// 精确调用下方同一真实公共测试的peer分支，无测试侧锁协议/资格状态机。
fn spawn_peer(root: &Path, path: &Path, mode: &str, managed: bool, control: Option<&Path>) -> Peer {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["test_public_exclusive_cross_process_arbitration", "--exact", "--nocapture", "--test-threads=1"])
        .env("PI_ASYNC_FS_PUBLIC_PEER_MODE", mode)
        .env("PI_ASYNC_FS_PUBLIC_PEER_ROOT", root)
        .env("PI_ASYNC_FS_PUBLIC_PEER_FILE", path)
        .env("PI_ASYNC_FS_PUBLIC_PEER_MANAGED", if managed { "1" } else { "0" })
        .stdout(Stdio::piped()).stderr(Stdio::piped());
    if let Some(control) = control { command.env("PI_ASYNC_FS_PUBLIC_PEER_CONTROL", control); }
    Peer { child: Some(command.spawn().unwrap()) }
}

// peer与主进程一样只使用真实公开入口；通知只用于明确仲裁时点与持续持有期。
fn execute_peer(mode: &str) {
    let root = PathBuf::from(std::env::var_os("PI_ASYNC_FS_PUBLIC_PEER_ROOT").unwrap());
    let path = PathBuf::from(std::env::var_os("PI_ASYNC_FS_PUBLIC_PEER_FILE").unwrap());
    let control = std::env::var_os("PI_ASYNC_FS_PUBLIC_PEER_CONTROL").map(PathBuf::from);
    let managed = std::env::var("PI_ASYNC_FS_PUBLIC_PEER_MANAGED").unwrap() == "1";
    set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(root)).unwrap();
    let namespace = LocalFileNamespace::new();
    let authority = if managed {
        // SAFETY: 所有参与者使用本库同一可信物理根/目标原生名称和原内容协议。
        Some(unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap())
    } else { None };
    let open_exclusive = || match authority.as_ref() {
        Some(authority) => block_on(namespace.open_exclusive_with_cross_process_authority(authority)),
        None => block_on(namespace.open_exclusive(&path)),
    };
    let open_shared = |access| match authority.as_ref() {
        Some(authority) => block_on(namespace.open_with_cross_process_authority(authority, access)),
        None => block_on(namespace.open(&path, access)),
    };
    match mode {
        "reject-all" => {
            for access in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
                FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap] {
                let error = match open_shared(access) { Ok(_) => panic!("peer bypassed EX"), Err(error) => error };
                assert_eq!(error.current_context(), &ErrorKind::Conflict);
            }
            assert_eq!(open_exclusive().unwrap_err().current_context(), &ErrorKind::Conflict);
        }
        "shared-success" => { drop(open_shared(FileAccessMode::Read).unwrap()); }
        "exclusive-success" => { drop(open_exclusive().unwrap()); }
        "wait-exclusive-success" => {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match open_exclusive() {
                    Ok(owner) => { drop(owner); break; }
                    Err(error) => {
                        assert_eq!(error.current_context(), &ErrorKind::Conflict);
                        assert!(Instant::now() < deadline, "real completion did not release exclusive qualification");
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            }
        }
        "hold-shared" | "hold-exclusive" | "race-shared" | "race-exclusive" => {
            let control = control.as_ref().unwrap();
            let race = mode.starts_with("race-");
            if race {
                std::fs::write(control.join("waiting"), b"waiting").unwrap();
                wait_marker(&control.join("start"));
            }
            let mut held_file = None;
            let mut held_owner = None;
            let error = if mode.ends_with("shared") {
                match open_shared(FileAccessMode::Read) {
                    Ok(file) => { held_file = Some(file); None }, Err(error) => Some(error),
                }
            } else {
                match open_exclusive() {
                    Ok(owner) => { held_owner = Some(owner); None }, Err(error) => Some(error),
                }
            };
            let outcome = match error {
                None => "success",
                Some(error) => { assert!(race); assert_eq!(error.current_context(), &ErrorKind::Conflict); "conflict" }
            };
            std::fs::write(control.join("outcome"), outcome).unwrap();
            std::fs::write(control.join("ready"), b"ready").unwrap();
            wait_marker(&control.join("release"));
            drop((held_file, held_owner));
        }
        _ => panic!("unknown peer mode"),
    }
    println!("PUBLIC_EXCLUSIVE_PEER_VERIFIED {mode}");
}

#[test]
// E-T03/E-T06/E-T09/PUB-A03：真实合作进程SH先/EX先、六角色及硬链接拒绝、
// 最后映射释放后重获，SH/EX和EX/EX同时申请精确一胜。原生不合作打开仅
// 作不强制排斥对照，绝不写入或截断。每竞争只两进程一轮，不是压力测试。
fn test_public_exclusive_cross_process_arbitration() {
    if let Ok(mode) = std::env::var("PI_ASYNC_FS_PUBLIC_PEER_MODE") { execute_peer(&mode); return; }
    if isolated("test_public_exclusive_cross_process_arbitration") { return; }
    let temporary = fixture();
    let root = configure(temporary.path());
    let path = temporary.path().join("source");
    let alias = temporary.path().join("alias");
    std::fs::write(&path, b"process-original").unwrap();
    std::fs::hard_link(&path, &alias).unwrap();
    let namespace = LocalFileNamespace::new();
    let hold = temporary.path().join("hold-control");
    std::fs::create_dir(&hold).unwrap();
    let peer = spawn_peer(&root, &alias, "hold-shared", false, Some(&hold));
    wait_marker(&hold.join("ready"));
    assert_eq!(block_on(namespace.open_exclusive(&path)).unwrap_err().current_context(), &ErrorKind::Conflict);
    std::fs::write(hold.join("release"), b"release").unwrap();
    peer.finish();
    let owner = block_on(namespace.open_exclusive(&path)).unwrap();
    for locator in [&path, &alias] { spawn_peer(&root, locator, "reject-all", false, None).finish(); }
    // 仅打开后立即关闭，不修改内容；证明合作式资格不强制限制原生旁路。
    drop(std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap());
    let mapper = block_on(namespace.open_with_exclusive_owner(&owner, &path, FileAccessMode::ReadMmap)).unwrap();
    // SAFETY: 所有合作peer只申请资源，无库外写入/截断；原生对照已关闭。
    let mapping = unsafe { block_on(mapper.map_read_only_uncoordinated(MmapRange::new(0, 7).unwrap())) }.unwrap();
    let last_mapping = mapping.clone();
    drop((owner, mapper, mapping));
    spawn_peer(&root, &alias, "reject-all", false, None).finish();
    assert_eq!(last_mapping.as_bytes(), b"process");
    drop(last_mapping);
    spawn_peer(&root, &path, "shared-success", false, None).finish();
    spawn_peer(&root, &alias, "exclusive-success", false, None).finish();
    for (index, second_mode) in ["race-shared", "race-exclusive"].into_iter().enumerate() {
        let first = temporary.path().join(format!("race-{index}-first"));
        let second = temporary.path().join(format!("race-{index}-second"));
        std::fs::create_dir(&first).unwrap(); std::fs::create_dir(&second).unwrap();
        let one = spawn_peer(&root, &path, "race-exclusive", false, Some(&first));
        let two = spawn_peer(&root, &alias, second_mode, false, Some(&second));
        wait_marker(&first.join("waiting")); wait_marker(&second.join("waiting"));
        std::fs::write(first.join("start"), b"start").unwrap();
        std::fs::write(second.join("start"), b"start").unwrap();
        wait_marker(&first.join("ready")); wait_marker(&second.join("ready"));
        let results = [std::fs::read_to_string(first.join("outcome")).unwrap(), std::fs::read_to_string(second.join("outcome")).unwrap()];
        let successes = results.iter().filter(|result| *result == "success").count();
        let conflicts = results.iter().filter(|result| *result == "conflict").count();
        std::fs::write(first.join("release"), b"release").unwrap();
        std::fs::write(second.join("release"), b"release").unwrap();
        one.finish(); two.finish();
        assert_eq!((successes, conflicts), (1, 1));
        assert_eq!(results.len() - successes - conflicts, 0);
    }
    drop(block_on(namespace.open_exclusive(&path)).unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"process-original");
    println!("PUBLIC_EXCLUSIVE_VERIFIED cross-process");
}

// 暂停真实共享阻塞worker，只控制环境时序；不代装生产Future或完成任务。
// 即使测试断言失败，Drop也通知worker退出；实际产品资源仍由生产任务自己收尾。
struct PausedWorker {
    release: Option<std::sync::mpsc::Sender<()>>,
    task: Option<async_global_executor::Task<()>>,
}

impl PausedWorker {
    fn new() -> Self {
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let task = async_global_executor::spawn(blocking::unblock(move || {
            started_sender.send(()).unwrap();
            release_receiver.recv_timeout(Duration::from_secs(15)).unwrap();
        }));
        started_receiver.recv_timeout(Duration::from_secs(10)).unwrap();
        Self { release: Some(release_sender), task: Some(task) }
    }

    fn resume(mut self) {
        self.release.take().unwrap().send(()).unwrap();
        block_on(self.task.take().unwrap());
    }
}

impl Drop for PausedWorker {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() { let _ = release.send(()); }
        if let Some(task) = self.task.take() { task.detach(); }
    }
}

#[test]
// PUB-A03/E-T06：普通/受管×派生/复制，在真实worker暂停时poll公开入口后取消
// 等待，并释放最后外部所有者。另一真实进程仍被EX排斥；原实际任务收尾后
// peer才能重获EX。顶层Future实际拥有输入并满足Send+'static，不在测试包装
// 生产完成Task或伪造资格。取消复制必须不发布、无暂存残留且源精确不变。
fn test_public_exclusive_cancelled_operations_keep_qualification_until_real_completion() {
    if isolated("test_public_exclusive_cancelled_operations_keep_qualification_until_real_completion") { return; }
    fn send_static<F: std::future::Future + Send + 'static>(future: F) -> F { future }
    let temporary = fixture();
    let root = configure(temporary.path());
    let namespace = LocalFileNamespace::new();
    for managed in [false, true] {
        for copying in [false, true] {
            let path = temporary.path().join(format!("source-{managed}-{copying}"));
            let destination = temporary.path().join(format!("destination-{managed}-{copying}"));
            std::fs::write(&path, b"cancel-original").unwrap();
            let authority = if managed {
                // SAFETY: 当前测试和真实peer均使用同一可信根/原名称和本库内容协议。
                Some(unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap())
            } else { None };
            let owner = Arc::new(match authority.as_ref() {
                Some(authority) => block_on(namespace.open_exclusive_with_cross_process_authority(authority)),
                None => block_on(namespace.open_exclusive(&path)),
            }.unwrap());
            let gate = PausedWorker::new();
            let operation_owner = Arc::clone(&owner);
            let operation_namespace = namespace.clone();
            let operation_path = path.clone();
            let operation_destination = destination.clone();
            // 调用者拥有输入的普通async move，唯一Task提交仍在真实生产入口。
            let mut operation = Box::pin(send_static(async move {
                if copying {
                    operation_namespace.copy_new_with_exclusive_owner(&operation_owner, &operation_path, &operation_destination).await.unwrap();
                } else {
                    drop(operation_namespace.open_with_exclusive_owner(&operation_owner, &operation_path, FileAccessMode::Append).await.unwrap());
                }
            }));
            assert!(block_on(futures_lite::future::poll_once(operation.as_mut())).is_none());
            drop(operation);
            drop(owner);
            spawn_peer(&root, &path, "reject-all", managed, None).finish();
            assert!(!destination.exists());
            gate.resume();
            // 最终证据是另一个进程真实取得EX，不以Rust值析构/空队列当作内核释放。
            spawn_peer(&root, &path, "wait-exclusive-success", managed, None).finish();
            assert!(!destination.exists());
            assert_eq!(std::fs::read(&path).unwrap(), b"cancel-original");
            assert!(!std::fs::read_dir(temporary.path()).unwrap().any(|entry| entry.unwrap().file_name().to_string_lossy().ends_with(".copy")));
        }
    }
    println!("PUBLIC_EXCLUSIVE_VERIFIED public-cancellation");
}
