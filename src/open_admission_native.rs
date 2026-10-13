//! EXO-NAT-M01：真实根能力和原生打开资格。只向下消费已验收值/查询和原生库，
//! 不依赖local、配置、状态注册表或公开trait；同步工作须由高层有界阻塞完成者承载。
//! 正式合同/三图/测试及验收入口：.scratch/pi-async-fs-mainline/
//! EXCLUSIVE_OPEN_ADMISSION_NATIVE_LAYER_2026-10-10.md，NAT-F01~06/T01~06。

use crate::coordination_root_snapshot::CoordinationRootSnapshot;
use crate::file_identity::StableFileIdentity;
use crate::open_admission_key::OpenAdmissionKey;
use pi_result::InteropResultExt;
use std::path::PathBuf;
use std::sync::Arc;

// 只描述本模块实际可达的参数、对象和仲裁错误，沿pi_result统一分类；
// OS错误另由InteropResultExt保留原始来源，不用字符串或panic替代错误。
#[derive(Debug, pi_result::thiserror::Error)]
enum NativeAdmissionError {
    #[error("coordination root must be a non-empty absolute path")]
    AbsoluteRootRequired,
    #[error("coordination root is not a real directory")]
    UnsupportedRoot,
    #[error("identity pin does not refer to a regular file")]
    UnsupportedTarget,
    #[error("admission carrier is not a regular file")]
    UnsupportedCarrier,
    #[error("admission carrier must remain empty")]
    NonEmptyCarrier,
    #[error("file opening conflicts with an active admission")]
    LockConflict,
    #[error("native admission is unsupported on this platform")]
    UnsupportedPlatform,
}

impl pi_result::ClassifyErrorKind for NativeAdmissionError {
    fn classify_error_kind(&self) -> pi_result::ErrorKind {
        match self {
            Self::AbsoluteRootRequired => pi_result::ErrorKind::InvalidInput,
            Self::UnsupportedRoot | Self::UnsupportedTarget | Self::UnsupportedCarrier
                | Self::UnsupportedPlatform => pi_result::ErrorKind::Unsupported,
            Self::NonEmptyCarrier => pi_result::ErrorKind::Corrupted,
            Self::LockConflict => pi_result::ErrorKind::Conflict,
        }
    }
}

/// 真实目录能力负责保活，快照仅记录来源。两者不泄漏可写状态/原生句柄，
/// 也不调用全局配置；可信和文件系统支持前提必须由上层在交付前确认。
pub(crate) struct CoordinationRootAnchor {
    directory: cap_std::fs::Dir,
    snapshot: CoordinationRootSnapshot,
}

impl CoordinationRootAnchor {
    /// NAT-F01：只接受本机绝对目录，保留现有“末级不能为链接”的根边界。
    /// 规范化及打开/身份查询可能阻塞，不能直接放入异步poll热路径。
    /// 所有查询在真正拥有文件的同步调用内完成；失败自动关闭未交付资源。
    pub(crate) fn open(path: PathBuf) -> pi_result::Result<Self> {
        if !path.is_absolute() {
            return Err(NativeAdmissionError::AbsoluteRootRequired).into_classified_error();
        }
        // 只裁剪无意义的尾分隔符/`.`，保留原生前缀和全部中间分量。
        // 不能collect到PathBuf：Windows扩展路径的push会消除中间`..`。
        // Unix对link/先按目录解析，因此lstat前仍须裁剪尾部，不能直接canonicalize。
        let mut components = path.components();
        while matches!(components.clone().next_back(), Some(std::path::Component::CurDir)) {
            components.next_back();
        }
        let path = components.as_path();
        let metadata = std::fs::symlink_metadata(path).into_classified_error()?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(NativeAdmissionError::UnsupportedRoot).into_classified_error();
        }
        let physical_path = std::fs::canonicalize(path).into_classified_error()?;
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            // cap-std::Dir::from_std_file要求根句柄不带删除共享，保持目录能力稳定。
            // 这不是数据文件的独占打开，也不改变数据句柄的共享/rename合同。
            options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE);
        }
        let file = options.open(&physical_path).into_classified_error()?;
        let metadata = file.metadata().into_classified_error()?;
        if !metadata.is_dir() {
            return Err(NativeAdmissionError::UnsupportedRoot).into_classified_error();
        }
        let identity = query_open_file_identity(&file, &metadata)?;
        Ok(Self {
            directory: cap_std::fs::Dir::from_std_file(file),
            snapshot: CoordinationRootSnapshot::from_parts(physical_path, identity),
        })
    }

    /// NAT-F02：借用绑定self；快照不替代directory能力，不重新按路径打开。
    #[must_use]
    pub(crate) fn snapshot(&self) -> &CoordinationRootSnapshot {
        &self.snapshot
    }

    /// NAT-F03：唯一入口用已持有根能力相对打开确定名称，避免按旧根路径重开。
    /// 原子create-or-open且不截断、不跟随末级链接；空载体永久保留，存在不表示持锁。
    fn open_carrier(&self, identity: &StableFileIdentity) -> pi_result::Result<std::fs::File> {
        use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
        let name = carrier_name(identity);
        let mut options = cap_std::fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false)
            .follow(FollowSymlinks::No);
        let file = self.directory.open_with(name, &options)
            .into_classified_error()?.into_std();
        let metadata = file.metadata().into_classified_error()?;
        if !metadata.is_file() {
            return Err(NativeAdmissionError::UnsupportedCarrier).into_classified_error();
        }
        if metadata.len() != 0 {
            return Err(NativeAdmissionError::NonEmptyCarrier).into_classified_error();
        }
        Ok(file)
    }
}

// 两种平台的L02出口形式不同：Unix消费已取元信息，Windows短借拥有型句柄。
// 这里仅适配同一真实来源，既不复制/关闭原句柄，也不保存借用或投递任务。
fn query_open_file_identity(
    file: &std::fs::File,
    metadata: &std::fs::Metadata,
) -> pi_result::Result<StableFileIdentity> {
    #[cfg(unix)]
    { crate::file_identity_query::identity_from_metadata(metadata) }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsHandle;
        crate::file_identity_query::identity_from_handle(file.as_handle())
    }
    #[cfg(not(any(unix, windows)))]
    { Err(NativeAdmissionError::UnsupportedPlatform).into_classified_error() }
}

// 资产编码与标准Hash/诊断严格分离。完整字段及平台/版本标签不会因路径别名分裂，
// 根由directory能力隐式限定；Windows按原生16字节顺序编码，不按GUID重排。
fn carrier_name(identity: &StableFileIdentity) -> String {
    match identity {
        #[cfg(unix)]
        StableFileIdentity::Unix { device, inode } =>
            format!(".pi_async_fs-open-v1-unix-{device:016x}-{inode:016x}.lock"),
        #[cfg(windows)]
        StableFileIdentity::Windows { volume_serial_number, file_id } => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let mut name = format!(".pi_async_fs-open-v1-windows-{volume_serial_number:016x}-");
            for byte in file_id {
                name.push(HEX[(byte >> 4) as usize] as char);
                name.push(HEX[(byte & 15) as usize] as char);
            }
            name.push_str(".lock");
            name
        }
    }
}

/// 本类别是“谁能持有”，不授予数据读写权限，不改变原操作许可。
#[derive(Debug)]
pub(crate) enum OpenAdmissionKind {
    Shared,
    Exclusive,
}

impl core::fmt::Display for OpenAdmissionKind {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(match self { Self::Shared => "shared", Self::Exclusive => "exclusive" })
    }
}

/// NAT-F04~06：独立拥有锁/身份保活/根。字段顺序就是最终关闭顺序，先结束
/// 资格锁才释放防编号复用的对象保活；不提供raw/unlock/资源Clone或高层回调。
pub(crate) struct OwnedOpenAdmission {
    _carrier: std::fs::File,
    _identity_pin: std::fs::File,
    _root: Arc<CoordinationRootAnchor>,
    key: OpenAdmissionKey,
    kind: OpenAdmissionKind,
}

impl OwnedOpenAdmission {
    /// 输入所有权交给真实完成者；失败不会发布半个资格，也不删除稳定载体。
    /// fs4 try-lock只保证不等持锁者，OS工作仍可能阻塞，须在有界阻塞设施内调用。
    pub(crate) fn try_acquire(
        root: Arc<CoordinationRootAnchor>,
        identity_pin: std::fs::File,
        kind: OpenAdmissionKind,
    ) -> pi_result::Result<Self> {
        let metadata = identity_pin.metadata().into_classified_error()?;
        if !metadata.is_file() {
            return Err(NativeAdmissionError::UnsupportedTarget).into_classified_error();
        }
        let identity = query_open_file_identity(&identity_pin, &metadata)?;
        let key = OpenAdmissionKey::from_parts(root.snapshot().identity().clone(), identity);
        let carrier = root.open_carrier(key.file_identity())?;
        let locked = match &kind {
            OpenAdmissionKind::Shared => fs4::FileExt::try_lock_shared(&carrier),
            OpenAdmissionKind::Exclusive => fs4::FileExt::try_lock(&carrier),
        };
        match locked {
            Ok(()) => Ok(Self { _carrier: carrier, _identity_pin: identity_pin, _root: root, key, kind }),
            Err(fs4::TryLockError::WouldBlock) =>
                Err(NativeAdmissionError::LockConflict).into_classified_error(),
            Err(fs4::TryLockError::Error(error)) => Err(error).into_classified_error(),
        }
    }

    /// NAT-F05：只给绑定self的完整值借用，不泄漏任何同步原语/根或数据句柄。
    #[must_use]
    pub(crate) fn key(&self) -> &OpenAdmissionKey {
        &self.key
    }

    /// NAT-F06：类别是已取得资格的固定值，查询不查系统/等待/修改状态。
    #[must_use]
    pub(crate) fn is_exclusive(&self) -> bool {
        matches!(&self.kind, OpenAdmissionKind::Exclusive)
    }
}

impl core::fmt::Debug for OwnedOpenAdmission {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("OwnedOpenAdmission").field("kind", &self.kind).finish_non_exhaustive()
    }
}

impl core::fmt::Display for OwnedOpenAdmission {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "OwnedOpenAdmission(kind={})", self.kind)
    }
}

impl core::fmt::Debug for CoordinationRootAnchor {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("CoordinationRootAnchor")
            .field("snapshot", &self.snapshot).finish_non_exhaustive()
    }
}

impl core::fmt::Display for CoordinationRootAnchor {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "CoordinationRootAnchor(snapshot={})", self.snapshot)
    }
}

#[cfg(all(test, any(target_os = "linux", windows)))]
mod open_admission_native_tests {
    use super::{CoordinationRootAnchor, OpenAdmissionKind, OwnedOpenAdmission};
    use std::path::PathBuf;
    use std::sync::Arc;

    // 真实小型磁盘夹具只准备目录/文件，不实现任何生产资格或系统锁。
    // Linux用系统临时盘并由TempDir清理，Windows留项目target，非性能测试。
    fn fixture() -> tempfile::TempDir {
        #[cfg(target_os = "linux")]
        { tempfile::Builder::new().prefix("pi-async-fs-admission-").tempdir().expect("fixture") }
        #[cfg(windows)]
        {
            let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("target").join("native-admission-fixtures");
            std::fs::create_dir_all(&root).expect("fixture root");
            tempfile::Builder::new().prefix("admission-").tempdir_in(root).expect("fixture")
        }
    }

    // NAT-T01：真实根取得必须保存已取得目录的身份和绝对位置；Drop不能删目录。
    // 身份预期用真实独立句柄/L02来源准备，不手写编号冒充操作系统结果。
    #[test]
    fn test_native_admission_root_owns_real_directory_and_preserves_content() {
        let temporary = fixture();
        std::fs::write(temporary.path().join("untouched"), b"root-content").expect("prepare");
        let path = std::fs::canonicalize(temporary.path()).expect("canonical path");
        #[cfg(unix)]
        let expected_identity = crate::file_identity_query::identity_from_metadata(
            &std::fs::metadata(&path).expect("root metadata"),
        ).expect("expected identity");
        #[cfg(windows)]
        let expected_identity = {
            use std::os::windows::{fs::OpenOptionsExt, io::AsHandle};
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
            let file = std::fs::OpenOptions::new().read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS).open(&path).expect("root handle");
            crate::file_identity_query::identity_from_handle(file.as_handle()).expect("expected identity")
        };
        let root = CoordinationRootAnchor::open(path.clone()).expect("acquire root");
        assert_eq!(root.snapshot().path(), path);
        assert_eq!(root.snapshot().identity(), &expected_identity);
        for spelling in [path.join(""), path.join(".")] {
            let equivalent = CoordinationRootAnchor::open(spelling).expect("real directory with trailing components");
            assert_eq!(equivalent.snapshot().identity(), &expected_identity);
            assert_eq!(equivalent.snapshot().path(), path);
        }
        assert_eq!(format!("{root}"), format!("CoordinationRootAnchor(snapshot={})", root.snapshot()));
        assert_eq!(std::fs::read_dir(&path).expect("entries").count(), 1);
        drop(root);
        assert!(path.is_dir());
        assert_eq!(std::fs::read(path.join("untouched")).expect("unchanged"), b"root-content");
    }

    // NAT-T02：同一固定载体上的真实SH/EX三种时序，逐项验证仍持有者与最后释放。
    // 文件分别独立open，只作身份保活；资格不能改变原数据或在线删除载体。
    #[test]
    fn test_native_admission_shared_exclusive_conflicts_and_last_drop() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"original-content").expect("prepare target");
        let root = Arc::new(CoordinationRootAnchor::open(
            std::fs::canonicalize(temporary.path()).expect("root path"),
        ).expect("root"));
        let acquire = |kind| OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).expect("independent pin"), kind,
        );
        let first = acquire(OpenAdmissionKind::Shared).expect("first shared");
        let second = acquire(OpenAdmissionKind::Shared).expect("second shared");
        assert!(!first.is_exclusive());
        assert_eq!(first.key(), second.key());
        assert_eq!(first.key().root_identity(), root.snapshot().identity());
        assert_eq!(format!("{first}"), "OwnedOpenAdmission(kind=shared)");
        assert_eq!(format!("{first:?}"), "OwnedOpenAdmission { kind: Shared, .. }");
        assert_eq!(acquire(OpenAdmissionKind::Exclusive).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(first);
        assert_eq!(acquire(OpenAdmissionKind::Exclusive).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(second);
        let exclusive = acquire(OpenAdmissionKind::Exclusive).expect("exclusive after last shared");
        assert!(exclusive.is_exclusive());
        assert_eq!(format!("{exclusive}"), "OwnedOpenAdmission(kind=exclusive)");
        for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
            assert_eq!(acquire(kind).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        }
        drop(exclusive);
        let last = acquire(OpenAdmissionKind::Shared).expect("shared after exclusive drop");
        drop(last);
        assert_eq!(std::fs::read(&path).expect("content"), b"original-content");
        let carriers = std::fs::read_dir(temporary.path()).expect("root entries")
            .map(|entry| entry.expect("entry").path())
            .filter(|path| path.file_name().unwrap().to_string_lossy().starts_with(".pi_async_fs-open-v1-"))
            .collect::<Vec<_>>();
        assert_eq!(carriers.len(), 1);
        assert_eq!(std::fs::metadata(&carriers[0]).expect("retained carrier").len(), 0);
    }

    // NAT-T01：非法根必须在建立载体前失败，缺失与类型错误保持不同分类。
    // 同时通过自动trait和诊断检查只读根对象，不导出其原生目录句柄。
    #[test]
    fn test_native_admission_root_rejects_invalid_sources_without_assets() {
        fn traits<T: Send + Sync + 'static>() {}
        traits::<CoordinationRootAnchor>();
        traits::<OwnedOpenAdmission>();
        let temporary = fixture();
        let file = temporary.path().join("not-directory");
        std::fs::write(&file, b"unchanged").expect("prepare file");
        for path in [PathBuf::new(), PathBuf::from("relative"), PathBuf::from(".")] {
            assert_eq!(CoordinationRootAnchor::open(path).unwrap_err().current_context(), &pi_result::ErrorKind::InvalidInput);
        }
        assert_eq!(CoordinationRootAnchor::open(file.clone()).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        assert_eq!(CoordinationRootAnchor::open(temporary.path().join("missing")).unwrap_err().current_context(), &pi_result::ErrorKind::NotFound);
        assert_eq!(std::fs::read_dir(temporary.path()).expect("entries").count(), 1);
        assert_eq!(std::fs::read(&file).expect("unchanged file"), b"unchanged");
    }

    // NAT-T03：实际硬链接/改名仍被同一EX锁排斥，同名新对象则能独立取得资格。
    // 旧身份由真实句柄保活，不依赖手写代次/哈希或测试资格表。
    #[test]
    fn test_native_admission_hard_links_rename_and_recreated_name_follow_objects() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let alias_directory = temporary.path().join("another");
        std::fs::create_dir(&alias_directory).expect("alias directory");
        let alias = alias_directory.join("alias.bin");
        let renamed = temporary.path().join("renamed.bin");
        std::fs::write(&path, b"old-object").expect("prepare");
        std::fs::hard_link(&path, &alias).expect("real hard link");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("path")).expect("root"));
        let acquire = |path: &std::path::Path| OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(path).expect("independent pin"), OpenAdmissionKind::Exclusive,
        );
        let old = acquire(&path).expect("original exclusive");
        assert_eq!(acquire(&alias).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        std::fs::rename(&path, &renamed).expect("rename original");
        assert_eq!(acquire(&renamed).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        std::fs::write(&path, b"new-object").expect("same-name new object");
        let new = acquire(&path).expect("distinct new object");
        assert_ne!(old.key(), new.key());
        assert_eq!(old.key().root_identity(), new.key().root_identity());
        drop(old);
        let alias_owner = acquire(&alias).expect("old object available after last drop");
        assert_ne!(alias_owner.key(), new.key());
        assert_eq!(std::fs::read(&renamed).expect("old bytes"), b"old-object");
        assert_eq!(std::fs::read(&path).expect("new bytes"), b"new-object");
    }

    // NAT-T04：独立字面量只验证固定资产编码，不将手写身份当真实资格。
    // 真实载体非空后必须失败且原字节保留；停用所有资格后的修复允许后续重试。
    #[test]
    fn test_native_admission_carrier_encoding_non_truncation_and_retry() {
        #[cfg(unix)]
        let identities = [
            (crate::file_identity::StableFileIdentity::Unix { device: 0, inode: 0 }, ".pi_async_fs-open-v1-unix-0000000000000000-0000000000000000.lock"),
            (crate::file_identity::StableFileIdentity::Unix { device: u64::MAX, inode: u64::MAX }, ".pi_async_fs-open-v1-unix-ffffffffffffffff-ffffffffffffffff.lock"),
            (crate::file_identity::StableFileIdentity::Unix { device: 0x0123456789abcdef, inode: 0xfedcba9876543210 }, ".pi_async_fs-open-v1-unix-0123456789abcdef-fedcba9876543210.lock"),
        ];
        #[cfg(windows)]
        let identities = [
            (crate::file_identity::StableFileIdentity::Windows { volume_serial_number: 0, file_id: [0; 16] }, ".pi_async_fs-open-v1-windows-0000000000000000-00000000000000000000000000000000.lock"),
            (crate::file_identity::StableFileIdentity::Windows { volume_serial_number: u64::MAX, file_id: [255; 16] }, ".pi_async_fs-open-v1-windows-ffffffffffffffff-ffffffffffffffffffffffffffffffff.lock"),
            (crate::file_identity::StableFileIdentity::Windows { volume_serial_number: 0x0123456789abcdef, file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] }, ".pi_async_fs-open-v1-windows-0123456789abcdef-000102030405060708090a0b0c0d0e0f.lock"),
        ];
        for (identity, expected) in identities {
            assert_eq!(super::carrier_name(&identity), expected);
        }

        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"data").expect("prepare");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("path")).expect("root"));
        let acquire = || OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive);
        drop(acquire().expect("initial qualification"));
        let carriers = std::fs::read_dir(temporary.path()).expect("entries")
            .map(|entry| entry.expect("entry").path()).filter(|entry| entry != &path).collect::<Vec<_>>();
        assert_eq!(carriers.len(), 1);
        let carrier = &carriers[0];
        std::fs::write(carrier, b"sentinel-do-not-truncate").expect("offline corrupt carrier");
        assert_eq!(acquire().unwrap_err().current_context(), &pi_result::ErrorKind::Corrupted);
        assert_eq!(std::fs::read(carrier).expect("preserved carrier"), b"sentinel-do-not-truncate");
        assert_eq!(std::fs::read(&path).expect("preserved target"), b"data");
        std::fs::write(carrier, b"").expect("offline same-object repair");
        drop(acquire().expect("retry succeeds without cached failure"));
        assert!(carrier.is_file());
        assert_eq!(std::fs::metadata(carrier).expect("empty").len(), 0);
    }

    // NAT-T05：8次独立EX申请同时开始，胜者一直持有到全部尝试结束。
    // 只运行一轮小规模功能竞争，精确断言1成功/7冲突/0其它错误，非压力测试。
    #[test]
    fn test_native_admission_simultaneous_exclusive_has_exactly_one_winner() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"winner-content").expect("prepare");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("path")).expect("root"));
        let pins = (0..8).map(|_| std::fs::File::open(&path).expect("independent pin")).collect::<Vec<_>>();
        let start = std::sync::Barrier::new(8);
        let finished = std::sync::Barrier::new(8);
        let results = std::thread::scope(|scope| {
            let tasks = pins.into_iter().map(|pin| {
                let root = Arc::clone(&root);
                let start = &start;
                let finished = &finished;
                scope.spawn(move || {
                    start.wait();
                    let result = OwnedOpenAdmission::try_acquire(root, pin, OpenAdmissionKind::Exclusive);
                    finished.wait();
                    result
                })
            }).collect::<Vec<_>>();
            tasks.into_iter().map(|task| task.join().expect("thread")).collect::<Vec<_>>()
        });
        let successes = results.iter().filter(|result| result.is_ok()).count();
        let conflicts = results.iter().filter(|result| result.as_ref().err().is_some_and(|error| error.current_context() == &pi_result::ErrorKind::Conflict)).count();
        assert_eq!((successes, conflicts, results.len() - successes - conflicts), (1, 7, 0));
        drop(results);
        drop(OwnedOpenAdmission::try_acquire(root, std::fs::File::open(&path).expect("new pin"), OpenAdmissionKind::Exclusive).expect("released winner"));
        assert_eq!(std::fs::read(path).expect("unchanged"), b"winner-content");
    }

    // NAT-T06：独立载体锁不强制锁住数据文件，这是有意的原生机制对照。
    // mmap本身由真实memmap2完成；本测试只证原生机制，不代替已装配公开owner的组合测试。
    #[test]
    fn test_native_admission_carrier_lock_does_not_restrict_data_io_or_mmap() {
        use std::io::Write;
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"abcd").expect("prepare");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("path")).expect("root"));
        let owner = OwnedOpenAdmission::try_acquire(root, std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive).expect("exclusive carrier");
        assert_eq!(std::fs::read(&path).expect("uncooperative native read"), b"abcd");
        let source = std::fs::File::open(&path).expect("mmap read source");
        // SAFETY: 本测试唯一维护目标，映射原4字节永不被截断/外部修改，追加只在EOF。
        // 源文件和整个临时目录一直保活，映射不逃逸测试，实际访问都在映射寿命内。
        let read_map = unsafe { memmap2::MmapOptions::new().len(4).map(&source) }.expect("read mmap");
        let mut append = std::fs::OpenOptions::new().append(true).open(&path).expect("independent append");
        append.write_all(b"-tail").expect("append while mapped");
        append.sync_all().expect("file flush while read mapped");
        assert_eq!(read_map.as_ref(), b"abcd");
        drop(read_map);
        let writable = std::fs::OpenOptions::new().read(true).write(true).open(&path).expect("writable source");
        // SAFETY: 只映射原4字节，唯一可写映射，无其它借用/同范围写者；追加仍在EOF。
        // 不截断、不重建，writable文件与目录保持至解映射完成。
        let mut write_map = unsafe { memmap2::MmapOptions::new().len(4).map_mut(&writable) }.expect("write mmap");
        write_map[..2].copy_from_slice(b"XY");
        write_map.flush().expect("mapped flush");
        append.write_all(b"-end").expect("append with write map");
        append.sync_all().expect("file flush with write map");
        assert_eq!(&write_map[..], b"XYcd");
        drop(write_map);
        assert_eq!(std::fs::read(&path).expect("final bytes"), b"XYcd-tail-end");
        drop(owner);
    }

    // NAT-T05：同一生产入口在真实独立进程内重新打开根/目标并参与系统锁。
    // 测试通过Command环境传参，不修改父进程全局状态；每个子进程只运行本用例。
    // 父进程有15秒单次看门狗，超时杀掉并收取输出，不能将超时当成功。
    #[test]
    fn test_native_admission_cross_process_shared_exclusive_and_release() {
        const ROOT: &str = "PI_ASYNC_FS_NATIVE_ADMISSION_CHILD_ROOT";
        const TARGET: &str = "PI_ASYNC_FS_NATIVE_ADMISSION_CHILD_TARGET";
        const KIND: &str = "PI_ASYNC_FS_NATIVE_ADMISSION_CHILD_KIND";
        const EXPECT: &str = "PI_ASYNC_FS_NATIVE_ADMISSION_CHILD_EXPECT";
        if let Some(path) = std::env::var_os(ROOT) {
            let root = Arc::new(CoordinationRootAnchor::open(PathBuf::from(path)).expect("child root"));
            let target = std::env::var_os(TARGET).expect("child target");
            let kind = match std::env::var(KIND).expect("child kind").as_str() {
                "shared" => OpenAdmissionKind::Shared,
                "exclusive" => OpenAdmissionKind::Exclusive,
                other => panic!("unknown child kind: {other}"),
            };
            let result = OwnedOpenAdmission::try_acquire(root, std::fs::File::open(target).expect("child pin"), kind);
            match std::env::var(EXPECT).expect("child expected").as_str() {
                "ok" => drop(result.expect("child qualification")),
                "conflict" => assert_eq!(result.unwrap_err().current_context(), &pi_result::ErrorKind::Conflict),
                other => panic!("unknown child expectation: {other}"),
            }
            println!("NAT_CHILD_VERIFIED");
            return;
        }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"cross-process-content").expect("prepare");
        let root_path = std::fs::canonicalize(temporary.path()).expect("root path");
        let root = Arc::new(CoordinationRootAnchor::open(root_path.clone()).expect("root"));
        let run_child = |kind, expected| {
            use std::process::{Command, Stdio};
            use std::time::{Duration, Instant};
            let mut child = Command::new(std::env::current_exe().expect("test executable"))
                .args(["--exact", "open_admission_native::open_admission_native_tests::test_native_admission_cross_process_shared_exclusive_and_release", "--nocapture", "--test-threads=1"])
                .env(ROOT, &root_path).env(TARGET, &path).env(KIND, kind).env(EXPECT, expected)
                .stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().expect("start child");
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut timed_out = false;
            loop {
                if child.try_wait().expect("poll child").is_some() { break; }
                if Instant::now() >= deadline {
                    timed_out = true;
                    child.kill().expect("kill timed out child");
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let output = child.wait_with_output().expect("collect child");
            let stdout = String::from_utf8(output.stdout).expect("child stdout utf8");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(!timed_out && output.status.success(), "child kind={kind} expected={expected}: {stdout}\n{stderr}");
            assert_eq!(stdout.matches("NAT_CHILD_VERIFIED").count(), 1);
            assert!(stdout.contains("running 1 test") && stdout.contains("1 passed; 0 failed"), "{stdout}");
        };
        let acquire = |kind| OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("parent pin"), kind).expect("parent qualification");
        let shared = acquire(OpenAdmissionKind::Shared);
        run_child("shared", "ok");
        run_child("exclusive", "conflict");
        drop(shared);
        run_child("exclusive", "ok");
        let exclusive = acquire(OpenAdmissionKind::Exclusive);
        run_child("shared", "conflict");
        run_child("exclusive", "conflict");
        drop(exclusive);
        run_child("shared", "ok");
        run_child("exclusive", "ok");
        assert_eq!(std::fs::read(&path).expect("unchanged target"), b"cross-process-content");
    }

    // NAT-T03：不同物理根不属于共同锁域，键必须明确分离，不谎称跨根协调。
    // 这是原生接缝，不提供绕过上层唯一全局Custom设置的公开能力。
    #[test]
    fn test_native_admission_distinct_physical_roots_are_distinct_domains() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"data").expect("prepare");
        let first_root = temporary.path().join("root-a");
        let second_root = temporary.path().join("root-b");
        std::fs::create_dir(&first_root).expect("first root");
        std::fs::create_dir(&second_root).expect("second root");
        let acquire = |root| OwnedOpenAdmission::try_acquire(
            Arc::new(CoordinationRootAnchor::open(root).expect("root")),
            std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive,
        ).expect("distinct domain qualification");
        let first = acquire(std::fs::canonicalize(&first_root).expect("absolute first root"));
        let second = acquire(std::fs::canonicalize(&second_root).expect("absolute second root"));
        assert_eq!(first.key().file_identity(), second.key().file_identity());
        assert_ne!(first.key().root_identity(), second.key().root_identity());
        assert_ne!(first.key(), second.key());
        assert_eq!(std::fs::read_dir(&first_root).expect("first carrier").count(), 1);
        assert_eq!(std::fs::read_dir(&second_root).expect("second carrier").count(), 1);
    }

    // NAT-T01/T05：真实资源移动线程与共享借用，末尾资格Drop后锁和根引用释放。
    // 只记录当前目标的结构体大小/对齐，不将其当堆总量/RSS或性能基准。
    #[test]
    fn test_native_admission_thread_transfer_last_root_owner_and_layout() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"thread-content").expect("prepare");
        let root_path = std::fs::canonicalize(temporary.path()).expect("root path");
        let root = Arc::new(CoordinationRootAnchor::open(root_path.clone()).expect("root"));
        let weak = Arc::downgrade(&root);
        let owner = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive).expect("exclusive");
        drop(root);
        assert!(weak.upgrade().is_some());
        let owner = std::thread::spawn(move || owner).join().expect("owned thread");
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| assert!(owner.is_exclusive()));
            reader.join().expect("shared reader");
        });
        drop(owner);
        assert!(weak.upgrade().is_none());
        drop(OwnedOpenAdmission::try_acquire(
            Arc::new(CoordinationRootAnchor::open(root_path).expect("reopen root")),
            std::fs::File::open(&path).expect("new pin"), OpenAdmissionKind::Shared,
        ).expect("last owner released lock"));
        println!("NAT_LAYOUT root_size={} root_align={} admission_size={} admission_align={} key_size={} key_align={}",
            std::mem::size_of::<CoordinationRootAnchor>(), std::mem::align_of::<CoordinationRootAnchor>(),
            std::mem::size_of::<OwnedOpenAdmission>(), std::mem::align_of::<OwnedOpenAdmission>(),
            std::mem::size_of::<crate::open_admission_key::OpenAdmissionKey>(), std::mem::align_of::<crate::open_admission_key::OpenAdmissionKey>());
        assert_eq!(std::fs::read(path).expect("unchanged"), b"thread-content");
    }

    #[cfg(unix)]
    // NAT-T01/T04：真实链接不得变成根/载体旁路；人为破坏只在全部资格停用后。
    // 文件/链接字节都独立观察，生产不得负责在线删除或“修复”载体。
    #[test]
    fn test_native_admission_rejects_root_and_carrier_symlinks_without_touching_target() {
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_link = temporary.path().join("root-link");
        std::fs::write(&path, b"must-not-change").expect("prepare");
        std::os::unix::fs::symlink(temporary.path(), &root_link).expect("root symlink");
        for spelling in [root_link.clone(), root_link.join(""), root_link.join(".")] {
            assert_eq!(CoordinationRootAnchor::open(spelling.clone()).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported, "{spelling:?}");
        }
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("path")).expect("root"));
        let acquire = || OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive);
        let owner = acquire().expect("initial qualification");
        let carrier = temporary.path().join(super::carrier_name(owner.key().file_identity()));
        drop(owner);
        let saved = temporary.path().join("offline-saved-carrier");
        std::fs::rename(&carrier, &saved).expect("offline move");
        std::os::unix::fs::symlink(&path, &carrier).expect("carrier symlink");
        assert!(acquire().is_err());
        assert!(std::fs::symlink_metadata(&carrier).expect("link").file_type().is_symlink());
        assert_eq!(std::fs::read(&path).expect("target preserved"), b"must-not-change");
        std::fs::remove_file(&carrier).expect("offline unlink symlink only");
        std::fs::rename(&saved, &carrier).expect("offline restore same carrier");
        drop(acquire().expect("repaired asset retry"));
        assert!(std::fs::symlink_metadata(root_link).expect("root link unchanged").file_type().is_symlink());
    }

    // NAT-T04：真实目录可以作为系统句柄输入，但不能被授予普通文件资格。
    // 所有类别均须在创建任何载体前拒绝，不能通过文件资源的类型系统猜合法性。
    #[test]
    fn test_native_admission_rejects_directory_identity_pin_before_carrier_creation() {
        let temporary = fixture();
        let path = std::fs::canonicalize(temporary.path()).expect("root path");
        let root = Arc::new(CoordinationRootAnchor::open(path.clone()).expect("root"));
        for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
                options.custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
            }
            let pin = options.open(&path).expect("directory pin");
            assert_eq!(OwnedOpenAdmission::try_acquire(Arc::clone(&root), pin, kind).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        }
        assert_eq!(std::fs::read_dir(path).expect("no assets").count(), 0);
    }

    // NAT-T01：本机路径能表达NUL，不表示OS接受；必须分类失败且无载体副作用。
    #[test]
    fn test_native_admission_root_rejects_embedded_nul_without_assets() {
        let temporary = fixture();
        let invalid = std::fs::canonicalize(temporary.path()).expect("absolute root").join("bad\0path");
        assert_eq!(CoordinationRootAnchor::open(invalid).unwrap_err().current_context(), &pi_result::ErrorKind::InvalidInput);
        assert_eq!(std::fs::read_dir(temporary.path()).expect("no assets").count(), 0);
    }

    #[cfg(windows)]
    // NAT-F01/NAT-I02：直接拼接原生字符串，防止夹具本身先由PathBuf::push
    // 消除verbatim路径的`..`。缺失中间目录不能被跳过而错误授予真实根能力。
    #[test]
    fn test_native_admission_verbatim_root_preserves_missing_parent_components() {
        use pi_result::InteropResultExt;
        let temporary = fixture();
        let root = std::fs::canonicalize(temporary.path()).expect("verbatim root");
        let mut raw = root.as_os_str().to_os_string();
        raw.push("\\never-created\\..");
        let invalid = PathBuf::from(raw);
        let expected = std::fs::symlink_metadata(&invalid).into_classified_error().unwrap_err();
        assert_eq!(CoordinationRootAnchor::open(invalid).unwrap_err().current_context(), expected.current_context());
        assert_eq!(std::fs::read_dir(root).expect("no assets").count(), 0);
    }

    #[cfg(windows)]
    // NAT-I03：真实目录能力必须满足cap-std的Windows“无删除共享”前提。
    // 仅根在保活期不能被移动，数据文件的默认rename合同不由此测试修改。
    #[test]
    fn test_native_admission_windows_root_excludes_delete_sharing_until_drop() {
        let temporary = fixture();
        let path = temporary.path().join("root");
        let moved = temporary.path().join("moved-root");
        std::fs::create_dir(&path).expect("real root");
        std::fs::write(path.join("sentinel"), b"root-content").expect("root sentinel");
        let root = CoordinationRootAnchor::open(std::fs::canonicalize(&path).expect("absolute root")).expect("root capability");
        let error = std::fs::rename(&path, &moved).expect_err("root capability must exclude delete sharing");
        assert_eq!(error.raw_os_error(), Some(windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION as i32));
        assert!(!moved.exists());
        assert_eq!(std::fs::read(path.join("sentinel")).expect("original root preserved"), b"root-content");
        drop(root);
        std::fs::rename(&path, &moved).expect("last root owner released native sharing restriction");
        assert!(!path.exists());
        assert_eq!(std::fs::read(moved.join("sentinel")).expect("same directory content after drop"), b"root-content");
    }

    // NAT-T04：载体名被真实目录占用时，原生打开错误必须保留，而不是改成
    // “已打开后才发现类型错误”。目录内容/对象身份和数据文件均不得被修改。
    #[test]
    fn test_native_admission_directory_carrier_preserves_native_error_and_contents() {
        use pi_result::InteropResultExt;
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"data").expect("prepare target");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("absolute root")).expect("root"));
        let owner = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive).expect("initial qualification");
        let carrier = temporary.path().join(super::carrier_name(owner.key().file_identity()));
        drop(owner);
        let saved = temporary.path().join("offline-saved-carrier");
        std::fs::rename(&carrier, &saved).expect("offline move only after all qualifications ended");
        std::fs::create_dir(&carrier).expect("directory at carrier name");
        std::fs::write(carrier.join("sentinel"), b"directory-content").expect("directory sentinel");
        let expected = std::fs::OpenOptions::new().read(true).write(true).open(&carrier)
            .into_classified_error().unwrap_err();
        for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
            let error = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), kind).unwrap_err();
            assert_eq!(error.current_context(), expected.current_context());
        }
        assert_eq!(std::fs::read(carrier.join("sentinel")).expect("preserved directory"), b"directory-content");
        assert_eq!(std::fs::read(&path).expect("preserved target"), b"data");
        assert_eq!(std::fs::metadata(saved).expect("original carrier retained offline").len(), 0);
    }

    #[cfg(target_os = "linux")]
    // NAT-T04：真实FIFO能够被Linux读写打开，但不是普通锁载体。
    // 这里只调用系统准备夹具，资格判断仍完全由生产入口执行。
    #[test]
    fn test_native_admission_non_regular_fifo_carrier_is_rejected_without_replacement() {
        use std::os::unix::{ffi::OsStrExt, fs::FileTypeExt};
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"data").expect("prepare target");
        let root = Arc::new(CoordinationRootAnchor::open(std::fs::canonicalize(temporary.path()).expect("absolute root")).expect("root"));
        let owner = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), OpenAdmissionKind::Exclusive).expect("initial qualification");
        let carrier = temporary.path().join(super::carrier_name(owner.key().file_identity()));
        drop(owner);
        let saved = temporary.path().join("offline-saved-carrier");
        std::fs::rename(&carrier, &saved).expect("offline move only after all qualifications ended");
        let native_path = std::ffi::CString::new(carrier.as_os_str().as_bytes()).expect("nul-free fixture path");
        // SAFETY: CString在调用期间保活且以NUL结尾，指针只读；路径属于本测试
        // 独占的真实临时目录，所有资格均已结束，不覆盖或删除在线锁资产。
        assert_eq!(unsafe { libc::mkfifo(native_path.as_ptr(), 0o600) }, 0);
        for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
            assert_eq!(OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).expect("pin"), kind).unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        }
        assert!(std::fs::symlink_metadata(carrier).expect("preserved fifo").file_type().is_fifo());
        assert_eq!(std::fs::read(&path).expect("preserved target"), b"data");
        assert_eq!(std::fs::metadata(saved).expect("original carrier retained offline").len(), 0);
    }
}
