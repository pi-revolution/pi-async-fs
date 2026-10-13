//! 新增独占能力对旧FileNamespace实现者的兼容性合同。
//!
//! 两个适配器只转发原21个方法到真实LocalFileNamespace，不实现新四方法。
//! 它们不模拟文件系统、资格或产品状态；验证默认关联类型无需新增声明，
//! 默认不支持不产生副作用，以及覆盖关联类型后F03/F04仍默认明确拒绝。

use core::future::Future;
use std::path::PathBuf;

use futures_lite::future::block_on;
use pi_async_fs::{
    AvailableSpace, CopyFailure, CopyOutcome, CopyStagingEvidence, CopyTargetEvidence,
    CreateDirectoriesFailure, CreateFailure, CrossProcessCreateSuccess,
    FileAccessMode, FileIo, FileMetadata, FileNamespace, LocalFile, LocalFileNamespace,
    RemoveFailure, RenameFailure, UnsupportedExclusiveFileOwner, WalkOptions,
};
use pi_result::ErrorKind;

// 仅旧接口转发，无任何独占接口声明。宏的可选关联类型用于两种编译兼容边界。
macro_rules! legacy_implementation {
    ($name:ident $($owner:item)?) => {
        impl FileNamespace for $name {
            type Locator = PathBuf;
            type File = LocalFile;
            $($owner)?
            fn metadata<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::Result<FileMetadata<PathBuf>>> + Send + 'a {
                self.0.metadata(locator)
            }
            fn symlink_metadata<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::Result<FileMetadata<PathBuf>>> + Send + 'a {
                self.0.symlink_metadata(locator)
            }
            fn try_exists<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::Result<bool>> + Send + 'a {
                self.0.try_exists(locator)
            }
            fn available_space<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::Result<AvailableSpace>> + Send + 'a {
                self.0.available_space(locator)
            }
            fn read_dir<'a>(&'a self, locator: PathBuf) -> impl Future<Output = pi_result::Result<Self::DirectoryStream<'a>>> + Send + 'a {
                self.0.read_dir(locator)
            }
            fn walk<'a>(&'a self, locator: PathBuf, options: WalkOptions) -> impl Future<Output = pi_result::Result<Self::WalkStream<'a>>> + Send + 'a {
                self.0.walk(locator, options)
            }
            fn create_dir<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), CreateFailure>> + Send + 'a {
                self.0.create_dir(locator)
            }
            fn create_dir_all<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), CreateDirectoriesFailure<PathBuf>>> + Send + 'a {
                self.0.create_dir_all(locator)
            }
            fn remove_file<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                self.0.remove_file(locator)
            }
            fn remove_file_uncoordinated<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                self.0.remove_file_uncoordinated(locator)
            }
            fn remove_dir<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                self.0.remove_dir(locator)
            }
            fn remove_dir_uncoordinated<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                self.0.remove_dir_uncoordinated(locator)
            }
            fn rename<'a>(&'a self, source: &'a PathBuf, destination: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RenameFailure>> + Send + 'a {
                self.0.rename(source, destination)
            }
            fn open<'a>(&'a self, locator: &'a PathBuf, access: FileAccessMode) -> impl Future<Output = pi_result::Result<Self::File>> + Send + 'a {
                self.0.open(locator, access)
            }
            fn create_new<'a>(&'a self, locator: &'a PathBuf, access: FileAccessMode) -> impl Future<Output = pi_result::RawResult<Self::File, CreateFailure>> + Send + 'a {
                self.0.create_new(locator, access)
            }
            fn copy_new<'a>(&'a self, source: &'a PathBuf, destination: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<CopyOutcome, CopyFailure>> + Send + 'a {
                self.0.copy_new(source, destination)
            }
            unsafe fn create_new_coordinated<'a>(&'a self, locator: PathBuf, access: FileAccessMode) -> impl Future<Output = pi_result::RawResult<CrossProcessCreateSuccess<Self::CrossProcessAuthority, Self::File>, CreateFailure>> + Send + 'a {
                // SAFETY: 原unsafe合同原样交由调用者满足，不新增授权或改变生命周期。
                unsafe { self.0.create_new_coordinated(locator, access) }
            }
            unsafe fn establish_cross_process_authority<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::Result<Self::CrossProcessAuthority>> + Send + 'a {
                // SAFETY: 仅转发原unsafe入口，调用者必须满足原完整跨进程合同。
                unsafe { self.0.establish_cross_process_authority(locator) }
            }
            fn open_with_cross_process_authority<'a>(&'a self, authority: &'a Self::CrossProcessAuthority, access: FileAccessMode) -> impl Future<Output = pi_result::Result<Self::File>> + Send + 'a {
                self.0.open_with_cross_process_authority(authority, access)
            }
            fn remove_file_with_cross_process_authority<'a>(&'a self, authority: &'a Self::CrossProcessAuthority) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                self.0.remove_file_with_cross_process_authority(authority)
            }
            unsafe fn resume_coordinated_file_removal<'a>(&'a self, locator: &'a PathBuf) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>> + Send + 'a {
                // SAFETY: 恢复的外部稳定性责任仍由调用者满足，转发不补全协议。
                unsafe { self.0.resume_coordinated_file_removal(locator) }
            }
        }
    };
}

struct LegacyNamespace(LocalFileNamespace);
struct LegacyWithOwnerRepresentation(LocalFileNamespace);
legacy_implementation!(LegacyNamespace);
legacy_implementation!(LegacyWithOwnerRepresentation type ExclusiveOwner = u8;);

#[test]
// PUB-A01/A04：不新增关联声明或四方法的旧实现仍可真实创建/写入/读取。
// 新默认方法均Unsupported；无值类型的F03/F04不可在安全代码中构造入参，
// 另一个自定义表示适配器只验证其默认拒绝，不声称u8是有效本地所有者。
fn test_exclusive_defaults_preserve_legacy_implementations_and_failure_evidence() {
    fn default_type<T: FileNamespace<ExclusiveOwner = UnsupportedExclusiveFileOwner>>() {}
    fn thread_traits<T: Send + Sync + 'static>() {}
    default_type::<LegacyNamespace>();
    thread_traits::<UnsupportedExclusiveFileOwner>();
    #[cfg(windows)]
    let temporary = {
        let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("exclusive-default-fixtures");
        std::fs::create_dir_all(&directory).unwrap();
        tempfile::tempdir_in(directory).unwrap()
    };
    #[cfg(not(windows))]
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    let destination = temporary.path().join("destination");
    let legacy = LegacyNamespace(LocalFileNamespace::new());
    assert_eq!(block_on(legacy.open_exclusive(&source)).unwrap_err().current_context(), &ErrorKind::Unsupported);
    assert!(!source.exists());
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
    let root = temporary.path().join("common-root");
    std::fs::create_dir(&root).unwrap();
    pi_async_fs::set_cross_process_coordination_root(pi_async_fs::CrossProcessCoordinationRoot::Custom(root.clone())).unwrap();
    let mut original = block_on(legacy.create_new(&source, FileAccessMode::Append)).unwrap();
    block_on(original.append(Vec::from(&b"legacy-data"[..]))).unwrap();
    drop(original);
    assert!(block_on(legacy.try_exists(&source)).unwrap());
    assert_eq!(std::fs::read(&source).unwrap(), b"legacy-data");
    // SAFETY: 夹具唯一访问者通过真实本库协议访问，根和目标名称均可信稳定。
    let authority = unsafe { block_on(legacy.establish_cross_process_authority(&source)) }.unwrap();
    let before = std::fs::read_dir(&root).unwrap().map(|entry| entry.unwrap().file_name()).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(block_on(legacy.open_exclusive(&source)).unwrap_err().current_context(), &ErrorKind::Unsupported);
    assert_eq!(block_on(legacy.open_exclusive_with_cross_process_authority(&authority)).unwrap_err().current_context(), &ErrorKind::Unsupported);
    let custom = LegacyWithOwnerRepresentation(LocalFileNamespace::new());
    let error = match block_on(custom.open_with_exclusive_owner(&0, &source, FileAccessMode::Read)) {
        Ok(_) => panic!("default derivation must not fabricate a resource"), Err(error) => error,
    };
    assert_eq!(error.current_context(), &ErrorKind::Unsupported);
    let failure = block_on(custom.copy_new_with_exclusive_owner(&0, &source, &destination)).unwrap_err();
    assert_eq!(failure.error().current_context(), &ErrorKind::Unsupported);
    assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
    assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
    assert!(!destination.exists());
    let after = std::fs::read_dir(&root).unwrap().map(|entry| entry.unwrap().file_name()).collect::<std::collections::BTreeSet<_>>();
    assert_eq!(after, before);
    assert_eq!(std::fs::read(&source).unwrap(), b"legacy-data");
}
