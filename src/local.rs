//! 本地文件系统适配器的私有实现模块。
//!
//! 本模块把公开的 [`crate::FileNamespace`] 与 [`crate::FileIo`] 接口落到本地
//! 文件系统，并负责把命名空间操作、文件内容操作、内存映射以及合作式跨进程
//! 操作收敛到同一套可验证的资源生命周期。公开调用者只依赖 crate 根部接口；
//! 本模块中的注册表、租约、协议记录和平台调用均不是可绕过的外部扩展点。
//!
//! 模块内依赖自底向上为：平台身份与原子文件原语 → 进程内文件／目录条目协调
//! → 可选跨进程协议 → 拥有型完成任务 → `LocalFile` 与
//! `LocalFileNamespace` 适配器。下层不得调用公开适配器入口，资源释放统一通过
//! 拥有型 guard／租约的析构路径闭环。
//!
//! 关键不变量、状态转换与失败恢复见
//! `docs/BUG_ISSUES_CONTEXT.md#issue-b2h-doc-001` 及
//! `.scratch/pi-async-fs-mainline/BATCH_ONE_INTERNAL_IMPLEMENTATION_DESIGN.md`。
//! 对外可观察语义由根部 trait Rustdoc 与 `tests/local_*.rs` 的真实集成测试
//! 共同验证；内部说明不得被解释为额外的公开保证。

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::{DefaultHasher, RandomState};
use std::fs::{
    DirEntry as NativeDirEntry, Metadata, ReadDir as NativeReadDir,
};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{Read as _, SeekFrom, Write as _};
use std::num::NonZeroUsize;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{
    AtomicBool, AtomicU64, Ordering as AtomicOrdering,
};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
#[cfg(unix)]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_lite::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use futures_core::Stream;
use pi_result::InteropResultExt;
use sha2::{Digest as _, Sha256};

use crate::bounded_blocking;
use crate::{
    AvailableSpace, BufferFailure, CopyFailure, CopyOutcome,
    CopyStagingEvidence, CopyTargetEvidence,
    CreateDirectoriesFailure, CreateFailure, CrossProcessCreateSuccess,
    DetachableWriteBuffer, DirectoryEntry, EntryName, FileAccessMode,
    FileFlushMode, FileIo, FileMetadata, FileNamespace, FileTimes, FileType,
    GrowableReadBuffer, MmapRange, OverwriteFailure, PortablePermissions,
    ReadGrowthLimit, OverwriteTargetEvidence, ReadGrowthOutcome,
    ReadMmapHandle, ReadTargetRegion, ReadWriteMmapHandle, RemoveFailure,
    RenameFailure, WalkEntry, WalkOptions,
};

const FILE_REGISTRY_SHARDS: usize = 64;
const NAMESPACE_ENTRY_REGISTRY_SHARDS: usize = 64;
const READ_TRANSFER_CHUNK_BYTES: usize = 64 * 1024;

// `async_executor::Task` 在 Drop 时会取消任务；公开 I/O Future 被取消时，
// 已提交的拥有型文件任务反而必须继续排空。这个包装在正常轮询时转发结果，
// 未完成即被释放时则把任务分离到内部执行器继续运行。
struct DetachOnDrop<T> {
    task: Option<async_global_executor::Task<T>>,
}

// 复制公开 Future 的取消不能取消已经提交的底层任务，但必须阻止尚未提交的
// 原子发布。这个包装在 Drop 时只设置无阻塞标志并分离拥有型任务；后台任务
// 在传输边界和发布提交前读取该标志，并自行清理稳定识别的暂存对象。
struct CancelCopyOnDrop<T> {
    task: Option<async_global_executor::Task<T>>,
    cancelled: Arc<AtomicBool>,
}

impl<T> CancelCopyOnDrop<T> {
    fn new(
        task: async_global_executor::Task<T>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            task: Some(task),
            cancelled,
        }
    }
}

impl<T> Unpin for CancelCopyOnDrop<T> {}

impl<T> Future for CancelCopyOnDrop<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let result = Pin::new(
            this.task.as_mut().expect("owned copy task must exist while polling"),
        )
        .poll(context);
        if result.is_ready() {
            this.task.take();
        }
        result
    }
}

impl<T> Drop for CancelCopyOnDrop<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.cancelled.store(true, AtomicOrdering::Release);
            task.detach();
        }
    }
}

impl<T> DetachOnDrop<T> {
    fn new(task: async_global_executor::Task<T>) -> Self {
        Self { task: Some(task) }
    }
}

impl<T> Unpin for DetachOnDrop<T> {}

impl<T> Future for DetachOnDrop<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let result = Pin::new(
            this.task.as_mut().expect("owned task must exist while polling"),
        )
        .poll(context);
        if result.is_ready() {
            this.task.take();
        }
        result
    }
}

impl<T> Drop for DetachOnDrop<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

type LocalDirectoryStep = Pin<
    Box<
        dyn Future<
                Output = core::result::Result<
                    (
                        NativeReadDir,
                        Option<std::io::Result<NativeDirEntry>>,
                    ),
                    Box<(pi_result::Error, NativeReadDir)>,
                >,
            > + Send
            + 'static,
    >,
>;

struct LocalDirectoryStream {
    entries: Option<NativeReadDir>,
    pending_step: Option<LocalDirectoryStep>,
    terminated: bool,
}

// 递归遍历必须同时握住目录能力句柄、由该句柄建立的枚举游标和对外使用的
// 逻辑定位符。子目录只能相对这个已打开句柄做无跟随打开，不能在 yield 后
// 重新解析先前保存的完整路径，否则目录到符号链接的并发替换会越出遍历根。
// 平台证明和升级触发条件见问题归档 `B2H-DEF-011` 所链接的专题研究。
struct LocalWalkDirectory {
    directory: cap_std::fs::Dir,
    entries: cap_std::fs::ReadDir,
    locator: PathBuf,
}

impl LocalDirectoryStream {
    fn new(entries: NativeReadDir) -> Self {
        Self {
            entries: Some(entries),
            pending_step: None,
            terminated: false,
        }
    }
}

impl Stream for LocalDirectoryStream {
    type Item = pi_result::Result<DirectoryEntry<PathBuf>>;

    fn poll_next(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }

        if this.pending_step.is_none() {
            let entries = this.entries.take().expect(
                "an active directory stream must own its iterator while idle",
            );
            this.pending_step = Some(Box::pin(async move {
                bounded_blocking::unblock_with_input(
                    entries,
                    |mut entries| {
                        let next = entries.next();
                        (entries, next)
                    },
                )
                .await
                .map_err(Box::new)
            }));
        }

        match this.pending_step
            .as_mut()
            .expect("an active directory stream must have a pending step")
            .as_mut()
            .poll(context)
        {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                this.pending_step.take();
                let (entries, next) = match result {
                    Ok(completed) => completed,
                    Err(failure) => {
                        let (error, _entries) = *failure;
                        this.terminated = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                };
                this.entries = Some(entries);
                let Some(next) = next else {
                    this.terminated = true;
                    return Poll::Ready(None);
                };
                let entry = match next {
                    Ok(entry) => entry,
                    Err(error) => {
                        this.terminated = true;
                        let converted = core::result::Result::<
                            DirectoryEntry<PathBuf>,
                            _,
                        >::Err(error)
                        .into_classified_error();
                        return Poll::Ready(Some(converted));
                    }
                };
                let name = EntryName::from(entry.file_name());
                let locator = entry.path();
                Poll::Ready(Some(Ok(DirectoryEntry::new(
                    name,
                    locator,
                    None,
                ))))
            }
        }
    }
}

async fn open_local_directory(
    locator: PathBuf,
) -> pi_result::Result<NativeReadDir> {
    bounded_blocking::unblock(move || std::fs::read_dir(locator))
        .await?
        .into_classified_error()
}

async fn open_local_walk_root(
    locator: PathBuf,
) -> pi_result::Result<LocalWalkDirectory> {
    bounded_blocking::unblock(move || {
        let directory = cap_std::fs::Dir::open_ambient_dir(
            &locator,
            cap_std::ambient_authority(),
        )?;
        let entries = directory.entries()?;
        Ok::<_, std::io::Error>(LocalWalkDirectory {
            directory,
            entries,
            locator,
        })
    })
    .await?
    .into_classified_error()
}

async fn next_local_walk_entry(
    directory: LocalWalkDirectory,
) -> core::result::Result<
    (
        LocalWalkDirectory,
        Option<std::io::Result<(OsString, cap_std::fs::FileType)>>,
    ),
    Box<(pi_result::Error, LocalWalkDirectory)>,
> {
    bounded_blocking::unblock_with_input(directory, |mut directory| {
        let next = directory.entries.next().map(|entry| {
            entry.and_then(|entry| {
                let file_type = entry.file_type()?;
                Ok((entry.file_name(), file_type))
            })
        });
        (directory, next)
    })
    .await
    .map_err(Box::new)
}

async fn open_local_walk_child(
    parent: LocalWalkDirectory,
    child_name: PathBuf,
    child_locator: PathBuf,
) -> core::result::Result<
    (LocalWalkDirectory, std::io::Result<LocalWalkDirectory>),
    Box<(pi_result::Error, LocalWalkDirectory)>,
> {
    bounded_blocking::unblock_with_input(
        (parent, child_name, child_locator),
        |(parent, child_name, child_locator)| {
            let child = (|| {
                // `open_dir_nofollow` 把最终分量检查与实际打开的目录句柄绑定；
                // 不能用 `symlink_metadata` 加普通 open 代替，否则两次调用之间
                // 仍存在可被目录／链接替换利用的检查后使用竞态。
                let directory = cap_fs_ext::DirExt::open_dir_nofollow(
                    &parent.directory,
                    &child_name,
                )?;
                let entries = directory.entries()?;
                Ok(LocalWalkDirectory {
                    directory,
                    entries,
                    locator: child_locator,
                })
            })();
            (parent, child)
        },
    )
    .await
    .map_err(|(error, (parent, _child_name, _child_locator))| {
        Box::new((error, parent))
    })
}

enum LocalOverwriteOutcome {
    Success,
    Failure {
        error: std::io::Error,
        progress: crate::TransferProgress,
        target_evidence: OverwriteTargetEvidence,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StableFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial_number: u64,
        file_id: [u8; 16],
    },
}

impl StableFileIdentity {
    fn protocol_text(&self) -> String {
        match self {
            #[cfg(unix)]
            Self::Unix { device, inode } => {
                format!("u:{device:016x}:{inode:016x}")
            }
            #[cfg(windows)]
            Self::Windows {
                volume_serial_number,
                file_id,
            } => format!(
                "w:{volume_serial_number:016x}:{}",
                encode_hex(file_id),
            ),
        }
    }

    fn parse_protocol_text(value: &str) -> pi_result::Result<Self> {
        let mut parts = value.split(':');
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            #[cfg(unix)]
            (Some("u"), Some(device), Some(inode), None) => {
                let device = u64::from_str_radix(device, 16).map_err(|_| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Corrupted,
                    )
                })?;
                let inode = u64::from_str_radix(inode, 16).map_err(|_| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Corrupted,
                    )
                })?;
                if inode == 0 {
                    return Err(pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Corrupted,
                    ));
                }
                Ok(Self::Unix { device, inode })
            }
            #[cfg(windows)]
            (Some("w"), Some(volume), Some(file_id), None) => {
                let volume_serial_number = u64::from_str_radix(volume, 16)
                    .map_err(|_| {
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::Corrupted,
                        )
                    })?;
                let decoded = decode_hex(file_id)?;
                let file_id: [u8; 16] = decoded.try_into().map_err(|_| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Corrupted,
                    )
                })?;
                Ok(Self::Windows {
                    volume_serial_number,
                    file_id,
                })
            }
            _ => Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )),
        }
    }

    #[cfg(unix)]
    fn from_std_metadata(metadata: &Metadata) -> pi_result::Result<Self> {
        use std::os::unix::fs::MetadataExt;

        if metadata.ino() == 0 {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ));
        }
        Ok(Self::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(windows)]
    fn from_std_file(file: &std::fs::File) -> pi_result::Result<Self> {
        use std::mem::size_of;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
        };

        let mut information = FILE_ID_INFO::default();
        // SAFETY: `file` 在调用期间保持有效；输出缓冲区按目标结构正确对齐，
        // 长度与类型一致，系统调用只写入该局部缓冲区。
        let succeeded = unsafe {
            GetFileInformationByHandleEx(
                file.as_raw_handle() as usize
                    as windows_sys::Win32::Foundation::HANDLE,
                FileIdInfo,
                (&mut information as *mut FILE_ID_INFO).cast(),
                size_of::<FILE_ID_INFO>() as u32,
            )
        };
        if succeeded == 0 {
            return core::result::Result::<Self, _>::Err(
                std::io::Error::last_os_error(),
            )
            .into_classified_error();
        }
        if information.VolumeSerialNumber == 0
            && information.FileId.Identifier.iter().all(|byte| *byte == 0)
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ));
        }
        Ok(Self::Windows {
            volume_serial_number: information.VolumeSerialNumber,
            file_id: information.FileId.Identifier,
        })
    }
}

struct FileCoordinationData {
    ordinary_resources: u64,
    authority_initializing: bool,
    cross_process_core: Option<Weak<CrossProcessAuthorityCore>>,
    cross_process_binding: Option<ManagedCrossProcessBinding>,
    coordinated_resources: u64,
    pending_coordinated_resources: u64,
    active_metadata_queries: u64,
    active_content_reads: u64,
    active_appends: u64,
    active_flushes: u64,
    exclusive_operation: bool,
    mapping_ranges: BTreeMap<u64, MappingRangeRegistration>,
    active_mapping_views: BTreeMap<usize, ActiveMappingView>,
    pending_mappings: u64,
    next_mapping_id: u64,
}

enum MappingRangeState {
    Pending,
    Active,
}

struct MappingRangeRegistration {
    end_exclusive: u64,
    mapping_id: u64,
    state: MappingRangeState,
}

struct ActiveMappingView {
    view_end_exclusive: usize,
    mapping_id: u64,
}

struct FileCoordinationState {
    identity: StableFileIdentity,
    registry: &'static FileRegistry,
    data: Mutex<FileCoordinationData>,
    append_gate: Arc<async_lock::Mutex<()>>,
}

impl FileCoordinationState {
    fn new(
        identity: StableFileIdentity,
        registry: &'static FileRegistry,
        cross_process_binding: Option<ManagedCrossProcessBinding>,
    ) -> Self {
        Self {
            identity,
            registry,
            data: Mutex::new(FileCoordinationData {
                ordinary_resources: 0,
                authority_initializing: false,
                cross_process_core: None,
                cross_process_binding,
                coordinated_resources: 0,
                pending_coordinated_resources: 0,
                active_metadata_queries: 0,
                active_content_reads: 0,
                active_appends: 0,
                active_flushes: 0,
                exclusive_operation: false,
                mapping_ranges: BTreeMap::new(),
                active_mapping_views: BTreeMap::new(),
                pending_mappings: 0,
                next_mapping_id: 1,
            }),
            append_gate: Arc::new(async_lock::Mutex::new(())),
        }
    }

    fn register_ordinary_resource(&self) -> pi_result::Result<()> {
        let mut data = lock_unpoisoned(&self.data);
        if data.authority_initializing
            || data.cross_process_binding.is_some()
            || data.coordinated_resources != 0
            || data.pending_coordinated_resources != 0
            || data.exclusive_operation
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.ordinary_resources = data
            .ordinary_resources
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(())
    }

    fn unregister_ordinary_resource(&self) {
        let mut data = lock_unpoisoned(&self.data);
        if data.ordinary_resources != 0 {
            data.ordinary_resources -= 1;
        }
    }

    fn try_begin_authority_initialization(
        self: &Arc<Self>,
    ) -> pi_result::Result<AuthorityInitializationLease> {
        let mut data = lock_unpoisoned(&self.data);
        let has_live_authority = data.cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some();
        if data.ordinary_resources != 0
            || data.authority_initializing
            || data.exclusive_operation
            || (!has_live_authority
                && (data.coordinated_resources != 0
                    || data.pending_coordinated_resources != 0
                    || data.active_metadata_queries != 0
                    || data.active_content_reads != 0
                    || data.active_appends != 0
                    || data.active_flushes != 0
                    || !data.mapping_ranges.is_empty()
                    || data.pending_mappings != 0))
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.authority_initializing = true;
        Ok(AuthorityInitializationLease {
            coordination: Arc::clone(self),
            active: true,
        })
    }

    fn existing_cross_process_core(
        &self,
    ) -> Option<Arc<CrossProcessAuthorityCore>> {
        lock_unpoisoned(&self.data)
            .cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
    }

    fn existing_cross_process_binding(
        &self,
    ) -> Option<CrossProcessProtocolBinding> {
        lock_unpoisoned(&self.data)
            .cross_process_binding
            .as_ref()
            .map(|managed| managed.binding.clone())
    }

    fn clear_cross_process_binding(
        &self,
        binding: &CrossProcessProtocolBinding,
    ) {
        let cleared = {
            let mut data = lock_unpoisoned(&self.data);
            if data.cross_process_binding.as_ref().is_some_and(|managed| {
                &managed.binding == binding
            }) {
                data.cross_process_binding = None;
                true
            } else {
                false
            }
        };
        if cleared {
            self.registry.clear_cross_process_binding(
                &self.identity,
                self,
                binding,
            );
        }
    }

    fn try_reserve_coordinated_resource(
        self: &Arc<Self>,
        authority: &Arc<CrossProcessAuthorityCore>,
    ) -> pi_result::Result<PendingCoordinatedResource> {
        let mut data = lock_unpoisoned(&self.data);
        let is_current_authority = data.cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, authority));
        if authority.retired.load(AtomicOrdering::Acquire)
            || !is_current_authority
            || data.authority_initializing
            || data.exclusive_operation
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.pending_coordinated_resources = data
            .pending_coordinated_resources
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(PendingCoordinatedResource {
            coordination: Arc::clone(self),
            active: true,
        })
    }

    fn try_reserve_coordinated_operation(
        self: &Arc<Self>,
        authority: &Arc<CrossProcessAuthorityCore>,
    ) -> pi_result::Result<PendingCoordinatedResource> {
        // 这里只为已经取得进程内操作租约的在途操作延长跨进程生命周期。
        // 因而不能再次用 `exclusive_operation` 拒绝该操作自己的完成所有者；
        // 新公开资源仍必须走上面的严格准入，不能借此绕过独占状态。
        let mut data = lock_unpoisoned(&self.data);
        let is_current_authority = data.cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, authority));
        if authority.retired.load(AtomicOrdering::Acquire)
            || !is_current_authority
            || data.authority_initializing
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.pending_coordinated_resources = data
            .pending_coordinated_resources
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(PendingCoordinatedResource {
            coordination: Arc::clone(self),
            active: true,
        })
    }

    fn try_acquire_coordinated_removal(
        self: &Arc<Self>,
        authority: &Arc<CrossProcessAuthorityCore>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        let is_current_authority = data.cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| Arc::ptr_eq(&current, authority));
        if !is_current_authority
            || data.ordinary_resources != 0
            || data.authority_initializing
            || data.coordinated_resources != 0
            || data.pending_coordinated_resources != 0
            || data.active_metadata_queries != 0
            || data.active_content_reads != 0
            || data.active_appends != 0
            || data.active_flushes != 0
            || data.exclusive_operation
            || !data.mapping_ranges.is_empty()
            || data.pending_mappings != 0
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.exclusive_operation = true;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Exclusive,
        ))
    }

    fn try_acquire_removal_recovery(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.ordinary_resources != 0
            || data.authority_initializing
            || data.coordinated_resources != 0
            || data.pending_coordinated_resources != 0
            || data.active_metadata_queries != 0
            || data.active_content_reads != 0
            || data.active_appends != 0
            || data.active_flushes != 0
            || data.exclusive_operation
            || !data.mapping_ranges.is_empty()
            || data.pending_mappings != 0
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.exclusive_operation = true;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Exclusive,
        ))
    }

    fn try_acquire_metadata_query(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.active_metadata_queries = data.active_metadata_queries
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::MetadataQuery,
        ))
    }

    fn try_acquire_content_read(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation
            || !data.mapping_ranges.is_empty()
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.active_content_reads = data.active_content_reads
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::ContentRead,
        ))
    }

    fn try_acquire_uncoordinated_content_read(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.cross_process_binding.is_some()
            || data.exclusive_operation
            || !data.mapping_ranges.is_empty()
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.active_content_reads = data.active_content_reads
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::ContentRead,
        ))
    }

    fn try_acquire_append(
        self: &Arc<Self>,
        buffer: &[u8],
    ) -> pi_result::Result<FileOperationLease> {
        let buffer_start = buffer.as_ptr() as usize;
        let buffer_end_exclusive = buffer_start
            .checked_add(buffer.len())
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation
            || data.pending_mappings != 0
            || address_range_conflicts(
                &data.active_mapping_views,
                buffer_start,
                buffer_end_exclusive,
            )
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.active_appends = data.active_appends.checked_add(1).ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Append,
        ))
    }

    // 刷新不改内容或范围，与映射、追加、读取和其它刷新共享文件。独立计数
    // 只用于反向阻止覆盖、截断等破坏性操作，并由后台完成者一直保留至同步结束。
    // 在原同步锁的短临界区内完成 O(1) 检查/增加；不等异步任务、不调用用户
    // 代码。计数溢出返回 ResourceExhausted 且不改状态；已有独占时返回 Conflict。
    // 非纯函数，成功取得一项必须由 RAII 归还的许可；独立请求不是严格幂等。
    // 设计：B2H-DES-001，见 .scratch/pi-async-fs-mainline/FILE_FLUSH_COEXISTENCE_DESIGN_2026-10-08.md。
    // 真实组合与取消入口：tests/local_file_flush.rs、tests/local_cross_process_integration.rs。
    fn try_acquire_flush(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.active_flushes = data.active_flushes.checked_add(1).ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Flush,
        ))
    }

    fn try_acquire_exclusive(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation
            || data.active_metadata_queries != 0
            || data.active_content_reads != 0
            || data.active_appends != 0
            || data.active_flushes != 0
            || !data.mapping_ranges.is_empty()
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.exclusive_operation = true;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Exclusive,
        ))
    }

    fn try_acquire_staging_exclusive(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.ordinary_resources != 0
            || data.authority_initializing
            || data.cross_process_binding.is_some()
            || data.coordinated_resources != 0
            || data.pending_coordinated_resources != 0
            || data.active_metadata_queries != 0
            || data.active_content_reads != 0
            || data.active_appends != 0
            || data.active_flushes != 0
            || data.exclusive_operation
            || !data.mapping_ranges.is_empty()
            || data.pending_mappings != 0
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.exclusive_operation = true;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Exclusive,
        ))
    }

    fn try_acquire_removal(
        self: &Arc<Self>,
    ) -> pi_result::Result<FileOperationLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.ordinary_resources != 0
            || data.authority_initializing
            || data.cross_process_binding.is_some()
            || data.coordinated_resources != 0
            || data.pending_coordinated_resources != 0
            || data.active_metadata_queries != 0
            || data.active_content_reads != 0
            || data.active_appends != 0
            || data.active_flushes != 0
            || data.exclusive_operation
            || !data.mapping_ranges.is_empty()
            || data.pending_mappings != 0
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        data.exclusive_operation = true;
        Ok(FileOperationLease::new(
            Arc::clone(self),
            FileOperationLeaseKind::Exclusive,
        ))
    }

    fn try_reserve_mapping(
        self: &Arc<Self>,
        start: u64,
        end_exclusive: u64,
    ) -> pi_result::Result<PendingMappingLease> {
        let mut data = lock_unpoisoned(&self.data);
        if data.exclusive_operation
            || data.active_content_reads != 0
            || data.active_appends != 0
            || mapping_range_conflicts(
                &data.mapping_ranges,
                start,
                end_exclusive,
            )
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        let next_pending_count = data.pending_mappings
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        let mapping_id = data.next_mapping_id;
        let next_mapping_id = data.next_mapping_id
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        data.mapping_ranges.insert(
            start,
            MappingRangeRegistration {
                end_exclusive,
                mapping_id,
                state: MappingRangeState::Pending,
            },
        );
        data.pending_mappings = next_pending_count;
        data.next_mapping_id = next_mapping_id;
        Ok(PendingMappingLease {
            coordination: Arc::clone(self),
            start,
            end_exclusive,
            mapping_id,
            active: true,
        })
    }
}

const CROSS_PROCESS_PROTOCOL_VERSION: u32 = 1;
const CROSS_PROCESS_RECORD_MAX_BYTES: u64 = 4096;

#[derive(Clone, PartialEq, Eq)]
struct CrossProcessLayout {
    coordination_directory: PathBuf,
    bootstrap_gate: PathBuf,
    lifecycle_gate: PathBuf,
    stable_content_gate: PathBuf,
    append_admission_gate: PathBuf,
    range_admission_gate: PathBuf,
    range_directory: PathBuf,
    root_identity: StableFileIdentity,
    parent_identity: StableFileIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CrossProcessProtocolState {
    Preparing,
    Published,
    Removing,
    Removed,
    IdentityConflict,
}

impl CrossProcessProtocolState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Published => "published",
            Self::Removing => "removing",
            Self::Removed => "removed",
            Self::IdentityConflict => "identity-conflict",
        }
    }

    fn parse(value: &str) -> pi_result::Result<Self> {
        match value {
            "preparing" => Ok(Self::Preparing),
            "published" => Ok(Self::Published),
            "removing" => Ok(Self::Removing),
            "removed" => Ok(Self::Removed),
            "identity-conflict" => Ok(Self::IdentityConflict),
            _ => Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CrossProcessIdentityConflictReason {
    Missing,
    DifferentIdentity,
    UnexpectedIdentityPresent,
}

impl CrossProcessIdentityConflictReason {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::DifferentIdentity => "different-identity",
            Self::UnexpectedIdentityPresent => "unexpected-identity-present",
        }
    }

    fn parse(value: &str) -> pi_result::Result<Option<Self>> {
        match value {
            "none" => Ok(None),
            "missing" => Ok(Some(Self::Missing)),
            "different-identity" => Ok(Some(Self::DifferentIdentity)),
            "unexpected-identity-present" => {
                Ok(Some(Self::UnexpectedIdentityPresent))
            }
            _ => Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CrossProcessProtocolRecord {
    sequence: u64,
    state: CrossProcessProtocolState,
    identity_conflict_reason: Option<CrossProcessIdentityConflictReason>,
    generation: [u8; 16],
    target_incarnation: [u8; 16],
    target_identity: StableFileIdentity,
    root_identity: StableFileIdentity,
    parent_identity: StableFileIdentity,
}

#[derive(Clone, PartialEq, Eq)]
struct CrossProcessProtocolBinding {
    layout: CrossProcessLayout,
    generation: [u8; 16],
    target_incarnation: [u8; 16],
    target_identity: StableFileIdentity,
}

#[derive(Clone, PartialEq, Eq)]
struct ManagedCrossProcessBinding {
    locator: PathBuf,
    binding: CrossProcessProtocolBinding,
}

// 协调式排他创建在目标出现之前取得并持续持有 bootstrap 门禁。这个拥有型
// 计划把“预检协议状态”和“创建后发布新实例”绑定起来，避免两个合作进程在
// 目标可见而 Published 尚未发布的窗口内进入不同协议分支。
struct CrossProcessCreatePlan {
    layout: CrossProcessLayout,
    bootstrap: std::fs::File,
    preparing_sequence: u64,
    published_sequence: u64,
    generation: [u8; 16],
    target_incarnation: [u8; 16],
}

struct CrossProcessLifecycleState {
    active_owners: u64,
    shared_lock: Option<std::fs::File>,
}

pub(crate) struct CrossProcessAuthorityCore {
    namespace_identity: usize,
    locator: PathBuf,
    binding: CrossProcessProtocolBinding,
    coordination: Arc<FileCoordinationState>,
    lifecycle_state: Mutex<CrossProcessLifecycleState>,
    lifecycle_admission: Arc<async_lock::Mutex<()>>,
    retired: AtomicBool,
    diagnostic_id: u64,
}

impl CrossProcessAuthorityCore {
    fn new(
        namespace_identity: usize,
        locator: PathBuf,
        binding: CrossProcessProtocolBinding,
        coordination: Arc<FileCoordinationState>,
    ) -> Self {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&binding.target_incarnation[..8]);
        Self {
            namespace_identity,
            locator,
            binding,
            coordination,
            lifecycle_state: Mutex::new(CrossProcessLifecycleState {
                active_owners: 0,
                shared_lock: None,
            }),
            lifecycle_admission: Arc::new(async_lock::Mutex::new(())),
            retired: AtomicBool::new(false),
            diagnostic_id: u64::from_le_bytes(bytes),
        }
    }

    pub(crate) fn protocol_version(&self) -> u32 {
        CROSS_PROCESS_PROTOCOL_VERSION
    }

    pub(crate) fn diagnostic_id(&self) -> u64 {
        self.diagnostic_id
    }

    fn matches(
        &self,
        namespace_identity: usize,
        binding: &CrossProcessProtocolBinding,
    ) -> bool {
        self.namespace_identity == namespace_identity
            && self.binding == *binding
    }

    async fn acquire_active_owner(
        self: &Arc<Self>,
    ) -> pi_result::Result<CoordinatedResourceLease> {
        if self.retired.load(AtomicOrdering::Acquire) {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        let pending = self.coordination
            .try_reserve_coordinated_resource(self)?;
        let admission = self.lifecycle_admission.clone().lock_arc().await;

        let requires_lock = {
            let state = lock_unpoisoned(&self.lifecycle_state);
            state.active_owners == 0
        };
        let new_lock = if requires_lock {
            let path = self.binding.layout.lifecycle_gate.clone();
            Some(bounded_blocking::unblock_result(move || {
                try_lock_sidecar_shared(&path)
            })
            .await?)
        } else {
            None
        };

        {
            let mut state = lock_unpoisoned(&self.lifecycle_state);
            if state.active_owners == 0 {
                state.shared_lock = new_lock;
            } else {
                drop(new_lock);
            }
            state.active_owners = state.active_owners.checked_add(1)
                .ok_or_else(|| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::ResourceExhausted,
                    )
                })?;
        }
        let process_lease = pending.promote()?;
        drop(admission);
        Ok(CoordinatedResourceLease {
            authority: Arc::clone(self),
            process_lease: Some(process_lease),
        })
    }

    async fn acquire_operation_owner(
        self: &Arc<Self>,
    ) -> pi_result::Result<CoordinatedResourceLease> {
        if self.retired.load(AtomicOrdering::Acquire) {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        let pending = self.coordination
            .try_reserve_coordinated_operation(self)?;
        let admission = self.lifecycle_admission.clone().lock_arc().await;

        let requires_lock = {
            let state = lock_unpoisoned(&self.lifecycle_state);
            state.active_owners == 0
        };
        let new_lock = if requires_lock {
            let path = self.binding.layout.lifecycle_gate.clone();
            Some(bounded_blocking::unblock_result(move || {
                try_lock_sidecar_shared(&path)
            })
            .await?)
        } else {
            None
        };

        {
            let mut state = lock_unpoisoned(&self.lifecycle_state);
            if state.active_owners == 0 {
                state.shared_lock = new_lock;
            } else {
                drop(new_lock);
            }
            state.active_owners = state.active_owners.checked_add(1)
                .ok_or_else(|| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::ResourceExhausted,
                    )
                })?;
        }
        let process_lease = pending.promote()?;
        drop(admission);
        Ok(CoordinatedResourceLease {
            authority: Arc::clone(self),
            process_lease: Some(process_lease),
        })
    }

    fn release_active_owner(&self) {
        let lock_to_release = {
            let mut state = lock_unpoisoned(&self.lifecycle_state);
            if state.active_owners == 0 {
                None
            } else {
                state.active_owners -= 1;
                if state.active_owners == 0 {
                    state.shared_lock.take()
                } else {
                    None
                }
            }
        };
        // 关闭专属 sidecar handle 会释放 fs4 锁；handle 从不复制或公开。
        drop(lock_to_release);
    }

    async fn acquire_read_operation(
        self: &Arc<Self>,
    ) -> pi_result::Result<CrossProcessReadLease> {
        let active_owner = self.acquire_operation_owner().await?;
        let binding = self.binding.clone();
        let record = bounded_blocking::unblock_result(move || {
            let _admission = try_lock_sidecar_exclusive(
                &binding.layout.range_admission_gate,
            )?;
            scan_cross_process_leases(
                &binding,
                CrossProcessLeaseRequest::Read,
            )?;
            create_cross_process_lease_record(
                &binding,
                CrossProcessLeaseRecordKind::Read,
            )
        })
        .await?;
        Ok(CrossProcessReadLease {
            record,
            _active_owner: active_owner,
        })
    }

    // 元信息与普通文件刷新均只需要内容门的共享许可。刷新不取得追加/范围门，
    // 因而不会把其它进程的活动映射或独立追加误判为破坏性全文件访问。
    async fn acquire_shared_content_operation(
        self: &Arc<Self>,
    ) -> pi_result::Result<CrossProcessSharedContentLease> {
        let active_owner = self.acquire_operation_owner().await?;
        let content_path = self.binding.layout.stable_content_gate.clone();
        let content = bounded_blocking::unblock_result(move || {
            try_lock_sidecar_shared(&content_path)
        })
        .await?;
        Ok(CrossProcessSharedContentLease {
            _content: content,
            _active_owner: active_owner,
        })
    }

    async fn acquire_append_operation(
        self: &Arc<Self>,
    ) -> pi_result::Result<CrossProcessAppendLease> {
        let active_owner = self.acquire_operation_owner().await?;
        let content_path = self.binding.layout.stable_content_gate.clone();
        let append_path = self.binding.layout.append_admission_gate.clone();
        let (content, append) = bounded_blocking::unblock_result(move || {
            let content = try_lock_sidecar_shared(&content_path)?;
            let append = lock_sidecar_exclusive(&append_path)?;
            Ok::<_, pi_result::Error>((content, append))
        })
        .await?;
        Ok(CrossProcessAppendLease {
            _content: content,
            _append: append,
            _active_owner: active_owner,
        })
    }

    async fn acquire_exclusive_operation(
        self: &Arc<Self>,
    ) -> pi_result::Result<CrossProcessExclusiveLease> {
        let active_owner = self.acquire_operation_owner().await?;
        let binding = self.binding.clone();
        let (content, append, range) = bounded_blocking::unblock_result(move || {
            let content = try_lock_sidecar_exclusive(
                &binding.layout.stable_content_gate,
            )?;
            let append = try_lock_sidecar_exclusive(
                &binding.layout.append_admission_gate,
            )?;
            let range = try_lock_sidecar_exclusive(
                &binding.layout.range_admission_gate,
            )?;
            scan_cross_process_leases(
                &binding,
                CrossProcessLeaseRequest::Exclusive,
            )?;
            Ok::<_, pi_result::Error>((content, append, range))
        })
        .await?;
        Ok(CrossProcessExclusiveLease {
            _content: content,
            _append: append,
            _range: range,
            _active_owner: active_owner,
        })
    }

    async fn acquire_mapping_operation(
        self: &Arc<Self>,
        start: u64,
        end_exclusive: u64,
    ) -> pi_result::Result<CrossProcessMappingLease> {
        let active_owner = self.acquire_operation_owner().await?;
        let binding = self.binding.clone();
        let (content, record) = bounded_blocking::unblock_result(move || {
            let content = try_lock_sidecar_shared(
                &binding.layout.stable_content_gate,
            )?;
            let _append = try_lock_sidecar_exclusive(
                &binding.layout.append_admission_gate,
            )?;
            let _range = try_lock_sidecar_exclusive(
                &binding.layout.range_admission_gate,
            )?;
            scan_cross_process_leases(
                &binding,
                CrossProcessLeaseRequest::Mapping {
                    start,
                    end_exclusive,
                },
            )?;
            let record = create_cross_process_lease_record(
                &binding,
                CrossProcessLeaseRecordKind::Mapping {
                    start,
                    end_exclusive,
                },
            )?;
            Ok::<_, pi_result::Error>((content, record))
        })
        .await?;
        Ok(CrossProcessMappingLease {
            _content: content,
            _record: record,
            _active_owner: active_owner,
        })
    }

    fn mark_retired(&self) {
        self.retired.store(true, AtomicOrdering::Release);
    }
}

struct AuthorityInitializationLease {
    coordination: Arc<FileCoordinationState>,
    active: bool,
}

impl AuthorityInitializationLease {
    fn complete(
        mut self,
        authority: &Arc<CrossProcessAuthorityCore>,
    ) -> pi_result::Result<()> {
        let mut data = lock_unpoisoned(&self.coordination.data);
        if !self.active || !data.authority_initializing {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidState,
            ));
        }
        if data.cross_process_binding.as_ref().is_some_and(|binding| {
            binding.binding != authority.binding
        }) && data.cross_process_core
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some_and(|current| {
                !current.retired.load(AtomicOrdering::Acquire)
            })
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        let binding = authority.binding.clone();
        let managed_binding = ManagedCrossProcessBinding {
            locator: authority.locator.clone(),
            binding: binding.clone(),
        };
        data.cross_process_core = Some(Arc::downgrade(authority));
        data.cross_process_binding = Some(managed_binding.clone());
        data.authority_initializing = false;
        self.active = false;
        drop(data);
        self.coordination.registry.install_cross_process_binding(
            &self.coordination.identity,
            &self.coordination,
            managed_binding,
        );
        Ok(())
    }
}

impl Drop for AuthorityInitializationLease {
    fn drop(&mut self) {
        if self.active {
            lock_unpoisoned(&self.coordination.data).authority_initializing = false;
        }
    }
}

struct PendingCoordinatedResource {
    coordination: Arc<FileCoordinationState>,
    active: bool,
}

impl PendingCoordinatedResource {
    fn promote(mut self) -> pi_result::Result<CoordinatedProcessLease> {
        let mut data = lock_unpoisoned(&self.coordination.data);
        if !self.active || data.pending_coordinated_resources == 0 {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidState,
            ));
        }
        data.pending_coordinated_resources -= 1;
        data.coordinated_resources = data.coordinated_resources
            .checked_add(1)
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })?;
        self.active = false;
        Ok(CoordinatedProcessLease {
            coordination: Arc::clone(&self.coordination),
        })
    }
}

impl Drop for PendingCoordinatedResource {
    fn drop(&mut self) {
        if self.active {
            let mut data = lock_unpoisoned(&self.coordination.data);
            if data.pending_coordinated_resources != 0 {
                data.pending_coordinated_resources -= 1;
            }
        }
    }
}

struct CoordinatedProcessLease {
    coordination: Arc<FileCoordinationState>,
}

impl Drop for CoordinatedProcessLease {
    fn drop(&mut self) {
        let mut data = lock_unpoisoned(&self.coordination.data);
        if data.coordinated_resources != 0 {
            data.coordinated_resources -= 1;
        }
    }
}

struct CoordinatedResourceLease {
    authority: Arc<CrossProcessAuthorityCore>,
    process_lease: Option<CoordinatedProcessLease>,
}

struct CrossProcessReadLease {
    record: std::fs::File,
    _active_owner: CoordinatedResourceLease,
}

struct CrossProcessSharedContentLease {
    _content: std::fs::File,
    _active_owner: CoordinatedResourceLease,
}

struct CrossProcessAppendLease {
    _content: std::fs::File,
    _append: std::fs::File,
    _active_owner: CoordinatedResourceLease,
}

struct CrossProcessExclusiveLease {
    _content: std::fs::File,
    _append: std::fs::File,
    _range: std::fs::File,
    _active_owner: CoordinatedResourceLease,
}

struct CrossProcessMappingLease {
    _content: std::fs::File,
    _record: std::fs::File,
    _active_owner: CoordinatedResourceLease,
}

impl Drop for CoordinatedResourceLease {
    fn drop(&mut self) {
        self.process_lease.take();
        self.authority.release_active_owner();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CrossProcessLeaseRecordKind {
    Read,
    Mapping { start: u64, end_exclusive: u64 },
}

enum CrossProcessLeaseRequest {
    Read,
    Mapping { start: u64, end_exclusive: u64 },
    Exclusive,
}

fn scan_cross_process_leases(
    binding: &CrossProcessProtocolBinding,
    request: CrossProcessLeaseRequest,
) -> pi_result::Result<()> {
    for entry in std::fs::read_dir(&binding.layout.range_directory)
        .into_classified_error()?
    {
        let entry = entry.into_classified_error()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            ));
        };
        if !name.starts_with("lease_") || !name.ends_with(".state") {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            ));
        }
        let path = entry.path();
        let file = open_existing_sidecar_file(&path)?;
        match fs4::FileExt::try_lock(&file) {
            Ok(()) => {
                drop(file);
                std::fs::remove_file(&path).into_classified_error()?;
            }
            Err(fs4::TryLockError::WouldBlock) => {
                let kind = read_cross_process_lease_record(
                    &file,
                    binding,
                )?;
                let conflicts = match (&request, kind) {
                    (
                        CrossProcessLeaseRequest::Read,
                        CrossProcessLeaseRecordKind::Read,
                    ) => false,
                    (
                        CrossProcessLeaseRequest::Read,
                        CrossProcessLeaseRecordKind::Mapping { .. },
                    ) => true,
                    (CrossProcessLeaseRequest::Exclusive, _) => true,
                    (
                        CrossProcessLeaseRequest::Mapping { .. },
                        CrossProcessLeaseRecordKind::Read,
                    ) => true,
                    (
                        CrossProcessLeaseRequest::Mapping {
                            start,
                            end_exclusive,
                        },
                        CrossProcessLeaseRecordKind::Mapping {
                            start: existing_start,
                            end_exclusive: existing_end,
                        },
                    ) => *start < existing_end
                        && existing_start < *end_exclusive,
                };
                if conflicts {
                    return Err(pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Conflict,
                    ));
                }
            }
            Err(fs4::TryLockError::Error(error)) => {
                return Err(classified_io_error(error));
            }
        }
    }
    Ok(())
}

fn create_cross_process_lease_record(
    binding: &CrossProcessProtocolBinding,
    kind: CrossProcessLeaseRecordKind,
) -> pi_result::Result<std::fs::File> {
    let body = match &kind {
        CrossProcessLeaseRecordKind::Read => format!(
            "magic=pi-async-fs-lease\nversion=1\nkind=read\ngeneration={}\nincarnation={}\ntarget={}\n",
            encode_hex(&binding.generation),
            encode_hex(&binding.target_incarnation),
            binding.target_identity.protocol_text(),
        ),
        CrossProcessLeaseRecordKind::Mapping {
            start,
            end_exclusive,
        } => format!(
            "magic=pi-async-fs-lease\nversion=1\nkind=mapping\nstart={start:016x}\nend={end_exclusive:016x}\ngeneration={}\nincarnation={}\ntarget={}\n",
            encode_hex(&binding.generation),
            encode_hex(&binding.target_incarnation),
            binding.target_identity.protocol_text(),
        ),
    };
    let encoded = format!(
        "{body}checksum={}\n",
        encode_hex(&Sha256::digest(body.as_bytes())),
    );
    for _ in 0..32 {
        let path = binding.layout.range_directory.join(format!(
            "lease_{}.state",
            encode_hex(&random_protocol_token()),
        ));
        let mut file = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                continue;
            }
            Err(error) => return Err(classified_io_error(error)),
        };
        if let Err(error) = file.write_all(encoded.as_bytes()) {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(classified_io_error(error));
        }
        // 记录持有方只需要阻止清理者取得排它锁。共享锁在 Windows 上仍允许
        // 其它进程读取记录正文；若改用排它锁，LockFileEx 会同时阻止扫描者
        // 读取正文，使一个正常的活动租约被误报为底层 I/O 错误。
        if let Err(error) = fs4::FileExt::try_lock_shared(&file) {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return match error {
                fs4::TryLockError::WouldBlock => Err(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Conflict,
                    ),
                ),
                fs4::TryLockError::Error(error) => {
                    Err(classified_io_error(error))
                }
            };
        }
        return Ok(file);
    }
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::ResourceExhausted,
    ))
}

fn read_cross_process_lease_record(
    file: &std::fs::File,
    binding: &CrossProcessProtocolBinding,
) -> pi_result::Result<CrossProcessLeaseRecordKind> {
    let metadata = file.metadata().into_classified_error()?;
    if metadata.len() > CROSS_PROCESS_RECORD_MAX_BYTES {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    let reader = file;
    reader
        .take(CROSS_PROCESS_RECORD_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .into_classified_error()?;
    if bytes.len() as u64 > CROSS_PROCESS_RECORD_MAX_BYTES {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    let checksum_start = text.rfind("checksum=").ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    let body = &text[..checksum_start];
    let checksum = text[checksum_start + "checksum=".len()..]
        .strip_suffix('\n')
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })?;
    if decode_hex(checksum)? != Sha256::digest(body.as_bytes()).as_slice() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let mut fields = HashMap::new();
    for line in body.lines() {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })?;
        if fields.insert(key, value).is_some() {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            ));
        }
    }
    if fields.get("magic") != Some(&"pi-async-fs-lease")
        || fields.get("version") != Some(&"1")
        || decode_fixed_token(required_field(&fields, "generation")?)?
            != binding.generation
        || decode_fixed_token(required_field(&fields, "incarnation")?)?
            != binding.target_incarnation
        || StableFileIdentity::parse_protocol_text(
            required_field(&fields, "target")?,
        )? != binding.target_identity
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    match required_field(&fields, "kind")? {
        "read" if fields.len() == 6 => Ok(CrossProcessLeaseRecordKind::Read),
        "mapping" if fields.len() == 8 => {
            let start = u64::from_str_radix(
                required_field(&fields, "start")?,
                16,
            )
            .map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Corrupted,
                )
            })?;
            let end_exclusive = u64::from_str_radix(
                required_field(&fields, "end")?,
                16,
            )
            .map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Corrupted,
                )
            })?;
            if start >= end_exclusive {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Corrupted,
                ));
            }
            Ok(CrossProcessLeaseRecordKind::Mapping {
                start,
                end_exclusive,
            })
        }
        _ => Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )),
    }
}

fn lock_sidecar_exclusive(path: &Path) -> pi_result::Result<std::fs::File> {
    let file = open_existing_sidecar_file(path)?;
    fs4::FileExt::lock(&file).map_err(classified_io_error)?;
    Ok(file)
}

fn try_lock_sidecar_shared(path: &Path) -> pi_result::Result<std::fs::File> {
    let file = open_existing_sidecar_file(path)?;
    match fs4::FileExt::try_lock_shared(&file) {
        Ok(()) => Ok(file),
        Err(fs4::TryLockError::WouldBlock) => Err(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ),
        ),
        Err(fs4::TryLockError::Error(error)) => Err(classified_io_error(error)),
    }
}

fn try_lock_sidecar_exclusive(path: &Path) -> pi_result::Result<std::fs::File> {
    let file = open_existing_sidecar_file(path)?;
    match fs4::FileExt::try_lock(&file) {
        Ok(()) => Ok(file),
        Err(fs4::TryLockError::WouldBlock) => Err(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ),
        ),
        Err(fs4::TryLockError::Error(error)) => Err(classified_io_error(error)),
    }
}

fn open_sidecar_file(path: &Path) -> pi_result::Result<std::fs::File> {
    reject_existing_symlink(path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .into_classified_error()?;
    if !file.metadata().into_classified_error()?.is_file() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    reject_existing_symlink(path)?;
    Ok(file)
}

fn open_existing_sidecar_file(path: &Path) -> pi_result::Result<std::fs::File> {
    reject_existing_symlink(path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .into_classified_error()?;
    if !file.metadata().into_classified_error()?.is_file() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    reject_existing_symlink(path)?;
    Ok(file)
}

fn reject_existing_symlink(path: &Path) -> pi_result::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
        ),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(classified_io_error(error)),
    }
}

fn establish_protocol_for_existing_target(
    locator: &Path,
    target_identity: &StableFileIdentity,
) -> pi_result::Result<CrossProcessProtocolBinding> {
    // 状态门禁：在 bootstrap 排它持有期内，只允许“无记录 → Preparing →
    // Published”、同身份的“Preparing → Published”恢复，或验证已有 Published。
    // Removing、Removed 与 IdentityConflict 都是本入口的冲突终点；任何返回的
    // binding 都必须对应同一根、父目录和目标身份。正式状态语义见
    // `.scratch/pi-async-fs-mainline/BATCH_ONE_INTERNAL_IMPLEMENTATION_DESIGN.md`，
    // 审查追踪见 `docs/BUG_ISSUES_CONTEXT.md#issue-b2h-doc-001`。
    let layout = resolve_cross_process_layout(locator, true, true)?;
    let _bootstrap = try_lock_sidecar_exclusive(&layout.bootstrap_gate)?;
    let current = read_current_protocol_record(&layout.coordination_directory)?;
    let published = match current {
        None => {
            let preparing = CrossProcessProtocolRecord {
                sequence: 0,
                state: CrossProcessProtocolState::Preparing,
                identity_conflict_reason: None,
                generation: random_protocol_token(),
                target_incarnation: random_protocol_token(),
                target_identity: target_identity.clone(),
                root_identity: layout.root_identity.clone(),
                parent_identity: layout.parent_identity.clone(),
            };
            let preparing = publish_protocol_record(
                &layout.coordination_directory,
                preparing,
            )?;
            let mut published = preparing;
            published.sequence = published.sequence.checked_add(1)
                .ok_or_else(|| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::ResourceExhausted,
                    )
                })?;
            published.state = CrossProcessProtocolState::Published;
            publish_protocol_record(&layout.coordination_directory, published)?
        }
        Some(record) => match record.state {
            CrossProcessProtocolState::Preparing => {
                validate_record_binding(&record, &layout, target_identity)?;
                let mut published = record;
                published.sequence = published.sequence.checked_add(1)
                    .ok_or_else(|| {
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::ResourceExhausted,
                        )
                    })?;
                published.state = CrossProcessProtocolState::Published;
                publish_protocol_record(
                    &layout.coordination_directory,
                    published,
                )?
            }
            CrossProcessProtocolState::Published => {
                validate_record_binding(&record, &layout, target_identity)?;
                record
            }
            CrossProcessProtocolState::Removing
            | CrossProcessProtocolState::Removed
            | CrossProcessProtocolState::IdentityConflict => {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ));
            }
        },
    };
    Ok(binding_from_record(layout, &published))
}

fn prepare_protocol_for_new_target(
    locator: &Path,
) -> pi_result::Result<CrossProcessCreatePlan> {
    // 本阶段只冻结协调布局、取得 bootstrap 排它并分配下一代次，不发布目标
    // 身份尚未知的记录。返回 plan 必须持续拥有 bootstrap，直到实际新文件已
    // 建立并由 `publish_protocol_for_new_target` 完成 Preparing → Published；
    // 中途 Drop 只释放准入，不伪造已发布状态。状态合同与追踪入口同上。
    let layout = resolve_cross_process_layout(locator, true, true)?;
    let bootstrap = try_lock_sidecar_exclusive(&layout.bootstrap_gate)?;
    let current = read_current_protocol_record(&layout.coordination_directory)?;

    let (preparing_sequence, generation) = match current {
        None => (0, random_protocol_token()),
        Some(record) => {
            if record.root_identity != layout.root_identity
                || record.parent_identity != layout.parent_identity
            {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ));
            }
            match record.state {
                CrossProcessProtocolState::Preparing
                | CrossProcessProtocolState::Removed => {
                    let sequence = record.sequence.checked_add(1).ok_or_else(|| {
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::ResourceExhausted,
                        )
                    })?;
                    (sequence, record.generation)
                }
                CrossProcessProtocolState::Published
                | CrossProcessProtocolState::Removing
                | CrossProcessProtocolState::IdentityConflict => {
                    return Err(pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Conflict,
                    ));
                }
            }
        }
    };
    let published_sequence = preparing_sequence.checked_add(1).ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::ResourceExhausted,
        )
    })?;

    Ok(CrossProcessCreatePlan {
        layout,
        bootstrap,
        preparing_sequence,
        published_sequence,
        generation,
        target_incarnation: random_protocol_token(),
    })
}

fn publish_protocol_for_new_target(
    plan: CrossProcessCreatePlan,
    target_identity: StableFileIdentity,
) -> pi_result::Result<(CrossProcessProtocolBinding, std::fs::File)> {
    // `plan` 独占 bootstrap，因此这两个发布点构成同一创建代次的唯一合法
    // Preparing → Published 序列。任一步失败都不能返回 authority；调用方以
    // `CreateTargetEvidence::CreatedByOperation` 报告真实目标可能已存在，并由
    // 后续显式恢复／运维处理，而不能静默假定状态已回滚。
    let preparing = CrossProcessProtocolRecord {
        sequence: plan.preparing_sequence,
        state: CrossProcessProtocolState::Preparing,
        identity_conflict_reason: None,
        generation: plan.generation,
        target_incarnation: plan.target_incarnation,
        target_identity: target_identity.clone(),
        root_identity: plan.layout.root_identity.clone(),
        parent_identity: plan.layout.parent_identity.clone(),
    };
    publish_protocol_record(
        &plan.layout.coordination_directory,
        preparing.clone(),
    )?;
    let mut published = preparing;
    published.sequence = plan.published_sequence;
    published.state = CrossProcessProtocolState::Published;
    let published = publish_protocol_record(
        &plan.layout.coordination_directory,
        published,
    )?;
    let binding = binding_from_record(plan.layout, &published);
    Ok((binding, plan.bootstrap))
}

struct CrossProcessRemovalPlan {
    authority: Arc<CrossProcessAuthorityCore>,
    bootstrap: std::fs::File,
    record: CrossProcessProtocolRecord,
}

#[derive(Clone, Copy)]
enum ManagedTargetObservation {
    Missing,
    Matching,
    Different,
}

fn prepare_authorized_removal(
    authority: Arc<CrossProcessAuthorityCore>,
) -> pi_result::Result<CrossProcessRemovalPlan> {
    // 规划阶段在 bootstrap 排它下把 authority 与当前持久记录重新绑定，但
    // 尚不改变协议状态或删除目标。只有根、父目录、代次、实例和目标身份全部
    // 一致的记录才能进入完成阶段，防止过期 authority 删除后继文件实例。
    let actual_layout = resolve_cross_process_layout(
        &authority.locator,
        false,
        true,
    )?;
    if actual_layout != authority.binding.layout {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    let bootstrap = try_lock_sidecar_exclusive(
        &authority.binding.layout.bootstrap_gate,
    )?;
    let record = read_current_protocol_record(
        &authority.binding.layout.coordination_directory,
    )?
    .ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    if record.generation != authority.binding.generation
        || record.target_incarnation != authority.binding.target_incarnation
        || record.target_identity != authority.binding.target_identity
        || record.root_identity != authority.binding.layout.root_identity
        || record.parent_identity != authority.binding.layout.parent_identity
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    Ok(CrossProcessRemovalPlan {
        authority,
        bootstrap,
        record,
    })
}

#[cfg(unix)]
fn observe_managed_target(
    locator: &Path,
    expected: &StableFileIdentity,
) -> pi_result::Result<ManagedTargetObservation> {
    let metadata = match std::fs::symlink_metadata(locator) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedTargetObservation::Missing);
        }
        Err(error) => return Err(classified_io_error(error)),
    };
    if !metadata.file_type().is_file() {
        return Ok(ManagedTargetObservation::Different);
    }
    let identity = StableFileIdentity::from_std_metadata(&metadata)?;
    Ok(if &identity == expected {
        ManagedTargetObservation::Matching
    } else {
        ManagedTargetObservation::Different
    })
}

#[cfg(windows)]
fn observe_managed_target(
    locator: &Path,
    expected: &StableFileIdentity,
) -> pi_result::Result<ManagedTargetObservation> {
    let metadata = match std::fs::symlink_metadata(locator) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedTargetObservation::Missing);
        }
        Err(error) => return Err(classified_io_error(error)),
    };
    if !metadata.file_type().is_file() {
        return Ok(ManagedTargetObservation::Different);
    }
    let file = match std::fs::OpenOptions::new().read(true).open(locator) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedTargetObservation::Missing);
        }
        Err(error) => return Err(classified_io_error(error)),
    };
    let identity = StableFileIdentity::from_std_file(&file)?;
    Ok(if &identity == expected {
        ManagedTargetObservation::Matching
    } else {
        ManagedTargetObservation::Different
    })
}

#[cfg(not(any(unix, windows)))]
fn observe_managed_target(
    _locator: &Path,
    _expected: &StableFileIdentity,
) -> pi_result::Result<ManagedTargetObservation> {
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

fn publish_protocol_state(
    directory: &Path,
    mut record: CrossProcessProtocolRecord,
    state: CrossProcessProtocolState,
) -> pi_result::Result<CrossProcessProtocolRecord> {
    if state == CrossProcessProtocolState::IdentityConflict {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }
    record.sequence = record.sequence.checked_add(1).ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::ResourceExhausted,
        )
    })?;
    record.state = state;
    record.identity_conflict_reason = None;
    publish_protocol_record(directory, record)
}

fn publish_identity_conflict(
    directory: &Path,
    mut record: CrossProcessProtocolRecord,
    reason: CrossProcessIdentityConflictReason,
) -> pi_result::Result<CrossProcessProtocolRecord> {
    record.sequence = record.sequence.checked_add(1).ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::ResourceExhausted,
        )
    })?;
    record.state = CrossProcessProtocolState::IdentityConflict;
    record.identity_conflict_reason = Some(reason);
    publish_protocol_record(directory, record)
}

fn finish_authorized_removal(
    mut plan: CrossProcessRemovalPlan,
) -> pi_result::RawResult<(), RemoveFailure> {
    // 删除线性化顺序为 Published → Removing → 目标名称移除 → Removed；
    // lifecycle 排它覆盖活动 owner 排空，bootstrap 排它由 plan 覆盖全部持久
    // 状态推进。身份缺失／替换会转入 IdentityConflict，不允许按名称继续删除。
    // `Removing` 可由同 authority 重试，`Removed + Missing` 是实例级幂等成功。
    // 详见内部实现设计与 `tests/local_cross_process_coordination.rs`、
    // `tests/local_cross_process_integration.rs` 的删除／恢复矩阵。
    let _lifecycle = try_lock_sidecar_exclusive(
        &plan.authority.binding.layout.lifecycle_gate,
    )
    .map_err(remove_failure)?;
    let directory = plan.authority
        .binding
        .layout
        .coordination_directory
        .clone();
    let observation = observe_managed_target(
        &plan.authority.locator,
        &plan.authority.binding.target_identity,
    )
    .map_err(remove_failure)?;

    match plan.record.state.clone() {
        CrossProcessProtocolState::Published => match observation {
            ManagedTargetObservation::Matching => {
                plan.record = publish_protocol_state(
                    &directory,
                    plan.record,
                    CrossProcessProtocolState::Removing,
                )
                .map_err(remove_failure)?;
                plan.authority.mark_retired();
            }
            ManagedTargetObservation::Missing => {
                publish_identity_conflict(
                    &directory,
                    plan.record,
                    CrossProcessIdentityConflictReason::Missing,
                )
                .map_err(remove_failure)?;
                plan.authority.mark_retired();
                return Err(remove_failure(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Conflict,
                    ),
                ));
            }
            ManagedTargetObservation::Different => {
                publish_identity_conflict(
                    &directory,
                    plan.record,
                    CrossProcessIdentityConflictReason::DifferentIdentity,
                )
                .map_err(remove_failure)?;
                plan.authority.mark_retired();
                return Err(remove_failure(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Conflict,
                    ),
                ));
            }
        },
        CrossProcessProtocolState::Removing => {
            plan.authority.mark_retired();
        }
        CrossProcessProtocolState::Removed => {
            plan.authority.mark_retired();
            return match observation {
                ManagedTargetObservation::Missing => {
                    plan.authority.coordination.clear_cross_process_binding(
                        &plan.authority.binding,
                    );
                    Ok(())
                }
                ManagedTargetObservation::Matching => {
                    publish_identity_conflict(
                        &directory,
                        plan.record,
                        CrossProcessIdentityConflictReason::UnexpectedIdentityPresent,
                    )
                    .map_err(remove_failure)?;
                    Err(remove_failure(
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::Conflict,
                        ),
                    ))
                }
                ManagedTargetObservation::Different => {
                    publish_identity_conflict(
                        &directory,
                        plan.record,
                        CrossProcessIdentityConflictReason::DifferentIdentity,
                    )
                    .map_err(remove_failure)?;
                    Err(remove_failure(
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::Conflict,
                        ),
                    ))
                }
            };
        }
        CrossProcessProtocolState::Preparing
        | CrossProcessProtocolState::IdentityConflict => {
            return Err(remove_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ),
            ));
        }
    }

    match observation {
        ManagedTargetObservation::Missing => {}
        ManagedTargetObservation::Matching => {
            if let Err(error) = std::fs::remove_file(&plan.authority.locator) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    return Err(remove_failure(classified_io_error(error)));
                }
            }
        }
        ManagedTargetObservation::Different => {
            publish_identity_conflict(
                &directory,
                plan.record,
                CrossProcessIdentityConflictReason::DifferentIdentity,
            )
            .map_err(|error| {
                RemoveFailure::new(
                    error,
                    crate::RemoveTargetEvidence::Unknown,
                )
            })?;
            return Err(RemoveFailure::new(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ),
                crate::RemoveTargetEvidence::Unknown,
            ));
        }
    }

    publish_protocol_state(
        &directory,
        plan.record,
        CrossProcessProtocolState::Removed,
    )
    .map_err(|error| {
        RemoveFailure::new(
            error,
            crate::RemoveTargetEvidence::RemovedByOperation,
        )
    })?;
    plan.authority.coordination.clear_cross_process_binding(
        &plan.authority.binding,
    );
    drop(plan.bootstrap);
    Ok(())
}

struct CrossProcessRemovalRecoveryPlan {
    locator: PathBuf,
    layout: CrossProcessLayout,
    bootstrap: std::fs::File,
    record: CrossProcessProtocolRecord,
}

fn prepare_removal_recovery(
    locator: PathBuf,
) -> pi_result::Result<CrossProcessRemovalRecoveryPlan> {
    // 无 authority 的恢复入口只接纳已经持久化为 Removing 或 Removed 的记录；
    // Published 从未提交删除，不能由恢复操作越权推进。bootstrap 排它随 plan
    // 跨越后续观察与完成阶段，阻止另一恢复者并行改变同一协议代次。
    let layout = resolve_cross_process_layout(&locator, false, true)?;
    let bootstrap = try_lock_sidecar_exclusive(&layout.bootstrap_gate)?;
    let record = read_current_protocol_record(&layout.coordination_directory)?
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::NotFound,
            )
        })?;
    if record.root_identity != layout.root_identity
        || record.parent_identity != layout.parent_identity
        || !matches!(
            record.state,
            CrossProcessProtocolState::Removing
                | CrossProcessProtocolState::Removed
        )
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    Ok(CrossProcessRemovalRecoveryPlan {
        locator,
        layout,
        bootstrap,
        record,
    })
}

fn finish_removal_recovery(
    plan: CrossProcessRemovalRecoveryPlan,
) -> pi_result::RawResult<(), RemoveFailure> {
    // 恢复只补完先前已提交的删除：Removing 下同身份仍在则删除，缺失则直接
    // 发布 Removed；任何不同身份均转为 IdentityConflict。Removed 仅在路径
    // 仍缺失时幂等成功，不得删除后来出现在同名路径上的任意对象。
    let _lifecycle = try_lock_sidecar_exclusive(&plan.layout.lifecycle_gate)
        .map_err(remove_failure)?;
    let observation = observe_managed_target(
        &plan.locator,
        &plan.record.target_identity,
    )
    .map_err(remove_failure)?;
    let directory = plan.layout.coordination_directory.clone();
    let state = plan.record.state.clone();

    match (state, observation) {
        (CrossProcessProtocolState::Removed, ManagedTargetObservation::Missing) => {
            drop(plan.bootstrap);
            Ok(())
        }
        (
            CrossProcessProtocolState::Removed,
            ManagedTargetObservation::Matching
            | ManagedTargetObservation::Different,
        ) => {
            let reason = if matches!(
                observation,
                ManagedTargetObservation::Matching,
            ) {
                CrossProcessIdentityConflictReason::UnexpectedIdentityPresent
            } else {
                CrossProcessIdentityConflictReason::DifferentIdentity
            };
            publish_identity_conflict(
                &directory,
                plan.record,
                reason,
            )
            .map_err(remove_failure)?;
            Err(remove_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ),
            ))
        }
        (
            CrossProcessProtocolState::Removing,
            ManagedTargetObservation::Different,
        ) => {
            publish_identity_conflict(
                &directory,
                plan.record,
                CrossProcessIdentityConflictReason::DifferentIdentity,
            )
            .map_err(|error| {
                RemoveFailure::new(
                    error,
                    crate::RemoveTargetEvidence::Unknown,
                )
            })?;
            Err(RemoveFailure::new(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ),
                crate::RemoveTargetEvidence::Unknown,
            ))
        }
        (
            CrossProcessProtocolState::Removing,
            ManagedTargetObservation::Missing
            | ManagedTargetObservation::Matching,
        ) => {
            if matches!(observation, ManagedTargetObservation::Matching) {
                if let Err(error) = std::fs::remove_file(&plan.locator) {
                    if error.kind() != std::io::ErrorKind::NotFound {
                        return Err(remove_failure(classified_io_error(error)));
                    }
                }
            }
            publish_protocol_state(
                &directory,
                plan.record,
                CrossProcessProtocolState::Removed,
            )
            .map_err(|error| {
                RemoveFailure::new(
                    error,
                    crate::RemoveTargetEvidence::RemovedByOperation,
                )
            })?;
            drop(plan.bootstrap);
            Ok(())
        }
        _ => Err(remove_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ),
        )),
    }
}

// `validate_published_protocol` 的物理布局复核需要原 locator；调用方直接使用
// authority 中的 locator 调用下面这个入口，避免从 sidecar 反推业务路径。
fn validate_published_protocol_at(
    locator: &Path,
    binding: &CrossProcessProtocolBinding,
) -> pi_result::Result<()> {
    let actual_layout = resolve_cross_process_layout(locator, false, true)?;
    if actual_layout != binding.layout {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    let _bootstrap = try_lock_sidecar_exclusive(
        &binding.layout.bootstrap_gate,
    )?;
    let record = read_current_protocol_record(
        &binding.layout.coordination_directory,
    )?
    .ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    if record.state != CrossProcessProtocolState::Published
        || record.generation != binding.generation
        || record.target_incarnation != binding.target_incarnation
        || record.target_identity != binding.target_identity
        || record.root_identity != binding.layout.root_identity
        || record.parent_identity != binding.layout.parent_identity
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    Ok(())
}

fn reject_persisted_cross_process_domain(
    locator: &Path,
) -> pi_result::Result<()> {
    if locator.file_name().is_none() {
        return Ok(());
    }
    let layout = match resolve_cross_process_layout(locator, false, false) {
        Ok(layout) => layout,
        Err(error) if matches!(
            error.current_context(),
            pi_result::ErrorKind::NotFound | pi_result::ErrorKind::Unsupported
        ) => return Ok(()),
        Err(error) => return Err(error),
    };
    let _bootstrap = try_lock_sidecar_exclusive(&layout.bootstrap_gate)?;
    if read_current_protocol_record(&layout.coordination_directory)?.is_some() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    Ok(())
}

fn binding_from_record(
    layout: CrossProcessLayout,
    record: &CrossProcessProtocolRecord,
) -> CrossProcessProtocolBinding {
    CrossProcessProtocolBinding {
        layout,
        generation: record.generation,
        target_incarnation: record.target_incarnation,
        target_identity: record.target_identity.clone(),
    }
}

fn validate_record_binding(
    record: &CrossProcessProtocolRecord,
    layout: &CrossProcessLayout,
    target_identity: &StableFileIdentity,
) -> pi_result::Result<()> {
    if record.target_identity != *target_identity
        || record.root_identity != layout.root_identity
        || record.parent_identity != layout.parent_identity
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    Ok(())
}

fn resolve_cross_process_layout(
    locator: &Path,
    allow_create: bool,
    freeze_root: bool,
) -> pi_result::Result<CrossProcessLayout> {
    let file_name = locator.file_name().ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        )
    })?;
    if file_name.is_empty() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }
    let parent = locator.parent().unwrap_or_else(|| Path::new("."));
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let physical_parent = std::fs::canonicalize(parent)
        .into_classified_error()?;
    validate_coordination_filesystem(&physical_parent)?;
    let parent_identity = stable_directory_identity(&physical_parent)?;

    let configured_root = if freeze_root {
        Some(crate::cross_process_coordination::frozen_cross_process_coordination_root())
    } else {
        crate::cross_process_coordination::configured_cross_process_coordination_root()
    };
    let root = match configured_root {
        None | Some(crate::CrossProcessCoordinationRoot::FileParent) => {
            physical_parent.clone()
        }
        Some(crate::CrossProcessCoordinationRoot::Custom(path)) => {
            let metadata = std::fs::symlink_metadata(path)
                .into_classified_error()?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Unsupported,
                ));
            }
            let physical = std::fs::canonicalize(path)
                .into_classified_error()?;
            validate_coordination_filesystem(&physical)?;
            physical
        }
    };
    let root_identity = stable_directory_identity(&root)?;

    let mut hasher = Sha256::new();
    hasher.update(b"pi-async-fs/bootstrap/v1\0");
    hasher.update(parent_identity.protocol_text().as_bytes());
    hasher.update(b"\0");
    hasher.update(native_os_string_bytes(file_name));
    let coordination_key = encode_hex(&hasher.finalize());
    let coordination_directory = root.join(format!(
        ".pi_async_fs_{coordination_key}.lock"
    ));
    if allow_create {
        match std::fs::create_dir(&coordination_directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(classified_io_error(error)),
        }
    }
    let metadata = std::fs::symlink_metadata(&coordination_directory)
        .into_classified_error()?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }

    let range_directory = coordination_directory.join("ranges");
    if allow_create {
        match std::fs::create_dir(&range_directory) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(classified_io_error(error)),
        }
    }
    let range_metadata = std::fs::symlink_metadata(&range_directory)
        .into_classified_error()?;
    if !range_metadata.is_dir() || range_metadata.file_type().is_symlink() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }

    let layout = CrossProcessLayout {
        bootstrap_gate: coordination_directory.join("bootstrap.lock"),
        lifecycle_gate: coordination_directory.join("lifecycle.lock"),
        stable_content_gate: coordination_directory.join("content.lock"),
        append_admission_gate: coordination_directory.join("append.lock"),
        range_admission_gate: coordination_directory.join("range.lock"),
        range_directory,
        coordination_directory,
        root_identity,
        parent_identity,
    };
    for gate in [
        &layout.bootstrap_gate,
        &layout.lifecycle_gate,
        &layout.stable_content_gate,
        &layout.append_admission_gate,
        &layout.range_admission_gate,
    ] {
        if allow_create {
            drop(open_sidecar_file(gate)?);
        } else {
            drop(open_existing_sidecar_file(gate)?);
        }
    }
    Ok(layout)
}

fn read_current_protocol_record(
    directory: &Path,
) -> pi_result::Result<Option<CrossProcessProtocolRecord>> {
    let mut selected: Option<(u64, PathBuf)> = None;
    for entry in std::fs::read_dir(directory).into_classified_error()? {
        let entry = entry.into_classified_error()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(sequence) = protocol_record_sequence(name)? else {
            continue;
        };
        if selected.as_ref().is_none_or(|(current, _)| sequence > *current) {
            selected = Some((sequence, entry.path()));
        }
    }
    let Some((sequence, path)) = selected else {
        return Ok(None);
    };
    reject_existing_symlink(&path)?;
    let metadata = std::fs::metadata(&path).into_classified_error()?;
    if !metadata.is_file() || metadata.len() > CROSS_PROCESS_RECORD_MAX_BYTES {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    std::fs::File::open(&path)
        .into_classified_error()?
        .take(CROSS_PROCESS_RECORD_MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .into_classified_error()?;
    if bytes.len() as u64 > CROSS_PROCESS_RECORD_MAX_BYTES {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let record = parse_protocol_record(&bytes)?;
    if record.sequence != sequence {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    Ok(Some(record))
}

fn protocol_record_sequence(name: &str) -> pi_result::Result<Option<u64>> {
    let Some(encoded) = name
        .strip_prefix("record_")
        .and_then(|name| name.strip_suffix(".state"))
    else {
        return Ok(None);
    };
    if encoded.len() != 16 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    u64::from_str_radix(encoded, 16)
        .map(Some)
        .map_err(|_| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })
}

fn publish_protocol_record(
    directory: &Path,
    record: CrossProcessProtocolRecord,
) -> pi_result::Result<CrossProcessProtocolRecord> {
    let final_path = directory.join(format!(
        "record_{:016x}.state",
        record.sequence,
    ));
    match std::fs::symlink_metadata(&final_path) {
        Ok(_) => {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(classified_io_error(error)),
    }

    let encoded = encode_protocol_record(&record);
    for _ in 0..32 {
        let temporary = directory.join(format!(
            ".record_{:016x}_{}.tmp",
            record.sequence,
            encode_hex(&random_protocol_token()),
        ));
        let mut file = match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                continue;
            }
            Err(error) => return Err(classified_io_error(error)),
        };
        let write_result = file.write_all(&encoded);
        drop(file);
        if let Err(error) = write_result {
            let _ = std::fs::remove_file(&temporary);
            return Err(classified_io_error(error));
        }
        match std::fs::rename(&temporary, &final_path) {
            Ok(()) => return Ok(record),
            Err(error) => {
                let _ = std::fs::remove_file(&temporary);
                return Err(classified_io_error(error));
            }
        }
    }
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::ResourceExhausted,
    ))
}

fn encode_protocol_record(record: &CrossProcessProtocolRecord) -> Vec<u8> {
    let identity_conflict_reason = record.identity_conflict_reason
        .as_ref()
        .map_or("none", CrossProcessIdentityConflictReason::as_str);
    let body = format!(
        "magic=pi-async-fs-cross-process\nversion={}\nsequence={:016x}\nstate={}\nidentity_conflict_reason={}\ngeneration={}\nincarnation={}\ntarget={}\nroot={}\nparent={}\n",
        CROSS_PROCESS_PROTOCOL_VERSION,
        record.sequence,
        record.state.as_str(),
        identity_conflict_reason,
        encode_hex(&record.generation),
        encode_hex(&record.target_incarnation),
        record.target_identity.protocol_text(),
        record.root_identity.protocol_text(),
        record.parent_identity.protocol_text(),
    );
    let checksum = Sha256::digest(body.as_bytes());
    format!("{body}checksum={}\n", encode_hex(&checksum)).into_bytes()
}

fn parse_protocol_record(
    bytes: &[u8],
) -> pi_result::Result<CrossProcessProtocolRecord> {
    let text = std::str::from_utf8(bytes).map_err(|_| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    let checksum_prefix = "checksum=";
    let checksum_line_start = text.rfind(checksum_prefix).ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })?;
    let body = &text[..checksum_line_start];
    let checksum_text = text[checksum_line_start + checksum_prefix.len()..]
        .strip_suffix('\n')
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })?;
    if decode_hex(checksum_text)? != Sha256::digest(body.as_bytes()).as_slice() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }

    let mut fields = HashMap::new();
    for line in body.lines() {
        let (key, value) = line.split_once('=').ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })?;
        if fields.insert(key, value).is_some() {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            ));
        }
    }
    if fields.len() != 10
        || fields.get("magic") != Some(&"pi-async-fs-cross-process")
        || fields.get("version") != Some(&"1")
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let sequence = u64::from_str_radix(required_field(&fields, "sequence")?, 16)
        .map_err(|_| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            )
        })?;
    let generation = decode_fixed_token(required_field(&fields, "generation")?)?;
    let target_incarnation = decode_fixed_token(
        required_field(&fields, "incarnation")?,
    )?;
    let state = CrossProcessProtocolState::parse(
        required_field(&fields, "state")?,
    )?;
    let identity_conflict_reason = CrossProcessIdentityConflictReason::parse(
        required_field(&fields, "identity_conflict_reason")?,
    )?;
    if matches!(state, CrossProcessProtocolState::IdentityConflict)
        != identity_conflict_reason.is_some()
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    Ok(CrossProcessProtocolRecord {
        sequence,
        state,
        identity_conflict_reason,
        generation,
        target_incarnation,
        target_identity: StableFileIdentity::parse_protocol_text(
            required_field(&fields, "target")?,
        )?,
        root_identity: StableFileIdentity::parse_protocol_text(
            required_field(&fields, "root")?,
        )?,
        parent_identity: StableFileIdentity::parse_protocol_text(
            required_field(&fields, "parent")?,
        )?,
    })
}

fn required_field<'a>(
    fields: &'a HashMap<&str, &str>,
    name: &str,
) -> pi_result::Result<&'a str> {
    fields.get(name).copied().ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })
}

fn decode_fixed_token(value: &str) -> pi_result::Result<[u8; 16]> {
    decode_hex(value)?.try_into().map_err(|_| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(value: &str) -> pi_result::Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    let mut output = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = decode_hex_digit(pair[0])?;
        let low = decode_hex_digit(pair[1])?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn decode_hex_digit(byte: u8) -> pi_result::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        )),
    }
}

fn random_protocol_token() -> [u8; 16] {
    static NEXT_NONCE: AtomicU64 = AtomicU64::new(1);

    let nonce = NEXT_NONCE.fetch_add(1, AtomicOrdering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let random_state = RandomState::new();
    let mut first = random_state.build_hasher();
    nonce.hash(&mut first);
    now.hash(&mut first);
    std::process::id().hash(&mut first);
    let first = first.finish();
    let mut second = random_state.build_hasher();
    first.hash(&mut second);
    now.rotate_left(47).hash(&mut second);
    let second = second.finish();
    let mut token = [0_u8; 16];
    token[..8].copy_from_slice(&first.to_le_bytes());
    token[8..].copy_from_slice(&second.to_le_bytes());
    token
}

#[cfg(unix)]
fn native_os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;

    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn native_os_string_bytes(value: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;

    value
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect()
}

#[cfg(not(any(unix, windows)))]
fn native_os_string_bytes(_value: &std::ffi::OsStr) -> Vec<u8> {
    Vec::new()
}

#[cfg(unix)]
fn stable_directory_identity(path: &Path) -> pi_result::Result<StableFileIdentity> {
    let metadata = std::fs::symlink_metadata(path).into_classified_error()?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    StableFileIdentity::from_std_metadata(&metadata)
}

#[cfg(windows)]
fn stable_directory_identity(path: &Path) -> pi_result::Result<StableFileIdentity> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;

    let metadata = std::fs::symlink_metadata(path).into_classified_error()?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)
        .into_classified_error()?;
    StableFileIdentity::from_std_file(&file)
}

#[cfg(not(any(unix, windows)))]
fn stable_directory_identity(_path: &Path) -> pi_result::Result<StableFileIdentity> {
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

#[cfg(target_os = "linux")]
fn validate_coordination_filesystem(path: &Path) -> pi_result::Result<()> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        )
    })?;
    let mut information = core::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `path` 是调用期间存活且以 NUL 结尾的路径；输出指针指向大小和
    // 对齐均正确的未初始化 `statfs` 存储，成功返回后由系统完整初始化。
    if unsafe { libc::statfs(path.as_ptr(), information.as_mut_ptr()) } != 0 {
        return core::result::Result::<(), _>::Err(
            std::io::Error::last_os_error(),
        )
        .into_classified_error();
    }
    // SAFETY: 上述系统调用已经成功并初始化整个结构。
    let information = unsafe { information.assume_init() };
    let file_system_type = information.f_type as u64;
    let supported = matches!(
        file_system_type,
        0x0000_ef53 // ext2/3/4
            | 0x0102_1994 // tmpfs
            | 0x5846_5342 // XFS
            | 0x9123_683e // Btrfs
            | 0x2fc1_2fc1 // ZFS
            | 0xf2f5_2010 // F2FS
    );
    if supported {
        Ok(())
    } else {
        Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ))
    }
}

#[cfg(windows)]
fn validate_coordination_filesystem(path: &Path) -> pi_result::Result<()> {
    use std::path::{Component, Prefix};

    if matches!(
        path.components().next(),
        Some(Component::Prefix(prefix)) if matches!(
            prefix.kind(),
            Prefix::UNC(_, _) | Prefix::VerbatimUNC(_, _)
        )
    ) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", windows)))]
fn validate_coordination_filesystem(_path: &Path) -> pi_result::Result<()> {
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

fn mapping_range_conflicts(
    mapping_ranges: &BTreeMap<u64, MappingRangeRegistration>,
    start: u64,
    end_exclusive: u64,
) -> bool {
    let overlaps_predecessor = mapping_ranges
        .range(..=start)
        .next_back()
        .is_some_and(|(_, mapping)| mapping.end_exclusive > start);
    let overlaps_successor = mapping_ranges
        .range(start..)
        .next()
        .is_some_and(|(mapping_start, _)| *mapping_start < end_exclusive);
    overlaps_predecessor || overlaps_successor
}

fn address_range_conflicts(
    active_views: &BTreeMap<usize, ActiveMappingView>,
    start: usize,
    end_exclusive: usize,
) -> bool {
    let overlaps_predecessor = active_views
        .range(..=start)
        .next_back()
        .is_some_and(|(_, view)| view.view_end_exclusive > start);
    let overlaps_successor = active_views
        .range(start..)
        .next()
        .is_some_and(|(view_start, _)| *view_start < end_exclusive);
    overlaps_predecessor || overlaps_successor
}

struct PendingMappingLease {
    coordination: Arc<FileCoordinationState>,
    start: u64,
    end_exclusive: u64,
    mapping_id: u64,
    active: bool,
}

impl PendingMappingLease {
    fn promote(
        mut self,
        view_start: usize,
        view_end_exclusive: usize,
    ) -> pi_result::Result<MappingLease> {
        let mut data = lock_unpoisoned(&self.coordination.data);
        let is_current_pending = data.mapping_ranges
            .get(&self.start)
            .is_some_and(|registration| {
                registration.mapping_id == self.mapping_id
                    && registration.end_exclusive == self.end_exclusive
                    && matches!(
                        registration.state,
                        MappingRangeState::Pending
                    )
            });
        if !self.active
            || !is_current_pending
            || address_range_conflicts(
                &data.active_mapping_views,
                view_start,
                view_end_exclusive,
            )
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        if let Some(registration) = data.mapping_ranges.get_mut(&self.start) {
            registration.state = MappingRangeState::Active;
        }
        data.active_mapping_views.insert(
            view_start,
            ActiveMappingView {
                view_end_exclusive,
                mapping_id: self.mapping_id,
            },
        );
        data.pending_mappings -= 1;
        self.active = false;
        Ok(MappingLease {
            coordination: Arc::clone(&self.coordination),
            start: self.start,
            view_start,
            mapping_id: self.mapping_id,
        })
    }
}

impl Drop for PendingMappingLease {
    fn drop(&mut self) {
        if self.active {
            let mut data = lock_unpoisoned(&self.coordination.data);
            let is_current_pending = data.mapping_ranges
                .get(&self.start)
                .is_some_and(|registration| {
                    registration.mapping_id == self.mapping_id
                        && matches!(
                            registration.state,
                            MappingRangeState::Pending
                        )
                });
            if is_current_pending {
                data.mapping_ranges.remove(&self.start);
                data.pending_mappings -= 1;
            }
        }
    }
}

struct MappingLease {
    coordination: Arc<FileCoordinationState>,
    start: u64,
    view_start: usize,
    mapping_id: u64,
}

impl Drop for MappingLease {
    fn drop(&mut self) {
        let mut data = lock_unpoisoned(&self.coordination.data);
        let is_current = data.mapping_ranges
            .get(&self.start)
            .is_some_and(|registration| {
                registration.mapping_id == self.mapping_id
                    && matches!(
                        registration.state,
                        MappingRangeState::Active
                    )
            });
        if is_current {
            data.mapping_ranges.remove(&self.start);
        }
        let is_current_view = data.active_mapping_views
            .get(&self.view_start)
            .is_some_and(|view| view.mapping_id == self.mapping_id);
        if is_current_view {
            data.active_mapping_views.remove(&self.view_start);
        }
    }
}

// 原生建图从这里开始已经拥有文件和待建图租约。即使公开 Future 被取消，
// 外层 `DetachOnDrop` 也会让本任务完成：失败时恢复文件并撤销预留；成功但
// 无调用方接收结果时，返回句柄会立刻析构并依次解映射、释放范围登记。
async fn finish_read_only_mapping(
    file: async_fs::File,
    mut file_guard: async_lock::MutexGuardArc<Option<async_fs::File>>,
    range: MmapRange,
    mapping_len: usize,
    pending_mapping: PendingMappingLease,
    cross_process_mapping: Option<CrossProcessMappingLease>,
) -> pi_result::Result<ReadMmapHandle> {
    let mapping_start = range.start();
    let mapping_attempt = bounded_blocking::unblock_with_input(file, move |file| {
        let mut options = memmap2::MmapOptions::new();
        options.offset(mapping_start).len(mapping_len);
        // SAFETY: 文件句柄由闭包独占拥有并覆盖整个同步建图调用；调用前已
        // 冻结长度、预留非相交逻辑范围并阻止进程内破坏性操作。未协调公开
        // 入口要求调用方另外保证所有进程外参与者不会破坏文件或映射范围。
        let result = unsafe { options.map(&file) };
        (file, result)
    })
    .await;
    let (file, mapping_result) = match mapping_attempt {
        Ok(result) => result,
        Err((error, file)) => {
            *file_guard = Some(file);
            return Err(error);
        }
    };
    *file_guard = Some(file);

    let mapping = mapping_result.into_classified_error()?;
    if mapping.len() != mapping_len {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidState,
        ));
    }
    let view_start = mapping.as_ptr() as usize;
    let view_end_exclusive = view_start.checked_add(mapping.len())
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
    let mapping_lease = pending_mapping
        .promote(view_start, view_end_exclusive)?;
    Ok(ReadMmapHandle::from_mapping(
        range,
        mapping,
        Box::new(move || {
            drop(mapping_lease);
            drop(cross_process_mapping);
        }),
    ))
}

// 可写建图同样必须由拥有型完成者把文件、待建图租约和原生结果收敛到一个
// 确定终点。取消后的成功结果无人接收时会直接析构透明句柄，不会把可写
// 地址或范围登记遗留在协调表中。
async fn finish_read_write_mapping(
    file: async_fs::File,
    mut file_guard: async_lock::MutexGuardArc<Option<async_fs::File>>,
    range: MmapRange,
    mapping_len: usize,
    pending_mapping: PendingMappingLease,
    cross_process_mapping: Option<CrossProcessMappingLease>,
) -> pi_result::Result<ReadWriteMmapHandle> {
    let mapping_start = range.start();
    let mapping_attempt = bounded_blocking::unblock_with_input(file, move |file| {
        let mut options = memmap2::MmapOptions::new();
        options.offset(mapping_start).len(mapping_len);
        // 原始映射只提供指针和刷新原语；透明句柄稍后在范围租约与独占句柄
        // 不变量下构造受限切片。文件在同步调用期间由本闭包独占拥有。
        let result = options.map_raw(&file);
        (file, result)
    })
    .await;
    let (file, mapping_result) = match mapping_attempt {
        Ok(result) => result,
        Err((error, file)) => {
            *file_guard = Some(file);
            return Err(error);
        }
    };
    *file_guard = Some(file);

    let mapping = mapping_result.into_classified_error()?;
    if mapping.len() != mapping_len {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidState,
        ));
    }
    let view_start = mapping.as_ptr() as usize;
    let view_end_exclusive = view_start.checked_add(mapping.len())
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
    let mapping_lease = pending_mapping
        .promote(view_start, view_end_exclusive)?;
    Ok(ReadWriteMmapHandle::from_mapping(
        range,
        mapping,
        Box::new(move || {
            drop(mapping_lease);
            drop(cross_process_mapping);
        }),
    ))
}

enum FileOperationLeaseKind {
    MetadataQuery,
    ContentRead,
    Append,
    Flush,
    Exclusive,
}

struct FileOperationLease {
    coordination: Arc<FileCoordinationState>,
    kind: FileOperationLeaseKind,
}

impl FileOperationLease {
    fn new(
        coordination: Arc<FileCoordinationState>,
        kind: FileOperationLeaseKind,
    ) -> Self {
        Self { coordination, kind }
    }
}

impl Drop for FileOperationLease {
    fn drop(&mut self) {
        let mut data = lock_unpoisoned(&self.coordination.data);
        match &self.kind {
            FileOperationLeaseKind::MetadataQuery => {
                if data.active_metadata_queries != 0 {
                    data.active_metadata_queries -= 1;
                }
            }
            FileOperationLeaseKind::ContentRead => {
                if data.active_content_reads != 0 {
                    data.active_content_reads -= 1;
                }
            }
            FileOperationLeaseKind::Append => {
                if data.active_appends != 0 {
                    data.active_appends -= 1;
                }
            }
            FileOperationLeaseKind::Flush => {
                if data.active_flushes != 0 {
                    data.active_flushes -= 1;
                }
            }
            FileOperationLeaseKind::Exclusive => {
                data.exclusive_operation = false;
            }
        }
    }
}

// `async-fs` 的读取会在私有管道中继续后台预读。公开读取 Future 返回或取消
// 时，必须先通过 seek 让该后台任务停止，才能释放内容租约并允许 MMAP 或
// 排它操作。guard 在正常路径显式等待归位；取消路径把锁和租约移交 actor。
struct ContentReadGuard {
    file_guard: Option<
        async_lock::MutexGuardArc<Option<async_fs::File>>,
    >,
    lease: Option<FileOperationLease>,
    cross_process_lease: Option<CrossProcessReadLease>,
}

impl ContentReadGuard {
    async fn acquire(
        file_slot: Arc<async_lock::Mutex<Option<async_fs::File>>>,
        coordination: &Arc<FileCoordinationState>,
        authority: Option<Arc<CrossProcessAuthorityCore>>,
    ) -> pi_result::Result<Self> {
        let lease = coordination.try_acquire_content_read()?;
        let file_guard = file_slot.lock_arc().await;
        if file_guard.is_none() {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidState,
            ));
        }
        let cross_process_lease = match authority {
            Some(authority) => Some(authority.acquire_read_operation().await?),
            None => None,
        };
        Ok(Self {
            file_guard: Some(file_guard),
            lease: Some(lease),
            cross_process_lease,
        })
    }

    fn file_mut(&mut self) -> pi_result::Result<&mut async_fs::File> {
        self.file_guard
            .as_mut()
            .and_then(|guard| guard.as_mut())
            .ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })
    }

    async fn quiesce(mut self) -> pi_result::Result<()> {
        let result = match self.file_mut() {
            Ok(file) => file
                .seek(SeekFrom::Current(0))
                .await
                .map(|_| ())
                .into_classified_error(),
            Err(error) => Err(error),
        };
        self.file_guard.take();
        self.lease.take();
        self.cross_process_lease.take();
        result
    }
}

impl Drop for ContentReadGuard {
    fn drop(&mut self) {
        let Some(mut file_guard) = self.file_guard.take() else {
            return;
        };
        let lease = self.lease.take();
        let cross_process_lease = self.cross_process_lease.take();
        async_global_executor::spawn(async move {
            if let Some(file) = file_guard.as_mut() {
                let _ = file.seek(SeekFrom::Current(0)).await;
            }
            drop(file_guard);
            drop(lease);
            drop(cross_process_lease);
        })
        .detach();
    }
}

impl Drop for FileCoordinationState {
    fn drop(&mut self) {
        self.registry.remove_if_current(
            &self.identity,
            self as *const FileCoordinationState,
        );
    }
}

struct FileRegistry {
    shards: [Mutex<HashMap<StableFileIdentity, FileRegistryEntry>>;
        FILE_REGISTRY_SHARDS],
}

struct FileRegistryEntry {
    coordination: Weak<FileCoordinationState>,
    cross_process_binding: Option<ManagedCrossProcessBinding>,
}

impl FileRegistry {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn shard_index(identity: &StableFileIdentity) -> usize {
        let mut hasher = DefaultHasher::new();
        identity.hash(&mut hasher);
        (hasher.finish() as usize) & (FILE_REGISTRY_SHARDS - 1)
    }

    async fn coordination_for(
        &'static self,
        identity: StableFileIdentity,
    ) -> pi_result::Result<Arc<FileCoordinationState>> {
        let shard_index = Self::shard_index(&identity);
        loop {
            let candidate_binding = {
                let shard = lock_unpoisoned(&self.shards[shard_index]);
                if let Some(existing) = shard
                    .get(&identity)
                    .and_then(|entry| entry.coordination.upgrade())
                {
                    return Ok(existing);
                }
                shard
                    .get(&identity)
                    .and_then(|entry| entry.cross_process_binding.clone())
            };

            let retain_candidate = match candidate_binding.clone() {
                Some(managed) => {
                    let expected = managed.binding.target_identity.clone();
                    let locator = managed.locator.clone();
                    matches!(
                        bounded_blocking::unblock_result(move || {
                            observe_managed_target(&locator, &expected)
                        })
                        .await?,
                        ManagedTargetObservation::Matching,
                    )
                }
                None => false,
            };

            let mut shard = lock_unpoisoned(&self.shards[shard_index]);
            if let Some(existing) = shard
                .get(&identity)
                .and_then(|entry| entry.coordination.upgrade())
            {
                return Ok(existing);
            }
            let current_binding = shard
                .get(&identity)
                .and_then(|entry| entry.cross_process_binding.clone());
            if current_binding != candidate_binding {
                continue;
            }
            let cross_process_binding = if retain_candidate {
                candidate_binding
            } else {
                None
            };
            let coordination = Arc::new(FileCoordinationState::new(
                identity.clone(),
                self,
                cross_process_binding.clone(),
            ));
            shard.insert(identity, FileRegistryEntry {
                coordination: Arc::downgrade(&coordination),
                cross_process_binding,
            });
            return Ok(coordination);
        }
    }

    fn install_cross_process_binding(
        &self,
        identity: &StableFileIdentity,
        coordination: &Arc<FileCoordinationState>,
        binding: ManagedCrossProcessBinding,
    ) {
        let shard_index = Self::shard_index(identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let entry = shard.entry(identity.clone()).or_insert_with(|| {
            FileRegistryEntry {
                coordination: Arc::downgrade(coordination),
                cross_process_binding: None,
            }
        });
        entry.coordination = Arc::downgrade(coordination);
        entry.cross_process_binding = Some(binding);
    }

    fn clear_cross_process_binding(
        &self,
        identity: &StableFileIdentity,
        coordination: &FileCoordinationState,
        binding: &CrossProcessProtocolBinding,
    ) {
        let shard_index = Self::shard_index(identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let should_remove = if let Some(entry) = shard.get_mut(identity) {
            if std::ptr::eq(entry.coordination.as_ptr(), coordination)
                && entry.cross_process_binding.as_ref().is_some_and(|managed| {
                    &managed.binding == binding
                })
            {
                entry.cross_process_binding = None;
            }
            entry.coordination.strong_count() == 0
                && entry.cross_process_binding.is_none()
        } else {
            false
        };
        if should_remove {
            shard.remove(identity);
        }
    }

    fn remove_if_current(
        &self,
        identity: &StableFileIdentity,
        state: *const FileCoordinationState,
    ) {
        let shard_index = Self::shard_index(identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let should_remove = shard.get(identity).is_some_and(|registered| {
            registered.coordination.as_ptr() == state
                && registered.cross_process_binding.is_none()
        });
        if should_remove {
            shard.remove(identity);
        }
    }
}

#[cfg(not(windows))]
struct NamespaceEntryName {
    native: OsString,
}

#[cfg(windows)]
struct NamespaceEntryName {
    units: Vec<u16>,
    case_sensitive: bool,
}

impl NamespaceEntryName {
    fn is_equivalent_to(&self, other: &Self) -> bool {
        #[cfg(not(windows))]
        {
            self.native == other.native
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::Globalization::{
                CSTR_EQUAL, CompareStringOrdinal,
            };

            if self.case_sensitive && other.case_sensitive {
                return self.units == other.units;
            }

            let left_length = i32::try_from(self.units.len());
            let right_length = i32::try_from(other.units.len());
            let (Ok(left_length), Ok(right_length)) =
                (left_length, right_length)
            else {
                // 名称长度已在构造时验证；若内部状态损坏，保守地认为冲突。
                return true;
            };
            // SAFETY: 两个 UTF-16 缓冲区在调用期间均保持存活，显式长度与
            // 对应分配一致；API 只读取缓冲区，不要求 NUL 结尾。
            unsafe {
                CompareStringOrdinal(
                    self.units.as_ptr(),
                    left_length,
                    other.units.as_ptr(),
                    right_length,
                    1,
                ) == CSTR_EQUAL
            }
        }
    }

    fn cmp_for_lock_order(&self, other: &Self) -> std::cmp::Ordering {
        #[cfg(not(windows))]
        {
            self.native.cmp(&other.native)
        }

        #[cfg(windows)]
        {
            self.case_sensitive
                .cmp(&other.case_sensitive)
                .then_with(|| self.units.cmp(&other.units))
        }
    }
}

struct NamespaceEntryKey {
    parent_identity: StableFileIdentity,
    name: NamespaceEntryName,
}

impl NamespaceEntryKey {
    fn is_equivalent_to(&self, other: &Self) -> bool {
        self.parent_identity == other.parent_identity
            && self.name.is_equivalent_to(&other.name)
    }

    fn cmp_for_lock_order(&self, other: &Self) -> std::cmp::Ordering {
        self.parent_identity
            .cmp(&other.parent_identity)
            .then_with(|| self.name.cmp_for_lock_order(&other.name))
    }
}

struct ActiveNamespaceEntry {
    reservation_id: u64,
    name: NamespaceEntryName,
}

struct NamespaceParentState {
    active_entries: Vec<ActiveNamespaceEntry>,
    retirement_id: Option<u64>,
}

struct NamespaceEntryRegistry {
    shards: [Mutex<HashMap<StableFileIdentity, NamespaceParentState>>;
        NAMESPACE_ENTRY_REGISTRY_SHARDS],
}

impl NamespaceEntryRegistry {
    fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    fn shard_index(identity: &StableFileIdentity) -> usize {
        let mut hasher = DefaultHasher::new();
        identity.hash(&mut hasher);
        (hasher.finish() as usize) & (NAMESPACE_ENTRY_REGISTRY_SHARDS - 1)
    }

    fn try_reserve(
        &'static self,
        key: NamespaceEntryKey,
        reservation_id: u64,
    ) -> pi_result::Result<NamespaceEntryReservation> {
        let shard_index = Self::shard_index(&key.parent_identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let parent = shard
            .entry(key.parent_identity.clone())
            .or_insert_with(|| NamespaceParentState {
                active_entries: Vec::new(),
                retirement_id: None,
            });
        if parent.retirement_id.is_some()
            || parent.active_entries
            .iter()
            .any(|entry| entry.name.is_equivalent_to(&key.name))
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        parent.active_entries.push(ActiveNamespaceEntry {
            reservation_id,
            name: key.name,
        });
        Ok(NamespaceEntryReservation {
            registry: self,
            parent_identity: key.parent_identity,
            reservation_id,
        })
    }

    fn try_retire(
        &'static self,
        identity: StableFileIdentity,
        reservation_id: u64,
    ) -> pi_result::Result<DirectoryRetirementReservation> {
        let shard_index = Self::shard_index(&identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let parent = shard
            .entry(identity.clone())
            .or_insert_with(|| NamespaceParentState {
                active_entries: Vec::new(),
                retirement_id: None,
            });
        if parent.retirement_id.is_some() || !parent.active_entries.is_empty() {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        parent.retirement_id = Some(reservation_id);
        Ok(DirectoryRetirementReservation {
            registry: self,
            identity,
            reservation_id,
        })
    }

    fn release(
        &self,
        parent_identity: &StableFileIdentity,
        reservation_id: u64,
    ) {
        let shard_index = Self::shard_index(parent_identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let should_remove_parent = if let Some(parent) = shard.get_mut(parent_identity) {
            if let Some(index) = parent.active_entries
                .iter()
                .position(|entry| entry.reservation_id == reservation_id)
            {
                parent.active_entries.swap_remove(index);
            }
            parent.active_entries.is_empty() && parent.retirement_id.is_none()
        } else {
            false
        };
        if should_remove_parent {
            shard.remove(parent_identity);
        }
    }

    fn release_retirement(
        &self,
        identity: &StableFileIdentity,
        reservation_id: u64,
    ) {
        let shard_index = Self::shard_index(identity);
        let mut shard = lock_unpoisoned(&self.shards[shard_index]);
        let should_remove_parent = if let Some(parent) = shard.get_mut(identity) {
            if parent.retirement_id == Some(reservation_id) {
                parent.retirement_id = None;
            }
            parent.active_entries.is_empty() && parent.retirement_id.is_none()
        } else {
            false
        };
        if should_remove_parent {
            shard.remove(identity);
        }
    }
}

struct NamespaceEntryReservation {
    registry: &'static NamespaceEntryRegistry,
    parent_identity: StableFileIdentity,
    reservation_id: u64,
}

impl Drop for NamespaceEntryReservation {
    fn drop(&mut self) {
        self.registry
            .release(&self.parent_identity, self.reservation_id);
    }
}

struct NamespaceEntryReservations {
    reservations: Vec<NamespaceEntryReservation>,
}

impl Drop for NamespaceEntryReservations {
    fn drop(&mut self) {
        while self.reservations.pop().is_some() {}
    }
}

struct DirectoryRetirementReservation {
    registry: &'static NamespaceEntryRegistry,
    identity: StableFileIdentity,
    reservation_id: u64,
}

impl Drop for DirectoryRetirementReservation {
    fn drop(&mut self) {
        self.registry
            .release_retirement(&self.identity, self.reservation_id);
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

// 同一份已加载 crate 的所有公开 namespace 句柄都引用此唯一逻辑核心。
// 稳定身份表必须留在这个核心中，不能被移动到公开句柄形成隔离副本。
struct LocalNamespaceCore {
    registry: FileRegistry,
    namespace_entries: NamespaceEntryRegistry,
    next_resource_id: AtomicU64,
}

impl LocalNamespaceCore {
    fn new() -> Self {
        Self {
            registry: FileRegistry::new(),
            namespace_entries: NamespaceEntryRegistry::new(),
            next_resource_id: AtomicU64::new(1),
        }
    }

    fn allocate_resource_id(&self) -> pi_result::Result<u64> {
        self.next_resource_id
            .fetch_update(
                AtomicOrdering::Relaxed,
                AtomicOrdering::Relaxed,
                |current| current.checked_add(1),
            )
            .map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::ResourceExhausted,
                )
            })
    }
}

static LOCAL_NAMESPACE_CORE: OnceLock<LocalNamespaceCore> = OnceLock::new();

fn parent_path_for_namespace_entry(locator: &Path) -> &Path {
    match locator.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

#[cfg(unix)]
fn namespace_entry_key_sync(
    locator: &Path,
) -> pi_result::Result<Option<NamespaceEntryKey>> {
    use std::os::unix::fs::MetadataExt;

    let Some(name) = locator.file_name() else {
        return Ok(None);
    };
    let parent_metadata = std::fs::metadata(
        parent_path_for_namespace_entry(locator),
    )
    .into_classified_error()?;
    let inode = parent_metadata.ino();
    if inode == 0 {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok(Some(NamespaceEntryKey {
        parent_identity: StableFileIdentity::Unix {
            device: parent_metadata.dev(),
            inode,
        },
        name: NamespaceEntryName {
            native: name.to_os_string(),
        },
    }))
}

#[cfg(windows)]
fn namespace_entry_key_sync(
    locator: &Path,
) -> pi_result::Result<Option<NamespaceEntryKey>> {
    use core::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{
        CloseHandle, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, FILE_CASE_SENSITIVE_INFO, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_ID_INFO, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileCaseSensitiveInfo, FileIdInfo,
        GetFileInformationByHandleEx, OPEN_EXISTING,
    };
    use windows_sys::Win32::System::SystemServices::FILE_CS_FLAG_CASE_SENSITIVE_DIR;

    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            // SAFETY: 本值只在成功的 `CreateFileW` 后构造并独占该句柄。
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    let Some(name) = locator.file_name() else {
        return Ok(None);
    };
    let name_units = name.encode_wide().collect::<Vec<_>>();
    if name_units.contains(&0) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }
    if i32::try_from(name_units.len()).is_err() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::ResourceExhausted,
        ));
    }

    let mut parent_units = parent_path_for_namespace_entry(locator)
        .as_os_str()
        .encode_wide()
        .collect::<Vec<_>>();
    if parent_units.contains(&0) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }
    parent_units.push(0);

    // SAFETY: `parent_units` 是本次调用内存活的 NUL 结尾 UTF-16 路径；空安全
    // 属性指针表示默认安全属性，其余标志只以最小属性读取权限打开目录。
    let raw_handle = unsafe {
        CreateFileW(
            parent_units.as_ptr(),
            FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            core::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            core::ptr::null_mut(),
        )
    };
    if raw_handle == INVALID_HANDLE_VALUE {
        return core::result::Result::<Option<NamespaceEntryKey>, _>::Err(
            std::io::Error::last_os_error(),
        )
        .into_classified_error();
    }
    let handle = OwnedHandle(raw_handle);

    let mut file_id = FILE_ID_INFO::default();
    // SAFETY: 输出缓冲区可写、对齐且大小准确，`handle` 在调用期间有效。
    let identity_succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileIdInfo,
            (&mut file_id as *mut FILE_ID_INFO).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if identity_succeeded == 0 {
        return core::result::Result::<Option<NamespaceEntryKey>, _>::Err(
            std::io::Error::last_os_error(),
        )
        .into_classified_error();
    }
    if file_id.VolumeSerialNumber == 0
        && file_id.FileId.Identifier.iter().all(|byte| *byte == 0)
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }

    let mut case_information = FILE_CASE_SENSITIVE_INFO::default();
    // SAFETY: 输出缓冲区可写、对齐且大小准确，`handle` 在调用期间有效。
    let case_succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle.0,
            FileCaseSensitiveInfo,
            (&mut case_information as *mut FILE_CASE_SENSITIVE_INFO).cast(),
            size_of::<FILE_CASE_SENSITIVE_INFO>() as u32,
        )
    };
    let case_sensitive = if case_succeeded != 0 {
        case_information.Flags & FILE_CS_FLAG_CASE_SENSITIVE_DIR != 0
    } else {
        let error = std::io::Error::last_os_error();
        match error.raw_os_error() {
            // 较早的 Windows 10 或不支持逐目录大小写标志的文件系统按传统
            // 大小写不敏感规则协调；其它错误不能静默降级。
            Some(1 | 50 | 87) => false,
            _ => {
                return core::result::Result::<
                    Option<NamespaceEntryKey>,
                    _,
                >::Err(error)
                .into_classified_error();
            }
        }
    };

    Ok(Some(NamespaceEntryKey {
        parent_identity: StableFileIdentity::Windows {
            volume_serial_number: file_id.VolumeSerialNumber,
            file_id: file_id.FileId.Identifier,
        },
        name: NamespaceEntryName {
            units: name_units,
            case_sensitive,
        },
    }))
}

#[cfg(not(any(unix, windows)))]
fn namespace_entry_key_sync(
    locator: &Path,
) -> pi_result::Result<Option<NamespaceEntryKey>> {
    if locator.file_name().is_none() {
        return Ok(None);
    }
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

async fn local_namespace_entry_key(
    locator: &Path,
) -> pi_result::Result<Option<NamespaceEntryKey>> {
    let locator = locator.to_path_buf();
    bounded_blocking::unblock_result(move || {
        namespace_entry_key_sync(&locator)
    })
    .await
}

async fn reserve_resolved_namespace_entry(
    core: &'static LocalNamespaceCore,
    locator: &Path,
    key: NamespaceEntryKey,
) -> pi_result::Result<NamespaceEntryReservation> {
    let expected_parent_identity = key.parent_identity.clone();
    let reservation_id = core.allocate_resource_id()?;
    let reservation = core.namespace_entries.try_reserve(key, reservation_id)?;

    // 身份查询与注册表预约之间存在一个异步调度窗口。预约建立后，同进程受管
    // 目录退役会被该记录拒绝；因此在不持有注册表互斥锁的情况下重新核验一次
    // 父目录身份，即可把窗口内已经发生的改名、删除或同名重建归类为冲突。
    // 外部绕过本库直接修改 namespace 仍不属于进程内协调保证域。
    let current_key = local_namespace_entry_key(locator).await?;
    if current_key.is_none_or(|current| {
        current.parent_identity != expected_parent_identity
    }) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }

    Ok(reservation)
}

async fn acquire_namespace_entry_reservation(
    core: &'static LocalNamespaceCore,
    locator: &Path,
) -> pi_result::Result<Option<NamespaceEntryReservation>> {
    let key = local_namespace_entry_key(locator).await?;
    let Some(key) = key else {
        return Ok(None);
    };
    reserve_resolved_namespace_entry(core, locator, key)
        .await
        .map(Some)
}

async fn acquire_namespace_entry_reservations(
    core: &'static LocalNamespaceCore,
    first: &Path,
    second: &Path,
) -> pi_result::Result<NamespaceEntryReservations> {
    let first_key = local_namespace_entry_key(first).await?.ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        )
    })?;
    let second_key = local_namespace_entry_key(second).await?.ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        )
    })?;
    if first_key.is_equivalent_to(&second_key) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }

    let mut entries = vec![
        (first.to_path_buf(), first_key),
        (second.to_path_buf(), second_key),
    ];
    entries.sort_by(|left, right| {
        left.1.cmp_for_lock_order(&right.1)
    });
    let mut reservations = Vec::with_capacity(entries.len());
    for (locator, key) in entries {
        reservations.push(
            reserve_resolved_namespace_entry(core, &locator, key).await?,
        );
    }
    Ok(NamespaceEntryReservations { reservations })
}

async fn local_directory_identity(
    locator: &Path,
) -> pi_result::Result<StableFileIdentity> {
    let identity_probe = locator.join(".pi_async_fs_identity_probe");
    let key = bounded_blocking::unblock_result(move || {
        namespace_entry_key_sync(&identity_probe)
    })
    .await?
    .ok_or_else(|| {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        )
    })?;
    Ok(key.parent_identity)
}

fn acquire_directory_retirement_reservation(
    core: &'static LocalNamespaceCore,
    identity: StableFileIdentity,
) -> pi_result::Result<DirectoryRetirementReservation> {
    let reservation_id = core.allocate_resource_id()?;
    core.namespace_entries
        .try_retire(identity, reservation_id)
}

fn classified_io_error(error: std::io::Error) -> pi_result::Error {
    match core::result::Result::<(), _>::Err(error).into_classified_error() {
        Err(error) => error,
        Ok(()) => unreachable!("an explicit I/O error cannot convert to success"),
    }
}

async fn create_local_directory(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::RawResult<(), CreateFailure> {
    let reservation = acquire_namespace_entry_reservation(core, &locator)
        .await
        .map_err(|error| {
            CreateFailure::new(
                error,
                crate::CreateTargetEvidence::NotCreatedByOperation,
            )
        })?;
    let protocol_locator = locator.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_locator)
    })
    .await
    .map_err(|error| {
        CreateFailure::new(
            error,
            crate::CreateTargetEvidence::NotCreatedByOperation,
        )
    })?;
    let result = async_fs::create_dir(&locator).await;
    drop(reservation);
    result.map_err(|error| {
        CreateFailure::new(
            classified_io_error(error),
            crate::CreateTargetEvidence::NotCreatedByOperation,
        )
    })
}

async fn create_local_directories(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::RawResult<(), CreateDirectoriesFailure<PathBuf>> {
    let mut confirmed_created = Vec::new();
    let mut levels = locator
        .ancestors()
        .filter(|level| !level.as_os_str().is_empty())
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    levels.reverse();

    for level in levels {
        match async_fs::metadata(&level).await {
            Ok(metadata) if metadata.is_dir() => continue,
            Ok(_) => {
                return Err(CreateDirectoriesFailure::new(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::AlreadyExists,
                    ),
                    confirmed_created,
                    Vec::new(),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(CreateDirectoriesFailure::new(
                    classified_io_error(error),
                    confirmed_created,
                    Vec::new(),
                ));
            }
        }

        let reservation = match acquire_namespace_entry_reservation(core, &level).await {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(CreateDirectoriesFailure::new(
                    error,
                    confirmed_created,
                    Vec::new(),
                ));
            }
        };
        let protocol_level = level.clone();
        if let Err(error) = bounded_blocking::unblock_result(move || {
            reject_persisted_cross_process_domain(&protocol_level)
        })
        .await
        {
            drop(reservation);
            return Err(CreateDirectoriesFailure::new(
                error,
                confirmed_created,
                Vec::new(),
            ));
        }
        match async_fs::create_dir(&level).await {
            Ok(()) => {
                drop(reservation);
                confirmed_created.push(level);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let verification = async_fs::metadata(&level).await;
                drop(reservation);
                match verification {
                    Ok(metadata) if metadata.is_dir() => {}
                    Ok(_) => {
                        return Err(CreateDirectoriesFailure::new(
                            classified_io_error(error),
                            confirmed_created,
                            Vec::new(),
                        ));
                    }
                    Err(verification_error) => {
                        return Err(CreateDirectoriesFailure::new(
                            classified_io_error(verification_error),
                            confirmed_created,
                            Vec::new(),
                        ));
                    }
                }
            }
            Err(error) => {
                drop(reservation);
                return Err(CreateDirectoriesFailure::new(
                    classified_io_error(error),
                    confirmed_created,
                    Vec::new(),
                ));
            }
        }
    }

    Ok(())
}

async fn create_local_file(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
    access: FileAccessMode,
) -> pi_result::RawResult<LocalFile, CreateFailure> {
    let entry_reservation = acquire_namespace_entry_reservation(core, &locator)
        .await
        .map_err(|error| {
            CreateFailure::new(
                error,
                crate::CreateTargetEvidence::NotCreatedByOperation,
            )
        })?;

    let protocol_locator = locator.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_locator)
    })
    .await
    .map_err(|error| {
        CreateFailure::new(
            error,
            crate::CreateTargetEvidence::NotCreatedByOperation,
        )
    })?;

    // Windows 会把“以普通文件方式 create_new 一个现有目录”报告为访问被拒绝，
    // 但冻结契约把任何现有目录项统一分类为 AlreadyExists。这里先取得一个可
    // 线性化的存在性观察；真正的跨进程竞态仍由下面的原子 create_new 决定。
    match async_fs::symlink_metadata(&locator).await {
        Ok(_) => {
            return Err(CreateFailure::new(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::AlreadyExists,
                ),
                crate::CreateTargetEvidence::NotCreatedByOperation,
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CreateFailure::new(
                classified_io_error(error),
                crate::CreateTargetEvidence::NotCreatedByOperation,
            ));
        }
    }

    let mut options = async_fs::OpenOptions::new();
    match &access {
        FileAccessMode::Read | FileAccessMode::ReadMmap => {
            // `OpenOptions::create_new` 要求创建句柄具有写或追加权限。首个
            // 句柄不能在创建后按路径重开，否则会丢失刚创建对象的身份锚点；
            // 对外可执行的操作仍由不可变的 `access` 角色严格收窄。
            options.read(true).write(true);
        }
        FileAccessMode::Append => {
            options.append(true);
        }
        FileAccessMode::Overwrite | FileAccessMode::Truncate => {
            options.write(true);
        }
        FileAccessMode::ReadWriteMmap => {
            options.read(true).write(true);
        }
    }
    options.create_new(true);
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        options.custom_flags(libc::O_NONBLOCK);
    }

    let file = options.open(&locator).await.map_err(|error| {
        CreateFailure::new(
            classified_io_error(error),
            crate::CreateTargetEvidence::NotCreatedByOperation,
        )
    })?;
    let post_create_failure = |error| {
        CreateFailure::new(
            error,
            crate::CreateTargetEvidence::CreatedByOperation,
        )
    };
    let metadata = file
        .metadata()
        .await
        .into_classified_error()
        .map_err(post_create_failure)?;
    if !metadata.is_file() {
        return Err(post_create_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
        ));
    }
    let (file, identity) = stable_file_identity(file, &metadata)
        .await
        .map_err(post_create_failure)?;
    let coordination = core.registry.coordination_for(identity)
        .await
        .map_err(post_create_failure)?;
    let resource_id = core
        .allocate_resource_id()
        .map_err(post_create_failure)?;
    coordination
        .register_ordinary_resource()
        .map_err(post_create_failure)?;

    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(file))),
        access,
        read_position: 0,
        coordination,
        coordinated_lease: None,
        resource_id,
    })
}

async fn create_local_file_coordinated(
    namespace: &'static LocalNamespaceCore,
    locator: PathBuf,
    access: FileAccessMode,
) -> pi_result::RawResult<
    CrossProcessCreateSuccess<crate::CrossProcessFileAuthority, LocalFile>,
    CreateFailure,
> {
    let before_create_failure = |error| {
        CreateFailure::new(
            error,
            crate::CreateTargetEvidence::NotCreatedByOperation,
        )
    };
    let entry_reservation = acquire_namespace_entry_reservation(
        namespace,
        &locator,
    )
    .await
    .map_err(before_create_failure)?;

    // 与非协调创建保持相同的跨平台分类；此检查不替代稍后的原子创建。
    match async_fs::symlink_metadata(&locator).await {
        Ok(_) => {
            return Err(before_create_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::AlreadyExists,
                ),
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(before_create_failure(classified_io_error(error)));
        }
    }
    let planning_locator = locator.clone();
    let protocol_plan = bounded_blocking::unblock_result(move || {
        prepare_protocol_for_new_target(&planning_locator)
    })
    .await
    .map_err(before_create_failure)?;

    let mut create_options = async_fs::OpenOptions::new();
    match &access {
        FileAccessMode::Read | FileAccessMode::ReadMmap => {
            create_options.read(true).write(true);
        }
        FileAccessMode::Append => {
            create_options.append(true);
        }
        FileAccessMode::Overwrite | FileAccessMode::Truncate => {
            create_options.write(true);
        }
        FileAccessMode::ReadWriteMmap => {
            create_options.read(true).write(true);
        }
    }
    create_options.create_new(true);
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        create_options.custom_flags(libc::O_NONBLOCK);
    }
    let creation_file = create_options.open(&locator).await.map_err(|error| {
        before_create_failure(classified_io_error(error))
    })?;
    let after_create_failure = |error| {
        CreateFailure::new(
            error,
            crate::CreateTargetEvidence::CreatedByOperation,
        )
    };

    let metadata = creation_file
        .metadata()
        .await
        .into_classified_error()
        .map_err(after_create_failure)?;
    if !metadata.is_file() {
        return Err(after_create_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
        ));
    }
    let (creation_file, identity) = stable_file_identity(
        creation_file,
        &metadata,
    )
        .await
        .map_err(after_create_failure)?;
    let protocol_identity = identity.clone();
    let (binding, bootstrap) = bounded_blocking::unblock_result(move || {
        publish_protocol_for_new_target(protocol_plan, protocol_identity)
    })
    .await
    .map_err(after_create_failure)?;

    let coordination = namespace.registry.coordination_for(identity.clone())
        .await
        .map_err(after_create_failure)?;
    let initialization = coordination
        .try_begin_authority_initialization()
        .map_err(after_create_failure)?;
    let namespace_identity = namespace as *const LocalNamespaceCore as usize;
    let authority = if let Some(existing) =
        coordination.existing_cross_process_core()
    {
        if !existing.matches(namespace_identity, &binding)
            && !existing.retired.load(AtomicOrdering::Acquire)
        {
            return Err(after_create_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Conflict,
                ),
            ));
        }
        if existing.matches(namespace_identity, &binding) {
            existing
        } else {
            Arc::new(CrossProcessAuthorityCore::new(
                namespace_identity,
                locator.clone(),
                binding,
                Arc::clone(&coordination),
            ))
        }
    } else {
        Arc::new(CrossProcessAuthorityCore::new(
            namespace_identity,
            locator.clone(),
            binding,
            Arc::clone(&coordination),
        ))
    };
    initialization
        .complete(&authority)
        .map_err(after_create_failure)?;
    let coordinated_lease = authority
        .acquire_active_owner()
        .await
        .map_err(after_create_failure)?;

    let mut resource_options = async_fs::OpenOptions::new();
    match &access {
        FileAccessMode::Read | FileAccessMode::ReadMmap => {
            resource_options.read(true);
        }
        FileAccessMode::Append => {
            resource_options.append(true);
        }
        FileAccessMode::Overwrite | FileAccessMode::Truncate => {
            resource_options.write(true);
        }
        FileAccessMode::ReadWriteMmap => {
            resource_options.read(true).write(true);
        }
    }
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        resource_options.custom_flags(libc::O_NONBLOCK);
    }
    let file = resource_options
        .open(&locator)
        .await
        .into_classified_error()
        .map_err(after_create_failure)?;
    let resource_metadata = file
        .metadata()
        .await
        .into_classified_error()
        .map_err(after_create_failure)?;
    if !resource_metadata.is_file() {
        return Err(after_create_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
        ));
    }
    let (file, resource_identity) = stable_file_identity(
        file,
        &resource_metadata,
    )
        .await
        .map_err(after_create_failure)?;
    if resource_identity != identity {
        return Err(after_create_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ),
        ));
    }
    let resource_id = namespace
        .allocate_resource_id()
        .map_err(after_create_failure)?;

    drop(creation_file);
    drop(bootstrap);
    drop(entry_reservation);
    let file = LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(file))),
        access,
        read_position: 0,
        coordination,
        coordinated_lease: Some(coordinated_lease),
        resource_id,
    };
    Ok(CrossProcessCreateSuccess::new(
        crate::CrossProcessFileAuthority::from_local_core(authority),
        file,
    ))
}

fn remove_failure(error: pi_result::Error) -> RemoveFailure {
    RemoveFailure::new(
        error,
        crate::RemoveTargetEvidence::NotRemovedByOperation,
    )
}

async fn open_file_identity_probe(
    locator: &Path,
) -> pi_result::Result<(async_fs::File, StableFileIdentity)> {
    let mut options = async_fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        // 外部参与者可能在类型检查后替换路径；探测打开不能因此阻塞在 FIFO。
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(locator).await.into_classified_error()?;
    let metadata = file.metadata().await.into_classified_error()?;
    if !metadata.is_file() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    let (file, identity) = stable_file_identity(file, &metadata).await?;
    Ok((file, identity))
}

async fn establish_local_cross_process_authority(
    namespace: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::Result<crate::CrossProcessFileAuthority> {
    let entry_reservation = acquire_namespace_entry_reservation(
        namespace,
        &locator,
    )
    .await?;
    let (identity_probe, identity) = open_file_identity_probe(&locator).await?;
    let coordination = namespace.registry.coordination_for(identity.clone())
        .await?;
    let initialization = coordination.try_begin_authority_initialization()?;

    let binding = match coordination.existing_cross_process_binding() {
        Some(binding) => {
            let validation_locator = locator.clone();
            let validation_binding = binding.clone();
            bounded_blocking::unblock_result(move || {
                validate_published_protocol_at(
                    &validation_locator,
                    &validation_binding,
                )
            })
            .await?;
            binding
        }
        None => {
            let protocol_locator = locator.clone();
            let protocol_identity = identity.clone();
            bounded_blocking::unblock_result(move || {
                establish_protocol_for_existing_target(
                    &protocol_locator,
                    &protocol_identity,
                )
            })
            .await?
        }
    };

    let namespace_identity = namespace as *const LocalNamespaceCore as usize;
    let authority = if let Some(existing) =
        coordination.existing_cross_process_core()
    {
        if !existing.matches(namespace_identity, &binding)
            && !existing.retired.load(AtomicOrdering::Acquire)
        {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        if existing.matches(namespace_identity, &binding) {
            existing
        } else {
            Arc::new(CrossProcessAuthorityCore::new(
                namespace_identity,
                locator,
                binding,
                Arc::clone(&coordination),
            ))
        }
    } else {
        Arc::new(CrossProcessAuthorityCore::new(
            namespace_identity,
            locator,
            binding,
            Arc::clone(&coordination),
        ))
    };
    initialization.complete(&authority)?;
    drop(identity_probe);
    drop(entry_reservation);
    Ok(crate::CrossProcessFileAuthority::from_local_core(authority))
}

async fn open_local_file_with_cross_process_authority(
    namespace: &'static LocalNamespaceCore,
    authority: Arc<CrossProcessAuthorityCore>,
    access: FileAccessMode,
) -> pi_result::Result<LocalFile> {
    let namespace_identity = namespace as *const LocalNamespaceCore as usize;
    if authority.namespace_identity != namespace_identity {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    let locator = authority.locator.clone();
    let entry_reservation = acquire_namespace_entry_reservation(
        namespace,
        &locator,
    )
    .await?;
    let coordinated_lease = authority.acquire_active_owner().await?;

    let validation_locator = locator.clone();
    let binding = authority.binding.clone();
    bounded_blocking::unblock_result(move || {
        validate_published_protocol_at(&validation_locator, &binding)
    })
    .await?;

    let mut options = async_fs::OpenOptions::new();
    match &access {
        FileAccessMode::Read | FileAccessMode::ReadMmap => {
            options.read(true);
        }
        FileAccessMode::Append => {
            options.append(true);
        }
        FileAccessMode::Overwrite | FileAccessMode::Truncate => {
            options.write(true);
        }
        FileAccessMode::ReadWriteMmap => {
            options.read(true).write(true);
        }
    }
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(&locator).await.into_classified_error()?;
    let metadata = file.metadata().await.into_classified_error()?;
    if !metadata.is_file() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    let (file, identity) = stable_file_identity(file, &metadata).await?;
    if identity != authority.binding.target_identity {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    let coordination = namespace.registry.coordination_for(identity).await?;
    if !Arc::ptr_eq(&coordination, &authority.coordination) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        ));
    }
    let resource_id = namespace.allocate_resource_id()?;
    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(file))),
        access,
        read_position: 0,
        coordination,
        coordinated_lease: Some(coordinated_lease),
        resource_id,
    })
}

async fn remove_local_file_with_cross_process_authority(
    namespace: &'static LocalNamespaceCore,
    authority: Arc<CrossProcessAuthorityCore>,
) -> pi_result::RawResult<(), RemoveFailure> {
    let namespace_identity = namespace as *const LocalNamespaceCore as usize;
    if authority.namespace_identity != namespace_identity {
        return Err(remove_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ),
        ));
    }
    let locator = authority.locator.clone();
    let entry_reservation = acquire_namespace_entry_reservation(
        namespace,
        &locator,
    )
    .await
    .map_err(remove_failure)?;
    let planning_authority = Arc::clone(&authority);
    let plan = bounded_blocking::unblock_result(move || {
        prepare_authorized_removal(planning_authority)
    })
    .await
    .map_err(remove_failure)?;
    let process_removal = authority
        .coordination
        .try_acquire_coordinated_removal(&authority)
        .map_err(remove_failure)?;
    let result = match bounded_blocking::unblock(move || {
        finish_authorized_removal(plan)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(remove_failure(error)),
    };
    drop(process_removal);
    drop(entry_reservation);
    result
}

async fn resume_local_coordinated_file_removal(
    namespace: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::RawResult<(), RemoveFailure> {
    let entry_reservation = acquire_namespace_entry_reservation(
        namespace,
        &locator,
    )
    .await
    .map_err(remove_failure)?;
    let plan = bounded_blocking::unblock_result(move || prepare_removal_recovery(locator))
        .await
        .map_err(remove_failure)?;
    let recovery_binding = binding_from_record(
        plan.layout.clone(),
        &plan.record,
    );
    let coordination = namespace
        .registry
        .coordination_for(plan.record.target_identity.clone())
        .await
        .map_err(remove_failure)?;
    let process_removal = coordination
        .try_acquire_removal_recovery()
        .map_err(remove_failure)?;
    if let Some(authority) = coordination.existing_cross_process_core() {
        authority.mark_retired();
    }
    let result = match bounded_blocking::unblock(move || {
        finish_removal_recovery(plan)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(remove_failure(error)),
    };
    if result.is_ok() {
        coordination.clear_cross_process_binding(&recovery_binding);
    }
    drop(process_removal);
    drop(entry_reservation);
    result
}

async fn remove_local_file_uncoordinated(
    locator: PathBuf,
) -> pi_result::RawResult<(), RemoveFailure> {
    let metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    let file_type = metadata.file_type();
    if file_type.is_dir() || (!file_type.is_file() && !file_type.is_symlink()) {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )));
    }
    async_fs::remove_file(locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))
}

async fn remove_local_file(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::RawResult<(), RemoveFailure> {
    let entry_reservation = acquire_namespace_entry_reservation(core, &locator)
        .await
        .map_err(remove_failure)?;
    let protocol_locator = locator.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_locator)
    })
    .await
    .map_err(remove_failure)?;
    let metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    let file_type = metadata.file_type();
    if file_type.is_dir() || (!file_type.is_file() && !file_type.is_symlink()) {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )));
    }

    if file_type.is_symlink() {
        let result = async_fs::remove_file(&locator).await;
        drop(entry_reservation);
        return result.map_err(|error| {
            remove_failure(classified_io_error(error))
        });
    }

    let (probe, identity) = open_file_identity_probe(&locator)
        .await
        .map_err(remove_failure)?;
    let coordination = core.registry.coordination_for(identity.clone())
        .await
        .map_err(remove_failure)?;
    let removal_lease = coordination
        .try_acquire_removal()
        .map_err(remove_failure)?;

    let verified_metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    if !verified_metadata.file_type().is_file() {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        )));
    }
    let (verification_probe, verified_identity) =
        open_file_identity_probe(&locator).await.map_err(remove_failure)?;
    if verified_identity != identity {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        )));
    }

    let result = async_fs::remove_file(&locator).await;
    drop(verification_probe);
    drop(probe);
    drop(removal_lease);
    drop(entry_reservation);
    result.map_err(|error| remove_failure(classified_io_error(error)))
}

async fn remove_local_directory_uncoordinated(
    locator: PathBuf,
) -> pi_result::RawResult<(), RemoveFailure> {
    let metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    if !metadata.file_type().is_dir() {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )));
    }
    async_fs::remove_dir(locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))
}

async fn remove_local_directory(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
) -> pi_result::RawResult<(), RemoveFailure> {
    let entry_reservation = acquire_namespace_entry_reservation(core, &locator)
        .await
        .map_err(remove_failure)?;
    let metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    if !metadata.file_type().is_dir() {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )));
    }

    let identity = local_directory_identity(&locator)
        .await
        .map_err(remove_failure)?;
    let retirement = acquire_directory_retirement_reservation(
        core,
        identity.clone(),
    )
    .map_err(remove_failure)?;
    let verified_metadata = async_fs::symlink_metadata(&locator)
        .await
        .map_err(|error| remove_failure(classified_io_error(error)))?;
    if !verified_metadata.file_type().is_dir() {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        )));
    }
    let verified_identity = local_directory_identity(&locator)
        .await
        .map_err(remove_failure)?;
    if verified_identity != identity {
        return Err(remove_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        )));
    }

    let result = async_fs::remove_dir(&locator).await;
    drop(retirement);
    drop(entry_reservation);
    result.map_err(|error| remove_failure(classified_io_error(error)))
}

const COPY_STAGING_CREATE_ATTEMPTS: u32 = 32;

struct LocalCopyStaging {
    path: PathBuf,
    identity: StableFileIdentity,
    file: Option<async_fs::File>,
    _exclusive_lease: FileOperationLease,
    _entry_reservation: NamespaceEntryReservation,
}

// 对一个已经取得稳定身份的复制暂存资源执行唯一的身份安全清理规则。
// `file` 若仍存在，必须先关闭；随后只有路径仍解析到 `expected_identity` 才能
// 删除。第一次删除失败后再次观察，避免把外部替换或消失误报为本库已清理。
async fn cleanup_local_copy_staging_resource(
    path: PathBuf,
    expected_identity: StableFileIdentity,
    file: Option<async_fs::File>,
) -> CopyStagingEvidence {
    drop(file);
    let current = open_file_identity_probe(&path).await;
    let Ok((probe, identity)) = current else {
        return CopyStagingEvidence::Unknown;
    };
    drop(probe);
    if identity != expected_identity {
        return CopyStagingEvidence::Unknown;
    }

    if async_fs::remove_file(&path).await.is_ok() {
        return CopyStagingEvidence::CreatedThenRemovedByOperation;
    }

    match open_file_identity_probe(&path).await {
        Ok((probe, identity)) if identity == expected_identity => {
            drop(probe);
            CopyStagingEvidence::KnownPresentAtCompletion
        }
        Ok((probe, _)) => {
            drop(probe);
            CopyStagingEvidence::Unknown
        }
        Err(_) => CopyStagingEvidence::Unknown,
    }
}

impl LocalCopyStaging {
    async fn verify_path_identity(&self) -> pi_result::Result<()> {
        let (probe, identity) = open_file_identity_probe(&self.path).await?;
        drop(probe);
        if identity != self.identity {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        Ok(())
    }

    async fn cleanup(mut self) -> CopyStagingEvidence {
        let evidence = cleanup_local_copy_staging_resource(
            self.path.clone(),
            self.identity.clone(),
            self.file.take(),
        )
        .await;
        // 路径预留和暂存排它租约必须覆盖关闭、身份复核与删除的完整过程。
        drop(self);
        evidence
    }
}

fn local_copy_failure(
    error: pi_result::Error,
    staging_evidence: CopyStagingEvidence,
) -> CopyFailure {
    CopyFailure::new(
        error,
        CopyTargetEvidence::NotPublishedByOperation,
        staging_evidence,
    )
}

fn copy_cancelled_error() -> pi_result::Error {
    pi_result::error_stack::Report::new(pi_result::ErrorKind::Cancelled)
}

fn copy_is_cancelled(cancelled: &AtomicBool) -> bool {
    cancelled.load(AtomicOrdering::Acquire)
}

fn next_copy_staging_path(
    core: &'static LocalNamespaceCore,
    destination: &Path,
    attempt: u32,
) -> pi_result::Result<PathBuf> {
    let operation_id = core.allocate_resource_id()?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let random_state = RandomState::new();
    let mut hasher = random_state.build_hasher();
    destination.hash(&mut hasher);
    std::process::id().hash(&mut hasher);
    timestamp.hash(&mut hasher);
    operation_id.hash(&mut hasher);
    attempt.hash(&mut hasher);
    let entropy = hasher.finish();
    let name = OsString::from(format!(
        ".pi_async_fs_{operation_id:016x}_{entropy:016x}.copy"
    ));
    Ok(parent_path_for_namespace_entry(destination).join(name))
}

async fn create_local_copy_staging(
    core: &'static LocalNamespaceCore,
    destination: &Path,
    cancelled: &AtomicBool,
) -> core::result::Result<
    LocalCopyStaging,
    (pi_result::Error, CopyStagingEvidence),
> {
    for attempt in 0..COPY_STAGING_CREATE_ATTEMPTS {
        if copy_is_cancelled(cancelled) {
            return Err((
                copy_cancelled_error(),
                CopyStagingEvidence::NotCreatedByOperation,
            ));
        }
        let path = next_copy_staging_path(core, destination, attempt)
            .map_err(|error| {
                (error, CopyStagingEvidence::NotCreatedByOperation)
            })?;
        let reservation = match acquire_namespace_entry_reservation(core, &path).await {
            Ok(Some(reservation)) => reservation,
            Ok(None) => {
                return Err((
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::InvalidInput,
                    ),
                    CopyStagingEvidence::NotCreatedByOperation,
                ));
            }
            Err(error)
                if error.current_context()
                    == &pi_result::ErrorKind::Conflict =>
            {
                continue;
            }
            Err(error) => {
                return Err((
                    error,
                    CopyStagingEvidence::NotCreatedByOperation,
                ));
            }
        };

        let mut options = async_fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use async_fs::unix::OpenOptionsExt;

            options.custom_flags(libc::O_NONBLOCK);
        }
        let file = match options.open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                drop(reservation);
                continue;
            }
            Err(error) => {
                return Err((
                    classified_io_error(error),
                    CopyStagingEvidence::NotCreatedByOperation,
                ));
            }
        };

        let metadata = match file.metadata().await.into_classified_error() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                drop(file);
                return Err((
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Unsupported,
                    ),
                    CopyStagingEvidence::Unknown,
                ));
            }
            Err(error) => {
                drop(file);
                return Err((error, CopyStagingEvidence::Unknown));
            }
        };
        let (file, identity) = match stable_file_identity(file, &metadata).await {
            Ok(result) => result,
            Err(error) => {
                return Err((error, CopyStagingEvidence::Unknown));
            }
        };
        let coordination = match core.registry.coordination_for(identity.clone()).await {
            Ok(coordination) => coordination,
            Err(error) => {
                let evidence = cleanup_local_copy_staging_resource(
                    path,
                    identity,
                    Some(file),
                )
                .await;
                return Err((error, evidence));
            }
        };
        let exclusive_lease = match coordination.try_acquire_staging_exclusive() {
            Ok(lease) => lease,
            Err(error) => {
                let evidence = cleanup_local_copy_staging_resource(
                    path,
                    identity,
                    Some(file),
                )
                .await;
                return Err((error, evidence));
            }
        };

        return Ok(LocalCopyStaging {
            path,
            identity,
            file: Some(file),
            _exclusive_lease: exclusive_lease,
            _entry_reservation: reservation,
        });
    }

    Err((
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::ResourceExhausted,
        ),
        CopyStagingEvidence::NotCreatedByOperation,
    ))
}

async fn transfer_local_copy_exact(
    source: &mut async_fs::File,
    staging: &mut async_fs::File,
    content_byte_len: u64,
    cancelled: &AtomicBool,
) -> pi_result::Result<()> {
    let mut buffer = vec![0_u8; READ_TRANSFER_CHUNK_BYTES];
    let mut remaining = content_byte_len;
    while remaining != 0 {
        if copy_is_cancelled(cancelled) {
            return Err(copy_cancelled_error());
        }
        let requested = usize::try_from(
            remaining.min(READ_TRANSFER_CHUNK_BYTES as u64),
        )
        .map_err(|_| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
        let read = source
            .read(&mut buffer[..requested])
            .await
            .into_classified_error()?;
        if read == 0 {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Corrupted,
            ));
        }
        if copy_is_cancelled(cancelled) {
            return Err(copy_cancelled_error());
        }
        staging
            .write_all(&buffer[..read])
            .await
            .into_classified_error()?;
        remaining -= read as u64;
    }
    staging.flush().await.into_classified_error()?;
    let staging_len = staging
        .metadata()
        .await
        .into_classified_error()?
        .len();
    if staging_len != content_byte_len {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Corrupted,
        ));
    }
    Ok(())
}

async fn copy_local_file(
    core: &'static LocalNamespaceCore,
    source: PathBuf,
    destination: PathBuf,
    cancelled: Arc<AtomicBool>,
) -> pi_result::RawResult<CopyOutcome, CopyFailure> {
    let destination_reservation = acquire_namespace_entry_reservation(
        core,
        &destination,
    )
    .await
    .map_err(|error| {
        local_copy_failure(
            error,
            CopyStagingEvidence::NotCreatedByOperation,
        )
    })?
    .ok_or_else(|| {
        local_copy_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidInput,
            ),
            CopyStagingEvidence::NotCreatedByOperation,
        )
    })?;

    let protocol_destination = destination.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_destination)
    })
    .await
    .map_err(|error| {
        local_copy_failure(
            error,
            CopyStagingEvidence::NotCreatedByOperation,
        )
    })?;

    match async_fs::symlink_metadata(&destination).await {
        Ok(_) => {
            return Err(local_copy_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::AlreadyExists,
                ),
                CopyStagingEvidence::NotCreatedByOperation,
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(local_copy_failure(
                classified_io_error(error),
                CopyStagingEvidence::NotCreatedByOperation,
            ));
        }
    }
    if copy_is_cancelled(&cancelled) {
        return Err(local_copy_failure(
            copy_cancelled_error(),
            CopyStagingEvidence::NotCreatedByOperation,
        ));
    }

    let source_reservation = acquire_namespace_entry_reservation(core, &source)
        .await
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    let protocol_source = source.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_source)
    })
    .await
    .map_err(|error| {
        local_copy_failure(
            error,
            CopyStagingEvidence::NotCreatedByOperation,
        )
    })?;
    // Windows 会在把目录当作可读普通文件打开时先返回拒绝访问；先跟随最终链接
    // 观察类型，才能把合法但不受支持的目录源稳定分类为 Unsupported。打开后仍
    // 再次从句柄核验类型，因此路径在两次观察间被替换也不会绕过对象类型合同。
    let source_path_metadata = async_fs::metadata(&source)
        .await
        .into_classified_error()
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    if !source_path_metadata.is_file() {
        return Err(local_copy_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
            CopyStagingEvidence::NotCreatedByOperation,
        ));
    }
    let mut source_options = async_fs::OpenOptions::new();
    source_options.read(true);
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;

        source_options.custom_flags(libc::O_NONBLOCK);
    }
    let source_file = source_options.open(&source).await.map_err(|error| {
        local_copy_failure(
            classified_io_error(error),
            CopyStagingEvidence::NotCreatedByOperation,
        )
    })?;
    let source_metadata = source_file
        .metadata()
        .await
        .into_classified_error()
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    if !source_metadata.is_file() {
        return Err(local_copy_failure(
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Unsupported,
            ),
            CopyStagingEvidence::NotCreatedByOperation,
        ));
    }
    let (mut source_file, source_identity) = stable_file_identity(
        source_file,
        &source_metadata,
    )
        .await
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    let source_coordination = core.registry.coordination_for(source_identity)
        .await
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    let source_lease = source_coordination
        .try_acquire_uncoordinated_content_read()
        .map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    drop(source_reservation);

    let content_byte_len = match source_file.metadata().await.into_classified_error() {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        Ok(_) => {
            return Err(local_copy_failure(
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Unsupported,
                ),
                CopyStagingEvidence::NotCreatedByOperation,
            ));
        }
        Err(error) => {
            return Err(local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            ));
        }
    };

    let mut staging = match create_local_copy_staging(
        core,
        &destination,
        &cancelled,
    )
    .await
    {
        Ok(staging) => staging,
        Err((error, evidence)) => {
            drop(source_file);
            drop(source_lease);
            drop(destination_reservation);
            return Err(local_copy_failure(error, evidence));
        }
    };
    let transfer_result = {
        let staging_file = staging.file.as_mut().ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidState,
            )
        });
        match staging_file {
            Ok(staging_file) => transfer_local_copy_exact(
                &mut source_file,
                staging_file,
                content_byte_len,
                &cancelled,
            )
            .await,
            Err(error) => Err(error),
        }
    };
    let quiesce_result = source_file
        .seek(SeekFrom::Current(0))
        .await
        .map(|_| ())
        .into_classified_error();
    drop(source_file);
    drop(source_lease);

    let transfer_result = match transfer_result {
        Ok(()) => quiesce_result,
        Err(error) => Err(error),
    };
    if let Err(error) = transfer_result {
        let staging_evidence = staging.cleanup().await;
        drop(destination_reservation);
        return Err(local_copy_failure(error, staging_evidence));
    }
    if copy_is_cancelled(&cancelled) {
        let staging_evidence = staging.cleanup().await;
        drop(destination_reservation);
        return Err(local_copy_failure(
            copy_cancelled_error(),
            staging_evidence,
        ));
    }
    if let Err(error) = staging.verify_path_identity().await {
        let staging_evidence = staging.cleanup().await;
        drop(destination_reservation);
        return Err(local_copy_failure(error, staging_evidence));
    }
    if copy_is_cancelled(&cancelled) {
        let staging_evidence = staging.cleanup().await;
        drop(destination_reservation);
        return Err(local_copy_failure(
            copy_cancelled_error(),
            staging_evidence,
        ));
    }

    staging.file.take();
    let staging_path = staging.path.clone();
    let publish_destination = destination.clone();
    let publish_result = bounded_blocking::unblock(move || {
        native_rename_no_replace(&staging_path, &publish_destination)
    })
    .await;
    match publish_result {
        Ok(Ok(())) => {
            drop(staging);
            drop(destination_reservation);
            Ok(CopyOutcome::new(content_byte_len))
        }
        Ok(Err(error)) => {
            let error = classified_rename_io_error(error);
            let staging_evidence = staging.cleanup().await;
            drop(destination_reservation);
            Err(local_copy_failure(error, staging_evidence))
        }
        Err(error) => {
            let staging_evidence = staging.cleanup().await;
            drop(destination_reservation);
            Err(local_copy_failure(error, staging_evidence))
        }
    }
}

fn rename_failure(error: pi_result::Error) -> RenameFailure {
    RenameFailure::new(
        error,
        crate::RenameCommitEvidence::NotRenamedByOperation,
    )
}

fn classified_rename_io_error(error: std::io::Error) -> pi_result::Error {
    let is_unsupported = matches!(
        error.kind(),
        std::io::ErrorKind::Unsupported | std::io::ErrorKind::CrossesDevices
    ) || {
        #[cfg(target_os = "linux")]
        {
            matches!(
                error.raw_os_error(),
                Some(libc::EXDEV | libc::ENOSYS | libc::EOPNOTSUPP)
            )
        }
        #[cfg(windows)]
        {
            error.raw_os_error() == Some(17)
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        {
            false
        }
    };
    if is_unsupported {
        pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )
    } else {
        classified_io_error(error)
    }
}

#[cfg(target_os = "linux")]
fn native_rename_no_replace(
    source: &Path,
    destination: &Path,
) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        rustix::fs::CWD,
        source,
        rustix::fs::CWD,
        destination,
        rustix::fs::RenameFlags::NOREPLACE,
    )
    .map_err(std::io::Error::from)
}

#[cfg(windows)]
fn native_rename_no_replace(
    source: &Path,
    destination: &Path,
) -> std::io::Result<()> {
    use core::mem::size_of;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{
        CloseHandle, HANDLE, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        CreateFileW, DELETE, FILE_FLAG_BACKUP_SEMANTICS,
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_RENAME_INFO,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileRenameInfo,
        OPEN_EXISTING, SetFileInformationByHandle,
    };

    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            // SAFETY: 本值只包装成功打开且未转移所有权的唯一原生句柄。
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    fn wide_path(path: &Path) -> std::io::Result<Vec<u16>> {
        let mut units = path.as_os_str().encode_wide().collect::<Vec<_>>();
        if units.contains(&0) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path contains an embedded NUL",
            ));
        }
        units.push(0);
        Ok(units)
    }

    fn open_handle(
        path: &Path,
        access: u32,
        flags: u32,
    ) -> std::io::Result<OwnedHandle> {
        let units = wide_path(path)?;
        // SAFETY: `units` 是调用期间存活的 NUL 结尾 UTF-16 路径；默认安全
        // 属性和模板句柄为空，其余常量构成一个只拥有当前返回句柄的打开。
        let handle = unsafe {
            CreateFileW(
                units.as_ptr(),
                access,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                core::ptr::null(),
                OPEN_EXISTING,
                flags,
                core::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(OwnedHandle(handle))
        }
    }

    let destination_units = destination.as_os_str().encode_wide().collect::<Vec<_>>();
    if destination_units.is_empty() || destination_units.contains(&0) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "destination path is invalid",
        ));
    }
    let file_name_bytes = destination_units
        .len()
        .checked_mul(size_of::<u16>())
        .and_then(|length| u32::try_from(length).ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "destination path is too long",
            )
        })?;
    // Windows 要求交给 SetFileInformationByHandle 的缓冲区至少覆盖一个完整
    // FILE_RENAME_INFO，再附加 FileNameLength 个名称字节；字段偏移只用于定位
    // 实际写入起点，不能代替系统调用的缓冲区长度合同。
    let information_bytes = size_of::<FILE_RENAME_INFO>()
        .checked_add(file_name_bytes as usize)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rename information size overflow",
            )
        })?;
    let information_length = u32::try_from(information_bytes).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "rename information is too large",
        )
    })?;
    let words = information_bytes
        .checked_add(size_of::<usize>() - 1)
        .map(|length| length / size_of::<usize>())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "rename information allocation overflow",
            )
        })?;
    let mut information = vec![0_usize; words];
    let information_ptr = information.as_mut_ptr().cast::<FILE_RENAME_INFO>();

    let source_handle = open_handle(
        source,
        DELETE,
        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
    )?;
    // SAFETY: `information` 按 `usize` 对齐且容量覆盖头部和全部 UTF-16 名称；
    // 所有字段写入均在已分配范围内，源句柄在系统调用返回前保持有效。
    let succeeded = unsafe {
        (*information_ptr).Anonymous.ReplaceIfExists = false;
        (*information_ptr).RootDirectory = core::ptr::null_mut();
        (*information_ptr).FileNameLength = file_name_bytes;
        core::ptr::copy_nonoverlapping(
            destination_units.as_ptr(),
            core::ptr::addr_of_mut!((*information_ptr).FileName).cast(),
            destination_units.len(),
        );
        SetFileInformationByHandle(
            source_handle.0,
            FileRenameInfo,
            information_ptr.cast(),
            information_length,
        )
    };
    if succeeded == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
fn native_rename_no_replace(
    _source: &Path,
    _destination: &Path,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "strict no-replace rename is unsupported on this target",
    ))
}

fn supports_local_rename_type(metadata: &Metadata) -> bool {
    let file_type = metadata.file_type();
    if !file_type.is_file() && !file_type.is_dir() && !file_type.is_symlink() {
        return false;
    }

    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
            && !file_type.is_symlink()
        {
            return false;
        }
    }

    true
}

async fn rename_local_entry(
    core: &'static LocalNamespaceCore,
    source: PathBuf,
    destination: PathBuf,
) -> pi_result::RawResult<(), RenameFailure> {
    let entry_reservations = acquire_namespace_entry_reservations(
        core,
        &source,
        &destination,
    )
    .await
    .map_err(rename_failure)?;
    // 普通改名不能迁移持久协调域，也不能占用已退役但仍受协议保护的位置；
    // 两个检查都发生在目录项许可内、任何原生改名副作用之前。
    let protocol_source = source.clone();
    let protocol_destination = destination.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_source)?;
        reject_persisted_cross_process_domain(&protocol_destination)
    })
    .await
    .map_err(rename_failure)?;
    let source_metadata = async_fs::symlink_metadata(&source)
        .await
        .map_err(|error| rename_failure(classified_io_error(error)))?;
    if !supports_local_rename_type(&source_metadata) {
        return Err(rename_failure(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        )));
    }
    match async_fs::symlink_metadata(&destination).await {
        Ok(_) => {
            return Err(rename_failure(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::AlreadyExists,
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(rename_failure(classified_io_error(error)));
        }
    }

    let directory_retirement = if source_metadata.file_type().is_dir() {
        let canonical_source = async_fs::canonicalize(&source)
            .await
            .map_err(|error| rename_failure(classified_io_error(error)))?;
        let canonical_destination_parent = async_fs::canonicalize(
            parent_path_for_namespace_entry(&destination),
        )
        .await
        .map_err(|error| rename_failure(classified_io_error(error)))?;
        if canonical_destination_parent == canonical_source
            || canonical_destination_parent.starts_with(&canonical_source)
        {
            return Err(rename_failure(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidInput,
            )));
        }

        let identity = local_directory_identity(&source)
            .await
            .map_err(rename_failure)?;
        let retirement = acquire_directory_retirement_reservation(
            core,
            identity.clone(),
        )
        .map_err(rename_failure)?;
        let verified_metadata = async_fs::symlink_metadata(&source)
            .await
            .map_err(|error| rename_failure(classified_io_error(error)))?;
        if !verified_metadata.file_type().is_dir()
            || local_directory_identity(&source)
                .await
                .map_err(rename_failure)?
                != identity
        {
            return Err(rename_failure(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            )));
        }
        Some(retirement)
    } else {
        None
    };

    match async_fs::symlink_metadata(&destination).await {
        Ok(_) => {
            return Err(rename_failure(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::AlreadyExists,
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(rename_failure(classified_io_error(error)));
        }
    }

    let result = bounded_blocking::unblock(move || {
        native_rename_no_replace(&source, &destination)
    })
    .await;
    drop(directory_retirement);
    drop(entry_reservations);
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            Err(rename_failure(classified_rename_io_error(error)))
        }
        Err(error) => Err(rename_failure(error)),
    }
}

fn classify_native_file_type(native_type: &std::fs::FileType) -> FileType {
    if native_type.is_file() {
        return FileType::RegularFile;
    }
    if native_type.is_dir() {
        return FileType::Directory;
    }
    if native_type.is_symlink() {
        return FileType::SymbolicLink;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt;

        if native_type.is_block_device() {
            return FileType::BlockDevice;
        }
        if native_type.is_char_device() {
            return FileType::CharacterDevice;
        }
        if native_type.is_fifo() {
            return FileType::Fifo;
        }
        if native_type.is_socket() {
            return FileType::Socket;
        }
    }

    FileType::Other
}

fn classify_capability_file_type(
    native_type: &cap_std::fs::FileType,
) -> FileType {
    if native_type.is_file() {
        return FileType::RegularFile;
    }
    if native_type.is_dir() {
        return FileType::Directory;
    }
    if native_type.is_symlink() {
        return FileType::SymbolicLink;
    }
    if cap_fs_ext::FileTypeExt::is_block_device(native_type) {
        return FileType::BlockDevice;
    }
    if cap_fs_ext::FileTypeExt::is_char_device(native_type) {
        return FileType::CharacterDevice;
    }
    if cap_fs_ext::FileTypeExt::is_fifo(native_type) {
        return FileType::Fifo;
    }
    if cap_fs_ext::FileTypeExt::is_socket(native_type) {
        return FileType::Socket;
    }

    FileType::Other
}

fn classify_local_file_type(metadata: &Metadata) -> FileType {
    classify_native_file_type(&metadata.file_type())
}

#[cfg(unix)]
fn system_time_from_unix_timestamp(
    seconds: i64,
    nanoseconds: i64,
) -> Option<SystemTime> {
    let nanoseconds = u32::try_from(nanoseconds).ok()?;
    if nanoseconds >= 1_000_000_000 {
        return None;
    }

    if seconds >= 0 {
        return UNIX_EPOCH.checked_add(Duration::new(
            u64::try_from(seconds).ok()?,
            nanoseconds,
        ));
    }

    // Unix timespec 把负时间规范化为负秒数与非负纳秒数。转换为
    // `UNIX_EPOCH - Duration` 时必须反向借一秒，避免把 -0.5 秒写成 -1.5 秒。
    let second_magnitude = if nanoseconds == 0 {
        -i128::from(seconds)
    } else {
        -i128::from(seconds) - 1
    };
    let nanosecond_magnitude = if nanoseconds == 0 {
        0
    } else {
        1_000_000_000 - nanoseconds
    };
    UNIX_EPOCH.checked_sub(Duration::new(
        u64::try_from(second_magnitude).ok()?,
        nanosecond_magnitude,
    ))
}

fn project_local_metadata(metadata: Metadata) -> FileMetadata<PathBuf> {
    let file_type = classify_local_file_type(&metadata);
    let byte_len = if matches!(&file_type, FileType::RegularFile) {
        Some(metadata.len())
    } else {
        None
    };
    let read_only = Some(metadata.permissions().readonly());

    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;

        matches!(&file_type, FileType::RegularFile)
            .then(|| metadata.permissions().mode() & 0o111 != 0)
    };
    #[cfg(not(unix))]
    let executable = None;

    #[cfg(unix)]
    let metadata_changed = {
        use std::os::unix::fs::MetadataExt;

        system_time_from_unix_timestamp(metadata.ctime(), metadata.ctime_nsec())
    };
    #[cfg(not(unix))]
    let metadata_changed = None;

    FileMetadata::new(
        file_type,
        byte_len,
        PortablePermissions {
            read_only,
            executable,
        },
        FileTimes {
            created: metadata.created().ok(),
            modified: metadata.modified().ok(),
            accessed: metadata.accessed().ok(),
            metadata_changed,
        },
        None,
    )
}

async fn validate_local_not_found_path(
    locator: &Path,
    original_error: std::io::Error,
) -> core::result::Result<std::io::Error, pi_result::Error> {
    // Windows 会把“中间分量是普通文件”和“路径确实缺失”都折叠成
    // ErrorKind::NotFound。只在这个失败慢路径向上寻找最近的现存父项：现存
    // 非目录或悬空链接证明查询不可回答为“不存在”；现存目录则证明此前至少
    // 有一个更近分量确实缺失。原始错误被保留，避免丢失底层诊断信息。
    let mut ancestor = locator.parent();
    while let Some(path) = ancestor {
        match async_fs::metadata(path).await {
            Ok(metadata) if metadata.is_dir() => {
                return Ok(original_error);
            }
            Ok(_) => {
                return Err(classified_io_error(original_error)
                    .change_context(pi_result::ErrorKind::Io));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match async_fs::symlink_metadata(path).await {
                    Ok(metadata) if metadata.is_dir() => {
                        return Ok(original_error);
                    }
                    Ok(_) => {
                        return Err(classified_io_error(original_error)
                            .change_context(pi_result::ErrorKind::Io));
                    }
                    Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(classified_io_error(error)),
                }
            }
            Err(error) => return Err(classified_io_error(error)),
        }
        ancestor = path.parent();
    }
    Ok(original_error)
}

fn validate_available_space_target(
    locator: &PathBuf,
) -> pi_result::Result<Metadata> {
    let metadata = std::fs::metadata(locator).into_classified_error()?;
    if !metadata.is_file() && !metadata.is_dir() {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok(metadata)
}

#[cfg(target_os = "linux")]
fn query_local_available_space(
    locator: &PathBuf,
) -> pi_result::Result<AvailableSpace> {
    validate_available_space_target(locator)?;
    let statistics = rustix::fs::statvfs(locator)
        .map_err(std::io::Error::from)
        .into_classified_error()?;
    let available_bytes = statistics
        .f_frsize
        .checked_mul(statistics.f_bavail)
        .ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::ResourceExhausted,
            )
        })?;
    Ok(AvailableSpace::new(available_bytes))
}

#[cfg(windows)]
fn query_local_available_space(
    locator: &PathBuf,
) -> pi_result::Result<AvailableSpace> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;

    let metadata = validate_available_space_target(locator)?;
    let canonical = std::fs::canonicalize(locator).into_classified_error()?;
    let query_directory = if metadata.is_dir() {
        canonical
    } else {
        canonical.parent().map(PathBuf::from).ok_or_else(|| {
            pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidInput,
            )
        })?
    };
    let mut wide: Vec<u16> = query_directory.as_os_str().encode_wide().collect();
    if wide.contains(&0) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::InvalidInput,
        ));
    }
    if !matches!(
        wide.last(),
        Some(value) if *value == b'\\' as u16 || *value == b'/' as u16
    ) {
        wide.push(b'\\' as u16);
    }
    wide.push(0);

    let mut available_bytes = 0_u64;
    // SAFETY: `wide` 是以 NUL 结尾且本次调用期间保持存活的 UTF-16 目录路径；
    // 唯一非空输出指针指向已初始化、可写且大小准确的 `u64`。
    let succeeded = unsafe {
        GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut available_bytes,
            core::ptr::null_mut(),
            core::ptr::null_mut(),
        )
    };
    if succeeded == 0 {
        return core::result::Result::<AvailableSpace, _>::Err(
            std::io::Error::last_os_error(),
        )
        .into_classified_error();
    }
    Ok(AvailableSpace::new(available_bytes))
}

#[cfg(not(any(target_os = "linux", windows)))]
fn query_local_available_space(
    _locator: &PathBuf,
) -> pi_result::Result<AvailableSpace> {
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

#[cfg(unix)]
async fn stable_file_identity(
    file: async_fs::File,
    metadata: &Metadata,
) -> pi_result::Result<(async_fs::File, StableFileIdentity)> {
    use std::os::unix::fs::MetadataExt;

    let inode = metadata.ino();
    if inode == 0 {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok((
        file,
        StableFileIdentity::Unix {
            device: metadata.dev(),
            inode,
        },
    ))
}

#[cfg(windows)]
async fn stable_file_identity(
    file: async_fs::File,
    _metadata: &Metadata,
) -> pi_result::Result<(async_fs::File, StableFileIdentity)> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };

    let (file, identity) = match bounded_blocking::unblock_with_input(
        file,
        move |file| {
            let raw_handle = file.as_raw_handle() as usize;
            let mut information = FILE_ID_INFO::default();
            // SAFETY: 阻塞闭包拥有 `file`，即使外层 Future 被取消也会把原生
            // 句柄保持到调用完成；输出缓冲区对齐、长度准确且只由本次调用写入。
            let succeeded = unsafe {
                GetFileInformationByHandleEx(
                    raw_handle as windows_sys::Win32::Foundation::HANDLE,
                    FileIdInfo,
                    (&mut information as *mut FILE_ID_INFO).cast(),
                    size_of::<FILE_ID_INFO>() as u32,
                )
            };
            let identity = if succeeded == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok((
                    information.VolumeSerialNumber,
                    information.FileId.Identifier,
                ))
            };
            (file, identity)
        },
    )
    .await
    {
        Ok((file, identity)) => (file, identity.into_classified_error()?),
        Err((error, _file)) => return Err(error),
    };

    if identity.0 == 0 && identity.1.iter().all(|byte| *byte == 0) {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok((
        file,
        StableFileIdentity::Windows {
            volume_serial_number: identity.0,
            file_id: identity.1,
        },
    ))
}

#[cfg(not(any(unix, windows)))]
async fn stable_file_identity(
    _file: async_fs::File,
    _metadata: &Metadata,
) -> pi_result::Result<(async_fs::File, StableFileIdentity)> {
    Err(pi_result::error_stack::Report::new(
        pi_result::ErrorKind::Unsupported,
    ))
}

#[cfg(all(test, windows))]
mod stable_file_identity_tests {
    use super::*;

    #[test]
    // Windows 身份查询必须消费并返还拥有型文件，不能只把借用得到的裸句柄
    // 交给取消后仍可能继续运行的阻塞任务。
    fn test_windows_stable_file_identity_returns_the_owned_file() {
        futures_lite::future::block_on(async {
            let temporary = tempfile::tempdir().expect("temporary directory");
            let path = temporary.path().join("identity-target");
            std::fs::write(&path, b"identity").expect("identity target");
            let file = async_fs::File::open(&path).await.expect("open target");
            let metadata = file.metadata().await.expect("target metadata");

            let (file, identity) = stable_file_identity(file, &metadata)
                .await
                .expect("stable identity");
            assert!(matches!(identity, StableFileIdentity::Windows { .. }));
            assert_eq!(file.metadata().await.expect("returned file").len(), 8);
        });
    }
}

/// 当前进程内本地文件命名空间的共享句柄。
///
/// 同一份已加载 crate 创建或克隆的所有句柄都属于同一个进程内协调域；它们
/// 必须共同遵守针对同一底层文件的操作互斥和内存映射范围规则。克隆句柄不会
/// 克隆已经打开的文件资源，也不会建立可以绕过协调规则的新命名空间。
///
/// 本类型可以在线程间移动和共享。其它进程、另一份独立加载的 crate 或绕过
/// 本库取得的原生文件句柄不属于该进程内协调域。
///
/// # 生命周期与并发
///
/// 句柄实现 `Clone + Send + Sync + 'static`；克隆只创建同一逻辑命名空间的
/// 另一个入口，不复制任何已打开文件。各异步方法返回的 Future 不绑定特定
/// 异步运行时，并按对应 [`FileNamespace`] 方法声明的生命周期借用或拥有参数。
/// 放弃等待某个已经提交的文件操作，不应被解释为底层副作用已经取消。
///
/// # 副作用、幂等性与性能
///
/// 构造或克隆句柄本身不访问路径、不创建文件或目录，也不调用用户代码；首次
/// 构造不会产生文件系统副作用。就文件系统可观察结果而言，重复构造是幂等的。
/// 构造与克隆的时间、额外空间均为 `O(1)`，不会按文件数量增长；具体文件操作
/// 的成本、幂等性和错误语义由 [`FileNamespace`] 的相应方法说明。
///
/// # 安全边界
///
/// 合法使用安全方法不会产生未定义行为。名称中带 `uncoordinated` 的操作以及
/// `unsafe` 协调入口具有额外的跨进程前置条件，必须逐项遵守 trait 文档；本
/// 句柄的 `Send + Sync` 不会自动把库外句柄或不合作进程纳入保护范围。
///
/// # 示例
///
/// ```
/// use pi_async_fs::LocalFileNamespace;
///
/// let namespace = LocalFileNamespace::new();
/// let worker_namespace = namespace.clone();
/// std::thread::spawn(move || drop(worker_namespace))
///     .join()
///     .expect("namespace handle may cross threads");
/// ```
pub struct LocalFileNamespace {
    core: &'static LocalNamespaceCore,
}

// `Default` 不是本类型的冻结公开合同；调用方应显式用 `new` 表达取得命名空间。
#[allow(clippy::new_without_default)]
impl LocalFileNamespace {
    /// 取得当前进程内本地文件命名空间的一个共享句柄。
    ///
    /// 重复调用会取得同一逻辑协调域的另一个句柄。该操作不访问文件系统，
    /// 不打开文件，也不会让后续操作脱离同进程内统一的并发语义。
    ///
    /// 本函数没有入参，返回拥有型、可跨线程共享的句柄；返回值不借用调用方
    /// 状态。合法调用没有业务错误返回和已定义的 panic 条件，不执行用户回调，
    /// 不进入调用方异步运行时，也不保留文件资源或调用方借用。
    ///
    /// 时间复杂度和额外空间复杂度均为 `O(1)`。重复调用在文件系统副作用上
    /// 幂等，但每次都会返回一个具有独立 Rust 所有权的句柄值。
    #[must_use]
    pub fn new() -> Self {
        Self {
            core: LOCAL_NAMESPACE_CORE.get_or_init(LocalNamespaceCore::new),
        }
    }
}

impl Clone for LocalFileNamespace {
    /// 克隆同一逻辑协调域的轻量句柄。
    fn clone(&self) -> Self {
        Self { core: self.core }
    }
}

/// 由本地文件命名空间独立打开的、不透明文件资源。
///
/// 本类型只能通过本地命名空间的文件创建或打开操作获得。它可以在线程间移动
/// 和共享，但不会提供 `Clone`：同一文件需要多个操作通道时，调用方必须分别
/// 打开独立资源，使每个写入或追加通道拥有明确的资源生命周期。
///
/// 文件资源的访问角色在创建或打开时固定；不符合该角色的方法会返回明确错误，
/// 不会隐式升级权限。释放资源只释放这个值拥有的资产，不会隐式刷新、截断、
/// 删除或关闭其它独立资源。各项内容操作、取消、并发和持久化
/// 语义见 [`FileIo`]。
pub struct LocalFile {
    file: Arc<async_lock::Mutex<Option<async_fs::File>>>,
    access: FileAccessMode,
    read_position: u64,
    coordination: Arc<FileCoordinationState>,
    coordinated_lease: Option<CoordinatedResourceLease>,
    resource_id: u64,
}

impl LocalFile {
    fn cross_process_authority(
        &self,
    ) -> Option<Arc<CrossProcessAuthorityCore>> {
        self.coordinated_lease.as_ref().map(|lease| {
            Arc::clone(&lease.authority)
        })
    }
}

impl Drop for LocalFile {
    fn drop(&mut self) {
        if self.coordinated_lease.is_none() {
            self.coordination.unregister_ordinary_resource();
        }
    }
}

// 实现显式保持 trait 冻结的 `impl Future + Send` 形状及生命周期；改写成
// `async fn` 会模糊这里需要逐项核对的返回 Future 边界。
#[allow(clippy::manual_async_fn)]
impl FileIo for LocalFile {
    fn byte_len(
        &self,
    ) -> impl Future<Output = pi_result::Result<u64>> + Send + '_ {
        async move {
            let _lease = self.coordination.try_acquire_metadata_query()?;
            let _cross_process_lease = match self.cross_process_authority() {
                Some(authority) => {
                    Some(authority.acquire_shared_content_operation().await?)
                }
                None => None,
            };
            let file_slot = self.file.lock().await;
            let file = file_slot.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let metadata = file.metadata().await.into_classified_error()?;
            Ok(metadata.len())
        }
    }

    fn read_at<'a, B>(
        &'a self,
        offset: u64,
        buffer: &'a mut B,
        target: ReadTargetRegion,
    ) -> impl Future<Output = pi_result::Result<usize>> + Send + 'a
    where
        B: AsMut<[u8]> + Send + ?Sized + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let buffer = buffer.as_mut();
            let target = target
                .resolve(buffer.len())
                .into_classified_error()?;
            let source_len = u64::try_from(target.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if offset.checked_add(source_len).is_none() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if target.is_empty() {
                return Ok(0);
            }
            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(offset))
                .await
                .into_classified_error()?;
            let read = read_guard.file_mut()?.read(&mut buffer[target])
                .await
                .into_classified_error()?;
            read_guard.quiesce().await?;
            Ok(read)
        }
    }

    fn read_exact_at<'a, B>(
        &'a self,
        offset: u64,
        buffer: &'a mut B,
        target: ReadTargetRegion,
    ) -> impl Future<Output = pi_result::Result<()>> + Send + 'a
    where
        B: AsMut<[u8]> + Send + ?Sized + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let buffer = buffer.as_mut();
            let target = target
                .resolve(buffer.len())
                .into_classified_error()?;
            let source_len = u64::try_from(target.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if offset.checked_add(source_len).is_none() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if target.is_empty() {
                return Ok(());
            }

            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(offset))
                .await
                .into_classified_error()?;
            let mut cursor = target.start;
            while cursor < target.end {
                let read = read_guard.file_mut()?
                    .read(&mut buffer[cursor..target.end])
                    .await
                    .into_classified_error()?;
                if read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "file ended before the exact read target was filled",
                    ))
                    .into_classified_error();
                }
                cursor += read;
            }
            read_guard.quiesce().await?;
            Ok(())
        }
    }

    fn read_to_end_at<'a, B>(
        &'a self,
        offset: u64,
        buffer: &'a mut B,
        limit: ReadGrowthLimit,
    ) -> impl Future<Output = pi_result::Result<ReadGrowthOutcome>> + Send + 'a
    where
        B: GrowableReadBuffer + Send + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let current_len = buffer.as_ref().len();
            limit
                .checked_final_len(current_len)
                .into_classified_error()?;
            let maximum = limit.max_additional_bytes();
            let source_len = u64::try_from(maximum).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if offset.checked_add(source_len).is_none() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if maximum == 0 {
                return Ok(ReadGrowthOutcome::LimitReached {
                    appended_bytes: 0,
                });
            }

            let chunk_len = maximum.min(READ_TRANSFER_CHUNK_BYTES);
            let mut transfer = vec![0_u8; chunk_len];
            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(offset))
                .await
                .into_classified_error()?;
            let mut appended = 0;
            let outcome = loop {
                if appended == maximum {
                    break ReadGrowthOutcome::LimitReached {
                        appended_bytes: appended,
                    };
                }
                let request = (maximum - appended).min(transfer.len());
                let read = read_guard.file_mut()?
                    .read(&mut transfer[..request])
                    .await
                    .into_classified_error()?;
                if read == 0 {
                    break ReadGrowthOutcome::EndOfFile {
                        appended_bytes: appended,
                    };
                }
                buffer.put_slice(&transfer[..read]);
                appended += read;
            };
            read_guard.quiesce().await?;
            Ok(outcome)
        }
    }

    fn read<'a, B>(
        &'a mut self,
        buffer: &'a mut B,
        target: ReadTargetRegion,
    ) -> impl Future<Output = pi_result::Result<usize>> + Send + 'a
    where
        B: AsMut<[u8]> + Send + ?Sized + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let buffer = buffer.as_mut();
            let target = target
                .resolve(buffer.len())
                .into_classified_error()?;
            let source_len = u64::try_from(target.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if self.read_position.checked_add(source_len).is_none() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if target.is_empty() {
                return Ok(0);
            }

            let source_start = self.read_position;
            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(source_start))
                .await
                .into_classified_error()?;
            let read = read_guard.file_mut()?
                .read(&mut buffer[target])
                .await
                .into_classified_error()?;
            read_guard.quiesce().await?;
            let read_u64 = u64::try_from(read).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Internal,
                )
            })?;
            self.read_position = source_start.checked_add(read_u64)
                .ok_or_else(|| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Internal,
                    )
                })?;
            Ok(read)
        }
    }

    fn read_exact<'a, B>(
        &'a mut self,
        buffer: &'a mut B,
        target: ReadTargetRegion,
    ) -> impl Future<Output = pi_result::Result<()>> + Send + 'a
    where
        B: AsMut<[u8]> + Send + ?Sized + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let buffer = buffer.as_mut();
            let target = target
                .resolve(buffer.len())
                .into_classified_error()?;
            let source_len = u64::try_from(target.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            let source_end = self.read_position.checked_add(source_len)
                .ok_or_else(|| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::InvalidInput,
                    )
                })?;
            if target.is_empty() {
                return Ok(());
            }

            let source_start = self.read_position;
            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(source_start))
                .await
                .into_classified_error()?;
            let mut cursor = target.start;
            while cursor < target.end {
                let read = read_guard.file_mut()?
                    .read(&mut buffer[cursor..target.end])
                    .await
                    .into_classified_error()?;
                if read == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "file ended before the exact sequential read target was filled",
                    ))
                    .into_classified_error();
                }
                cursor += read;
            }
            read_guard.quiesce().await?;
            self.read_position = source_end;
            Ok(())
        }
    }

    fn read_to_end<'a, B>(
        &'a mut self,
        buffer: &'a mut B,
        limit: ReadGrowthLimit,
    ) -> impl Future<Output = pi_result::Result<ReadGrowthOutcome>> + Send + 'a
    where
        B: GrowableReadBuffer + Send + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Read) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let current_len = buffer.as_ref().len();
            limit
                .checked_final_len(current_len)
                .into_classified_error()?;
            let maximum = limit.max_additional_bytes();
            let source_len = u64::try_from(maximum).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if self.read_position.checked_add(source_len).is_none() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if maximum == 0 {
                return Ok(ReadGrowthOutcome::LimitReached {
                    appended_bytes: 0,
                });
            }

            let chunk_len = maximum.min(READ_TRANSFER_CHUNK_BYTES);
            let mut transfer = vec![0_u8; chunk_len];
            let source_start = self.read_position;
            let mut read_guard = ContentReadGuard::acquire(
                Arc::clone(&self.file),
                &self.coordination,
                self.cross_process_authority(),
            )
            .await?;
            read_guard.file_mut()?.seek(SeekFrom::Start(source_start))
                .await
                .into_classified_error()?;
            let mut appended = 0;
            let outcome = loop {
                if appended == maximum {
                    break ReadGrowthOutcome::LimitReached {
                        appended_bytes: appended,
                    };
                }
                let request = (maximum - appended).min(transfer.len());
                let read = read_guard.file_mut()?
                    .read(&mut transfer[..request])
                    .await
                    .into_classified_error()?;
                if read == 0 {
                    break ReadGrowthOutcome::EndOfFile {
                        appended_bytes: appended,
                    };
                }
                let read_u64 = u64::try_from(read).map_err(|_| {
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::Internal,
                    )
                })?;
                let next_position = self.read_position.checked_add(read_u64)
                    .ok_or_else(|| {
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::Internal,
                        )
                    })?;
                buffer.put_slice(&transfer[..read]);
                self.read_position = next_position;
                appended += read;
            };
            read_guard.quiesce().await?;
            Ok(outcome)
        }
    }

    fn append<'a, B>(
        &'a mut self,
        buffer: B,
    ) -> impl Future<Output = pi_result::RawResult<(), BufferFailure<B>>> + Send + 'a
    where
        B: DetachableWriteBuffer + Send + 'a,
        B::Detached: Send,
        B::Recovery: Send + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Append) {
                return Err(BufferFailure::new(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::InvalidState,
                    ),
                    buffer,
                    crate::TransferProgress::Exact { bytes: 0 },
                ));
            }
            if buffer.as_ref().is_empty() {
                return Ok(());
            }

            // 同文件映射地址来源检查必须位于脱离之前；映射实现切片会在这里
            // 接入稳定身份与地址租约判定。普通 buffer 先执行已冻结脱离协议。
            let append_lease = match self.coordination
                .try_acquire_append(buffer.as_ref()) {
                Ok(lease) => lease,
                Err(error) => {
                    return Err(BufferFailure::new(
                        error,
                        buffer,
                        crate::TransferProgress::Exact { bytes: 0 },
                    ));
                }
            };
            let append_gate = Arc::clone(&self.coordination.append_gate);
            let append_guard = append_gate.lock_arc().await;
            let cross_process_lease = match self.cross_process_authority() {
                Some(authority) => match authority.acquire_append_operation().await {
                    Ok(lease) => Some(lease),
                    Err(error) => {
                        return Err(BufferFailure::new(
                            error,
                            buffer,
                            crate::TransferProgress::Exact { bytes: 0 },
                        ));
                    }
                },
                None => None,
            };
            let (detached, recovery) = buffer.try_detach()?;
            let file_slot = Arc::clone(&self.file);
            let mut file_guard = file_slot.lock_arc().await;
            let mut file = match file_guard.take() {
                Some(file) => file,
                None => {
                    let buffer = B::recover_from_detached(detached, recovery);
                    return Err(BufferFailure::new(
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::InvalidState,
                        ),
                        buffer,
                        crate::TransferProgress::Exact { bytes: 0 },
                    ));
                }
            };
            let task = async_global_executor::spawn(async move {
                let write_result = file.write_all(detached.as_ref()).await;
                // 无论提交阶段是否报错，都要停止并排空 async-fs 写管道，确保
                // 归还 detached 和释放追加租约前后台不再访问输入。
                let flush_result = file.flush().await;
                let result = match (write_result, flush_result) {
                    (Err(error), _) => Err(error),
                    (Ok(()), Err(error)) => Err(error),
                    (Ok(()), Ok(())) => Ok(()),
                };
                *file_guard = Some(file);
                drop(cross_process_lease);
                drop(append_guard);
                drop(append_lease);
                (result, detached)
            });
            let (result, detached) = DetachOnDrop::new(task).await;

            match result {
                Ok(()) => Ok(()),
                Err(error) => {
                    let kind = pi_result::ClassifyErrorKind::classify_error_kind(&error);
                    let error = pi_result::error_stack::Report::new(error)
                        .change_context(kind);
                    let buffer = B::recover_from_detached(detached, recovery);
                    Err(BufferFailure::new(
                        error,
                        buffer,
                        crate::TransferProgress::Unknown,
                    ))
                }
            }
        }
    }

    fn overwrite_all<'a, B>(
        &'a mut self,
        buffer: B,
    ) -> impl Future<Output = pi_result::RawResult<(), OverwriteFailure<B>>> + Send + 'a
    where
        B: DetachableWriteBuffer + Send + 'a,
        B::Detached: Send,
        B::Recovery: Send + 'a,
    {
        async move {
            if !matches!(&self.access, FileAccessMode::Overwrite) {
                return Err(OverwriteFailure::new(
                    pi_result::error_stack::Report::new(
                        pi_result::ErrorKind::InvalidState,
                    ),
                    buffer,
                    crate::TransferProgress::Exact { bytes: 0 },
                    OverwriteTargetEvidence::Unchanged,
                ));
            }
            let input_len = buffer.as_ref().len();
            let target_len = match u64::try_from(input_len) {
                Ok(length) => length,
                Err(_) => {
                    return Err(OverwriteFailure::new(
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::InvalidInput,
                        ),
                        buffer,
                        crate::TransferProgress::Exact { bytes: 0 },
                        OverwriteTargetEvidence::Unchanged,
                    ));
                }
            };
            let exclusive_lease = match self.coordination.try_acquire_exclusive() {
                Ok(lease) => lease,
                Err(error) => {
                    return Err(OverwriteFailure::new(
                        error,
                        buffer,
                        crate::TransferProgress::Exact { bytes: 0 },
                        OverwriteTargetEvidence::Unchanged,
                    ));
                }
            };
            let cross_process_lease = match self.cross_process_authority() {
                Some(authority) => match authority.acquire_exclusive_operation().await {
                    Ok(lease) => Some(lease),
                    Err(error) => {
                        return Err(OverwriteFailure::new(
                            error,
                            buffer,
                            crate::TransferProgress::Exact { bytes: 0 },
                            OverwriteTargetEvidence::Unchanged,
                        ));
                    }
                },
                None => None,
            };
            let (detached, recovery) = match buffer.try_detach() {
                Ok(detached) => detached,
                Err(failure) => {
                    let (error, buffer, progress) = failure.into_parts();
                    return Err(OverwriteFailure::new(
                        error,
                        buffer,
                        progress,
                        OverwriteTargetEvidence::Unchanged,
                    ));
                }
            };
            let file_slot = Arc::clone(&self.file);
            let mut file_guard = file_slot.lock_arc().await;
            let mut file = match file_guard.take() {
                Some(file) => file,
                None => {
                    let buffer = B::recover_from_detached(detached, recovery);
                    return Err(OverwriteFailure::new(
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::InvalidState,
                        ),
                        buffer,
                        crate::TransferProgress::Exact { bytes: 0 },
                        OverwriteTargetEvidence::Unchanged,
                    ));
                }
            };
            let task = async_global_executor::spawn(async move {
                let outcome = if input_len == 0 {
                    match file.set_len(0).await {
                        Ok(()) => LocalOverwriteOutcome::Success,
                        Err(error) => LocalOverwriteOutcome::Failure {
                            error,
                            progress: crate::TransferProgress::Exact { bytes: 0 },
                            target_evidence: OverwriteTargetEvidence::MutationStarted,
                        },
                    }
                } else if let Err(error) = file.seek(SeekFrom::Start(0)).await {
                    LocalOverwriteOutcome::Failure {
                        error,
                        progress: crate::TransferProgress::Exact { bytes: 0 },
                        target_evidence: OverwriteTargetEvidence::Unchanged,
                    }
                } else {
                    let write_result = file.write_all(detached.as_ref()).await;
                    let flush_result = file.flush().await;
                    match (write_result, flush_result) {
                        (Err(error), _) | (Ok(()), Err(error)) => {
                            LocalOverwriteOutcome::Failure {
                                error,
                                progress: crate::TransferProgress::Unknown,
                                target_evidence:
                                    OverwriteTargetEvidence::MutationStarted,
                            }
                        }
                        (Ok(()), Ok(())) => match file.set_len(target_len).await {
                            Ok(()) => LocalOverwriteOutcome::Success,
                            Err(error) => LocalOverwriteOutcome::Failure {
                                error,
                                progress: crate::TransferProgress::Exact {
                                    bytes: input_len,
                                },
                                target_evidence:
                                    OverwriteTargetEvidence::MutationStarted,
                            },
                        },
                    }
                };
                *file_guard = Some(file);
                drop(cross_process_lease);
                drop(exclusive_lease);
                (outcome, detached)
            });
            let (outcome, detached) = DetachOnDrop::new(task).await;
            match outcome {
                LocalOverwriteOutcome::Success => Ok(()),
                LocalOverwriteOutcome::Failure {
                    error,
                    progress,
                    target_evidence,
                } => {
                    let kind = pi_result::ClassifyErrorKind::classify_error_kind(&error);
                    let error = pi_result::error_stack::Report::new(error)
                        .change_context(kind);
                    let buffer = B::recover_from_detached(detached, recovery);
                    Err(OverwriteFailure::new(
                        error,
                        buffer,
                        progress,
                        target_evidence,
                    ))
                }
            }
        }
    }

    fn truncate(
        &mut self,
        new_len: u64,
    ) -> impl Future<Output = pi_result::Result<()>> + Send + '_ {
        async move {
            if !matches!(&self.access, FileAccessMode::Truncate) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let exclusive_lease = self.coordination.try_acquire_exclusive()?;
            let cross_process_lease = match self.cross_process_authority() {
                Some(authority) => Some(
                    authority.acquire_exclusive_operation().await?,
                ),
                None => None,
            };
            let file_slot = Arc::clone(&self.file);
            let mut file_guard = file_slot.lock_arc().await;
            let file = file_guard.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let current_len = file
                .metadata()
                .await
                .into_classified_error()?
                .len();
            if new_len > current_len {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            if new_len == current_len {
                return Ok(());
            }

            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let task = async_global_executor::spawn(async move {
                let result = file.set_len(new_len).await;
                *file_guard = Some(file);
                drop(cross_process_lease);
                drop(exclusive_lease);
                result
            });
            DetachOnDrop::new(task).await.into_classified_error()
        }
    }

    unsafe fn map_read_only_uncoordinated(
        &self,
        range: MmapRange,
    ) -> impl Future<Output = pi_result::Result<ReadMmapHandle>> + Send + '_ {
        async move {
            if self.coordinated_lease.is_some() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            if !matches!(
                &self.access,
                FileAccessMode::ReadMmap | FileAccessMode::ReadWriteMmap
            ) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }

            let start = range.start();
            let end_exclusive = range.end_exclusive();
            let mapping_len = usize::try_from(range.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if isize::try_from(mapping_len).is_err() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let pending_mapping = self.coordination
                .try_reserve_mapping(start, end_exclusive)?;
            let mut file_guard = self.file.lock_arc().await;
            let file = file_guard.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let snapshot_len = file.metadata()
                .await
                .into_classified_error()?
                .len();
            if end_exclusive > snapshot_len {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let task = async_global_executor::spawn(
                finish_read_only_mapping(
                    file,
                    file_guard,
                    range,
                    mapping_len,
                    pending_mapping,
                    None,
                ),
            );
            DetachOnDrop::new(task).await
        }
    }

    fn map_read_only(
        &self,
        range: MmapRange,
    ) -> impl Future<Output = pi_result::Result<ReadMmapHandle>> + Send + '_ {
        async move {
            if !matches!(
                &self.access,
                FileAccessMode::ReadMmap | FileAccessMode::ReadWriteMmap
            ) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let authority = self.cross_process_authority().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let start = range.start();
            let end_exclusive = range.end_exclusive();
            let mapping_len = usize::try_from(range.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if isize::try_from(mapping_len).is_err() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let pending_mapping = self.coordination
                .try_reserve_mapping(start, end_exclusive)?;
            let cross_process_mapping = authority
                .acquire_mapping_operation(start, end_exclusive)
                .await?;
            let mut file_guard = self.file.lock_arc().await;
            let file = file_guard.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let snapshot_len = file.metadata()
                .await
                .into_classified_error()?
                .len();
            if end_exclusive > snapshot_len {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let task = async_global_executor::spawn(
                finish_read_only_mapping(
                    file,
                    file_guard,
                    range,
                    mapping_len,
                    pending_mapping,
                    Some(cross_process_mapping),
                ),
            );
            DetachOnDrop::new(task).await
        }
    }

    unsafe fn map_read_write_uncoordinated(
        &self,
        range: MmapRange,
    ) -> impl Future<Output = pi_result::Result<ReadWriteMmapHandle>> + Send + '_ {
        async move {
            if self.coordinated_lease.is_some() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            if !matches!(&self.access, FileAccessMode::ReadWriteMmap) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }

            let start = range.start();
            let end_exclusive = range.end_exclusive();
            let mapping_len = usize::try_from(range.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if isize::try_from(mapping_len).is_err() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let pending_mapping = self.coordination
                .try_reserve_mapping(start, end_exclusive)?;
            let mut file_guard = self.file.lock_arc().await;
            let file = file_guard.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let snapshot_len = file.metadata()
                .await
                .into_classified_error()?
                .len();
            if end_exclusive > snapshot_len {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let task = async_global_executor::spawn(
                finish_read_write_mapping(
                    file,
                    file_guard,
                    range,
                    mapping_len,
                    pending_mapping,
                    None,
                ),
            );
            DetachOnDrop::new(task).await
        }
    }

    fn map_read_write(
        &self,
        range: MmapRange,
    ) -> impl Future<Output = pi_result::Result<ReadWriteMmapHandle>> + Send + '_ {
        async move {
            if !matches!(&self.access, FileAccessMode::ReadWriteMmap) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }
            let authority = self.cross_process_authority().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let start = range.start();
            let end_exclusive = range.end_exclusive();
            let mapping_len = usize::try_from(range.len()).map_err(|_| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                )
            })?;
            if isize::try_from(mapping_len).is_err() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }

            let pending_mapping = self.coordination
                .try_reserve_mapping(start, end_exclusive)?;
            let cross_process_mapping = authority
                .acquire_mapping_operation(start, end_exclusive)
                .await?;
            let mut file_guard = self.file.lock_arc().await;
            let file = file_guard.as_ref().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let snapshot_len = file.metadata()
                .await
                .into_classified_error()?
                .len();
            if end_exclusive > snapshot_len {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidInput,
                ));
            }
            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;
            let task = async_global_executor::spawn(
                finish_read_write_mapping(
                    file,
                    file_guard,
                    range,
                    mapping_len,
                    pending_mapping,
                    Some(cross_process_mapping),
                ),
            );
            DetachOnDrop::new(task).await
        }
    }

    fn flush(
        &mut self,
        mode: FileFlushMode,
    ) -> impl Future<Output = pi_result::Result<()>> + Send + '_ {
        async move {
            if !matches!(
                &self.access,
                FileAccessMode::Append
                    | FileAccessMode::Overwrite
                    | FileAccessMode::Truncate
                    | FileAccessMode::ReadWriteMmap
            ) {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                ));
            }

            let flush_lease = self.coordination.try_acquire_flush()?;
            let cross_process_lease = match self.cross_process_authority() {
                Some(authority) => Some(
                    authority.acquire_shared_content_operation().await?,
                ),
                None => None,
            };
            let file_slot = Arc::clone(&self.file);
            let mut file_guard = file_slot.lock_arc().await;
            let file = file_guard.take().ok_or_else(|| {
                pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::InvalidState,
                )
            })?;

            // 强刷新已经提交后，公开 Future 的取消只停止等待。actor 必须继续
            // 持有文件资源与刷新许可，直到系统刷新完成并归还资源；映射与追加
            // 可继续使用，但覆盖/截断等破坏性操作不能提前放行。
            // B2H-DES-001 的取消/跨线程回归见 tests/local_file_flush.rs；合作式
            // 父子进程持有映射的组合回归见 tests/local_cross_process_integration.rs。
            let task = async_global_executor::spawn(async move {
                let result = match mode {
                    FileFlushMode::Data => file.sync_data().await,
                    FileFlushMode::DataAndMetadata => file.sync_all().await,
                };
                *file_guard = Some(file);
                drop(cross_process_lease);
                drop(flush_lease);
                result
            });
            DetachOnDrop::new(task).await.into_classified_error()
        }
    }
}

// 与 FileIo 相同，显式 Future 形状用于逐项匹配已冻结的运行时无关合同。
#[allow(clippy::manual_async_fn)]
impl FileNamespace for LocalFileNamespace {
    type Locator = PathBuf;
    type File = LocalFile;

    fn metadata<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<
        Output = pi_result::Result<FileMetadata<Self::Locator>>,
    > + Send
           + 'a {
        async move {
            let metadata = match async_fs::metadata(locator).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let error = validate_local_not_found_path(locator, error)
                        .await?;
                    return Err(classified_io_error(error));
                }
                Err(error) => return Err(classified_io_error(error)),
            };
            Ok(project_local_metadata(metadata))
        }
    }

    fn symlink_metadata<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<
        Output = pi_result::Result<FileMetadata<Self::Locator>>,
    > + Send
           + 'a {
        async move {
            let metadata = match async_fs::symlink_metadata(locator).await {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    let error = validate_local_not_found_path(locator, error)
                        .await?;
                    return Err(classified_io_error(error));
                }
                Err(error) => return Err(classified_io_error(error)),
            };
            Ok(project_local_metadata(metadata))
        }
    }

    fn try_exists<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::Result<bool>> + Send + 'a {
        async move {
            match async_fs::metadata(locator).await {
                Ok(_) => Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    validate_local_not_found_path(locator, error).await?;
                    Ok(false)
                }
                Err(error) => core::result::Result::<bool, _>::Err(error)
                    .into_classified_error(),
            }
        }
    }

    fn available_space<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::Result<AvailableSpace>> + Send + 'a {
        async move {
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                bounded_blocking::unblock_result(move || {
                    query_local_available_space(&locator)
                })
                .await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn read_dir<'a>(
        &'a self,
        locator: Self::Locator,
    ) -> impl Future<
        Output = pi_result::Result<Self::DirectoryStream<'a>>,
    > + Send
           + 'a {
        async move {
            let entries = open_local_directory(locator).await?;
            let stream: crate::BoxDirectoryStream<'a, PathBuf> =
                Box::pin(LocalDirectoryStream::new(entries));
            Ok(stream)
        }
    }

    fn walk<'a>(
        &'a self,
        locator: Self::Locator,
        options: WalkOptions,
    ) -> impl Future<Output = pi_result::Result<Self::WalkStream<'a>>>
           + Send
           + 'a {
        async move {
            let root_directory = open_local_walk_root(locator).await?;
            let maximum_depth = options.depth_limit.maximum_depth();
            let stream = async_stream::stream! {
                if maximum_depth != Some(0) {
                    let mut stack = vec![(
                        root_directory,
                        NonZeroUsize::new(1)
                            .expect("walk descendants always start at depth one"),
                    )];

                    while let Some((directory, depth)) = stack.pop() {
                        let (directory, next) = match next_local_walk_entry(directory).await {
                            Ok(completed) => completed,
                            Err(failure) => {
                                let (error, directory) = *failure;
                                // 容量拒绝发生在同步工作提交前，因此目录游标仍然
                                // 可靠。重新放回栈，使调用方处理错误后可以继续。
                                stack.push((directory, depth));
                                yield Err(error);
                                continue;
                            }
                        };

                        let Some(next) = next else {
                            continue;
                        };
                        let (entry_name, native_type) = match next {
                            Ok(entry) => entry,
                            Err(error) => {
                                stack.push((directory, depth));
                                yield core::result::Result::<WalkEntry<PathBuf>, _>::Err(error)
                                    .into_classified_error();
                                continue;
                            }
                        };

                        let file_type = classify_capability_file_type(&native_type);
                        let is_directory = matches!(&file_type, FileType::Directory);
                        let can_descend = is_directory
                            && maximum_depth
                                .is_none_or(|maximum| depth.get() < maximum);
                        let entry_locator = directory.locator.join(&entry_name);
                        let child = can_descend.then(|| (
                            PathBuf::from(&entry_name),
                            entry_locator.clone(),
                        ));
                        let directory_entry = DirectoryEntry::new(
                            EntryName::from(entry_name),
                            entry_locator,
                            Some(file_type),
                        );
                        let walk_entry = WalkEntry::new(directory_entry, depth);
                        let Some((child_name, child_locator)) = child else {
                            stack.push((directory, depth));
                            yield Ok(walk_entry);
                            continue;
                        };
                        yield Ok(walk_entry);
                        let Some(next_depth) = depth
                            .get()
                            .checked_add(1)
                            .and_then(NonZeroUsize::new)
                        else {
                            stack.push((directory, depth));
                            yield Err(pi_result::error_stack::Report::new(
                                pi_result::ErrorKind::ResourceExhausted,
                            ));
                            continue;
                        };
                        match open_local_walk_child(
                            directory,
                            child_name,
                            child_locator,
                        )
                        .await {
                            Ok((parent, Ok(child))) => {
                                stack.push((parent, depth));
                                stack.push((child, next_depth));
                            }
                            Ok((parent, Err(error))) => {
                                stack.push((parent, depth));
                                yield core::result::Result::<WalkEntry<PathBuf>, _>::Err(error)
                                    .into_classified_error();
                            }
                            Err(failure) => {
                                let (error, parent) = *failure;
                                stack.push((parent, depth));
                                yield Err(error);
                            }
                        }
                    }
                }
            };
            let stream: crate::BoxWalkStream<'a, PathBuf> = Box::pin(stream);
            Ok(stream)
        }
    }

    fn create_dir<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), CreateFailure>>
           + Send
           + 'a {
        async move {
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                create_local_directory(core, locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn create_dir_all<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<
        Output = pi_result::RawResult<
            (),
            CreateDirectoriesFailure<Self::Locator>,
        >,
    > + Send
           + 'a {
        async move {
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                create_local_directories(core, locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn remove_file<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                remove_local_file(core, locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn remove_file_uncoordinated<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                remove_local_file_uncoordinated(locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn remove_dir<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                remove_local_directory(core, locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn remove_dir_uncoordinated<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                remove_local_directory_uncoordinated(locator).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn rename<'a>(
        &'a self,
        source: &'a Self::Locator,
        destination: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RenameFailure>>
           + Send
           + 'a {
        async move {
            let core = self.core;
            let source = source.clone();
            let destination = destination.clone();
            let task = async_global_executor::spawn(async move {
                rename_local_entry(core, source, destination).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn open<'a>(
        &'a self,
        locator: &'a Self::Locator,
        access: FileAccessMode,
    ) -> impl Future<Output = pi_result::Result<Self::File>> + Send + 'a {
        async move {
            let entry_reservation = acquire_namespace_entry_reservation(
                self.core,
                locator,
            )
            .await?;
            let path_metadata =
                async_fs::metadata(locator).await.into_classified_error()?;
            if !path_metadata.is_file() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Unsupported,
                ));
            }

            let mut options = async_fs::OpenOptions::new();
            match &access {
                FileAccessMode::Read | FileAccessMode::ReadMmap => {
                    options.read(true);
                }
                FileAccessMode::Append => {
                    options.append(true);
                }
                FileAccessMode::Overwrite | FileAccessMode::Truncate => {
                    options.write(true);
                }
                FileAccessMode::ReadWriteMmap => {
                    options.read(true).write(true);
                }
            }
            #[cfg(unix)]
            {
                use async_fs::unix::OpenOptionsExt;

                // 防止路径在预检后被替换成 FIFO 等会阻塞 open 的特殊对象。
                // 对常规文件，`O_NONBLOCK` 不改变后续读写语义。
                options.custom_flags(libc::O_NONBLOCK);
            }

            let file = options.open(locator).await.into_classified_error()?;
            let metadata = file.metadata().await.into_classified_error()?;
            if !metadata.is_file() {
                return Err(pi_result::error_stack::Report::new(
                    pi_result::ErrorKind::Unsupported,
                ));
            }

            let (file, identity) = stable_file_identity(file, &metadata).await?;
            let protocol_locator = locator.clone();
            bounded_blocking::unblock_result(move || {
                reject_persisted_cross_process_domain(&protocol_locator)
            })
            .await?;
            let coordination = self.core.registry.coordination_for(identity)
                .await?;
            let resource_id = self.core.allocate_resource_id()?;
            coordination.register_ordinary_resource()?;

            drop(entry_reservation);
            Ok(LocalFile {
                file: Arc::new(async_lock::Mutex::new(Some(file))),
                access,
                read_position: 0,
                coordination,
                coordinated_lease: None,
                resource_id,
            })
        }
    }

    fn create_new<'a>(
        &'a self,
        locator: &'a Self::Locator,
        access: FileAccessMode,
    ) -> impl Future<
        Output = pi_result::RawResult<Self::File, CreateFailure>,
    > + Send
           + 'a {
        async move {
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                create_local_file(core, locator, access).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn copy_new<'a>(
        &'a self,
        source: &'a Self::Locator,
        destination: &'a Self::Locator,
    ) -> impl Future<
        Output = pi_result::RawResult<CopyOutcome, CopyFailure>,
    > + Send
           + 'a {
        async move {
            let core = self.core;
            let source = source.clone();
            let destination = destination.clone();
            let cancelled = Arc::new(AtomicBool::new(false));
            let task_cancelled = Arc::clone(&cancelled);
            let task = async_global_executor::spawn(async move {
                copy_local_file(
                    core,
                    source,
                    destination,
                    task_cancelled,
                )
                .await
            });
            CancelCopyOnDrop::new(task, cancelled).await
        }
    }

    unsafe fn create_new_coordinated<'a>(
        &'a self,
        locator: Self::Locator,
        access: FileAccessMode,
    ) -> impl Future<
        Output = pi_result::RawResult<
            CrossProcessCreateSuccess<
                Self::CrossProcessAuthority,
                Self::File,
            >,
            CreateFailure,
        >,
    > + Send
           + 'a {
        async move {
            let namespace = self.core;
            let task = async_global_executor::spawn(async move {
                create_local_file_coordinated(namespace, locator, access).await
            });
            DetachOnDrop::new(task).await
        }
    }

    unsafe fn establish_cross_process_authority<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<
        Output = pi_result::Result<Self::CrossProcessAuthority>,
    > + Send
           + 'a {
        async move {
            let namespace = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                establish_local_cross_process_authority(
                    namespace,
                    locator,
                )
                .await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn open_with_cross_process_authority<'a>(
        &'a self,
        authority: &'a Self::CrossProcessAuthority,
        access: FileAccessMode,
    ) -> impl Future<Output = pi_result::Result<Self::File>> + Send + 'a {
        async move {
            let namespace = self.core;
            let authority = Arc::clone(authority.local_core());
            let task = async_global_executor::spawn(async move {
                open_local_file_with_cross_process_authority(
                    namespace,
                    authority,
                    access,
                )
                .await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn remove_file_with_cross_process_authority<'a>(
        &'a self,
        authority: &'a Self::CrossProcessAuthority,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let namespace = self.core;
            let authority = Arc::clone(authority.local_core());
            let task = async_global_executor::spawn(async move {
                remove_local_file_with_cross_process_authority(
                    namespace,
                    authority,
                )
                .await
            });
            DetachOnDrop::new(task).await
        }
    }

    unsafe fn resume_coordinated_file_removal<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<(), RemoveFailure>>
           + Send
           + 'a {
        async move {
            let namespace = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                resume_local_coordinated_file_removal(namespace, locator).await
            });
            DetachOnDrop::new(task).await
        }
    }
}

#[cfg(test)]
mod cross_process_protocol_tests {
    //! 跨进程协议记录与复制暂存清理的生产代码单元测试。
    //!
    //! 协议测试验证纯编码不变量；清理测试使用真实文件和真实稳定身份调用生产
    //! 清理入口。跨进程锁与公开 API 的完整装配另由 `tests/local_cross_process_*`
    //! 覆盖，因此这里不以内部直接构造替代生产可达性证据。

    use super::*;

    // 建立一个真实排他暂存文件并从其打开句柄取得平台稳定身份。
    async fn create_test_staging_file(
        path: &Path,
    ) -> (async_fs::File, StableFileIdentity) {
        let mut options = async_fs::OpenOptions::new();
        options.read(true).write(true).create_new(true);
        let file = options.open(path).await.expect("应能创建测试暂存文件");
        let metadata = file.metadata().await.expect("应能读取测试暂存文件元信息");
        let (file, identity) = stable_file_identity(file, &metadata)
            .await
            .expect("测试暂存文件应具有稳定身份");
        (file, identity)
    }

    #[cfg(unix)]
    // 构造仅供协议编解码纯测试使用的 Unix 身份值，不代表真实文件。
    fn test_identity(inode: u64) -> StableFileIdentity {
        StableFileIdentity::Unix { device: 1, inode }
    }

    #[cfg(windows)]
    // 构造仅供协议编解码纯测试使用的 Windows 身份值，不代表真实文件。
    fn test_identity(index: u64) -> StableFileIdentity {
        let mut file_id = [0_u8; 16];
        file_id[..8].copy_from_slice(&index.to_le_bytes());
        StableFileIdentity::Windows {
            volume_serial_number: 1,
            file_id,
        }
    }

    // 验证两种身份冲突原因经生产协议编码和解析后逐字段无损往返。
    #[test]
    fn test_identity_conflict_reason_round_trips_in_protocol_record() {
        for reason in [
            CrossProcessIdentityConflictReason::Missing,
            CrossProcessIdentityConflictReason::DifferentIdentity,
        ] {
            let record = CrossProcessProtocolRecord {
                sequence: 7,
                state: CrossProcessProtocolState::IdentityConflict,
                identity_conflict_reason: Some(reason.clone()),
                generation: [1_u8; 16],
                target_incarnation: [2_u8; 16],
                target_identity: test_identity(3),
                root_identity: test_identity(4),
                parent_identity: test_identity(5),
            };

            let decoded = parse_protocol_record(&encode_protocol_record(&record))
                .expect("完整的身份冲突记录应能无损解码");
            assert_eq!(decoded, record);
            assert_eq!(decoded.identity_conflict_reason, Some(reason));
        }
    }

    // 验证冲突原因与协议状态必须成对出现，任何不匹配记录均按损坏拒绝。
    #[test]
    fn test_protocol_rejects_identity_conflict_reason_state_mismatches() {
        let published_with_reason = CrossProcessProtocolRecord {
            sequence: 8,
            state: CrossProcessProtocolState::Published,
            identity_conflict_reason: Some(
                CrossProcessIdentityConflictReason::Missing,
            ),
            generation: [1_u8; 16],
            target_incarnation: [2_u8; 16],
            target_identity: test_identity(3),
            root_identity: test_identity(4),
            parent_identity: test_identity(5),
        };
        let conflict_without_reason = CrossProcessProtocolRecord {
            sequence: 9,
            state: CrossProcessProtocolState::IdentityConflict,
            identity_conflict_reason: None,
            generation: [1_u8; 16],
            target_incarnation: [2_u8; 16],
            target_identity: test_identity(3),
            root_identity: test_identity(4),
            parent_identity: test_identity(5),
        };

        for invalid in [published_with_reason, conflict_without_reason] {
            let error = parse_protocol_record(&encode_protocol_record(&invalid))
                .expect_err("状态与身份冲突原因不匹配的记录必须被拒绝");
            assert_eq!(error.current_context(), &pi_result::ErrorKind::Corrupted);
        }
    }

    // 验证已知路径仍指向本次创建的同一身份时，生产清理函数关闭原句柄、删除
    // 暂存文件，并给出精确的“创建后已移除”证据。测试使用真实临时目录和
    // 真实文件身份，不通过测试侧逻辑替代任何删除或身份检查。
    #[test]
    fn test_copy_staging_cleanup_removes_matching_identity() {
        let directory = tempfile::tempdir().expect("应能创建独立临时目录");
        let path = directory.path().join("matching.copy");
        let (file, identity) = futures_lite::future::block_on(
            create_test_staging_file(&path),
        );

        let evidence = futures_lite::future::block_on(
            cleanup_local_copy_staging_resource(
                path.clone(),
                identity,
                Some(file),
            ),
        );

        assert_eq!(
            evidence,
            CopyStagingEvidence::CreatedThenRemovedByOperation,
        );
        assert!(!path.exists(), "成功清理后暂存路径必须不存在");
    }

    // 验证暂存路径在清理前已经消失时，生产清理函数不会把外部消失误报为
    // “由本次操作移除”。本场景仅在 Unix 上使用真实的 unlink-while-open
    // 语义；它验证保守证据分类，不替代 Windows 专项验证。
    #[cfg(unix)]
    #[test]
    fn test_copy_staging_cleanup_reports_unknown_when_path_disappeared() {
        let directory = tempfile::tempdir().expect("应能创建独立临时目录");
        let path = directory.path().join("missing.copy");
        let (file, identity) = futures_lite::future::block_on(
            create_test_staging_file(&path),
        );
        std::fs::remove_file(&path).expect("Unix 应允许移除仍有句柄的路径");

        let evidence = futures_lite::future::block_on(
            cleanup_local_copy_staging_resource(
                path.clone(),
                identity,
                Some(file),
            ),
        );

        assert_eq!(evidence, CopyStagingEvidence::Unknown);
        assert!(!path.exists(), "清理不得重新创建已经消失的路径");
    }

    // 验证同一暂存名称已经指向不同稳定身份时，生产清理函数绝不按名称盲删。
    // 原文件保持打开并在 Unix 上解除名称，再创建带已知内容的替代文件；最终
    // 同时断言保守证据、替代文件仍在及其内容未被改变。
    #[cfg(unix)]
    #[test]
    fn test_copy_staging_cleanup_preserves_replacement_identity() {
        let directory = tempfile::tempdir().expect("应能创建独立临时目录");
        let path = directory.path().join("replaced.copy");
        let (file, identity) = futures_lite::future::block_on(
            create_test_staging_file(&path),
        );
        std::fs::remove_file(&path).expect("Unix 应允许解除原文件名称");
        std::fs::write(&path, b"replacement").expect("应能创建替代文件");

        let evidence = futures_lite::future::block_on(
            cleanup_local_copy_staging_resource(
                path.clone(),
                identity,
                Some(file),
            ),
        );

        assert_eq!(evidence, CopyStagingEvidence::Unknown);
        assert_eq!(
            std::fs::read(&path).expect("替代文件必须继续存在"),
            b"replacement",
        );
    }

    // 验证路径身份保持不变但操作系统拒绝删除时，生产清理函数经二次身份核验
    // 返回“完成时已知仍存在”。目录权限只负责制造真实 unlink 失败；若测试由
    // root 运行，Unix 权限不能形成可靠证据，因此明确拒绝该测试环境，不能
    // 通过提前返回把未执行的断言计为成功。
    #[cfg(unix)]
    #[test]
    fn test_copy_staging_cleanup_reports_known_present_after_delete_failure() {
        // SAFETY: `geteuid` 不解引用指针、没有前置内存条件，仅查询当前进程身份。
        assert_ne!(
            unsafe { libc::geteuid() },
            0,
            "该专项要求非 root Unix 身份，root 不能用目录权限制造可靠删除失败",
        );
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("应能创建独立临时目录");
        let original_mode = std::fs::metadata(directory.path())
            .expect("应能读取目录权限")
            .permissions()
            .mode();
        let path = directory.path().join("delete-denied.copy");
        let (file, identity) = futures_lite::future::block_on(
            create_test_staging_file(&path),
        );
        std::fs::set_permissions(
            directory.path(),
            std::fs::Permissions::from_mode(0o500),
        )
        .expect("应能移除目录写权限");

        let evidence = futures_lite::future::block_on(
            cleanup_local_copy_staging_resource(
                path.clone(),
                identity,
                Some(file),
            ),
        );
        std::fs::set_permissions(
            directory.path(),
            std::fs::Permissions::from_mode(original_mode),
        )
        .expect("应能恢复目录权限供夹具清理");

        assert_eq!(evidence, CopyStagingEvidence::KnownPresentAtCompletion);
        assert!(path.exists(), "删除失败后同一身份的暂存文件应仍存在");
    }
}

#[cfg(test)]
mod namespace_entry_registry_tests {
    //! Namespace 条目预约与真实父目录身份绑定的不变量测试。

    use super::*;

    // 先通过生产身份查询取得旧父目录键，再把该目录改名并在原位置创建一个
    // 新目录。生产预约入口必须拒绝过期键，不能让旧身份许可作用于新目录。
    #[test]
    fn test_resolved_namespace_entry_rejects_rebound_parent() {
        let temporary = tempfile::tempdir().expect("应能创建独立临时目录");
        let parent = temporary.path().join("parent");
        let moved_parent = temporary.path().join("moved-parent");
        let target = parent.join("target");
        std::fs::create_dir(&parent).expect("应能创建原父目录");

        let stale_key = futures_lite::future::block_on(
            local_namespace_entry_key(&target),
        )
        .expect("原父目录应具有稳定身份")
        .expect("目标应具有末级名称");
        std::fs::rename(&parent, &moved_parent)
            .expect("应能把原父目录移到其它名称");
        std::fs::create_dir(&parent).expect("应能在原位置创建新父目录");

        // 生产预约保存静态注册表引用；独立测试核心只存活到测试进程结束，避免
        // 与全局 namespace 及其它集成测试共享协调状态。
        let core = Box::leak(Box::new(LocalNamespaceCore::new()));
        let result = futures_lite::future::block_on(
            reserve_resolved_namespace_entry(core, &target, stale_key),
        );
        match result {
            Ok(_) => panic!("过期父目录身份不得取得可用于新目录的预约"),
            Err(error) => assert_eq!(
                error.current_context(),
                &pi_result::ErrorKind::Conflict,
            ),
        }
    }
}
