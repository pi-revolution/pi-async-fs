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
#[cfg(windows)]
use std::os::windows::io::AsHandle;
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
use crate::file_identity::StableFileIdentity;
use crate::open_admission_native::{CoordinationRootAnchor, OpenAdmissionKind};
use crate::open_admission_state::OpenAdmissionState;
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
    // EXO-ACQ：只有Custom实际取得才建立B04槽；沿原核心保活，不加每次IO查询。
    open_admission: OnceLock<OpenAdmissionState>,
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
            open_admission: OnceLock::new(),
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

    // EXO-LIF-F08：只在cold建立/删除时重新接上已验证凭据。原凭据可能比
    // 活动核心活得久；同完整绑定的独立凭据必须原子汇合到当前同一核心。
    // 临界区只处理值/Weak；原注册表持久摘要安装在放下data锁之后。
    fn attach_cross_process_authority(
        self: &Arc<Self>,
        authority: Arc<CrossProcessAuthorityCore>,
    ) -> pi_result::Result<Arc<CrossProcessAuthorityCore>> {
        let mut data = lock_unpoisoned(&self.data);
        if let Some(current) = data.cross_process_core.as_ref().and_then(Weak::upgrade) {
            return if current.matches(authority.namespace_identity, &authority.binding) {
                Ok(current)
            } else {
                Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict))
            };
        }
        if data.authority_initializing || data.ordinary_resources != 0
            || data.exclusive_operation
            || data.cross_process_binding.as_ref().is_some_and(|managed| managed.binding != authority.binding)
        {
            return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict));
        }
        let managed = ManagedCrossProcessBinding {
            locator: authority.locator.clone(),
            binding: authority.binding.clone(),
        };
        data.cross_process_core = Some(Arc::downgrade(&authority));
        data.cross_process_binding = Some(managed.clone());
        drop(data);
        self.registry.install_cross_process_binding(&self.identity, self, managed);
        Ok(authority)
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
    ) -> Self {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&binding.target_incarnation[..8]);
        Self {
            namespace_identity,
            locator,
            binding,
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

    // EXO-LIF-F01：只管旧原生生命周期，不持有/依赖活动核心类型。
    // 成功后的计数必须立即交给高层RAII租约；本出口不公开给凭据调用方。
    // LIF-INHERITED-R01：非0观察与登记必须在同一短状态锁内完成，防止最后
    // 释放者夹在两者之间关闭原生共享锁。原生取得/await仍在状态锁外；复用
    // 分支不增加锁或原生工作。实证与冻结边界见R01_LIFECYCLE_RACE_DIAGNOSIS_AND_REPAIR_PROPOSAL_2026-10-13.md。
    async fn acquire_lifecycle_owner_count(&self) -> pi_result::Result<()> {
        if self.retired.load(AtomicOrdering::Acquire) {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::Conflict,
            ));
        }
        let admission = self.lifecycle_admission.clone().lock_arc().await;

        let requires_lock = {
            let mut state = lock_unpoisoned(&self.lifecycle_state);
            if state.active_owners == 0 {
                true
            } else {
                state.active_owners = state.active_owners.checked_add(1)
                    .ok_or_else(|| {
                        pi_result::error_stack::Report::new(
                            pi_result::ErrorKind::ResourceExhausted,
                        )
                    })?;
                false
            }
        };
        if !requires_lock {
            drop(admission);
            return Ok(());
        }
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
        drop(admission);
        Ok(())
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

    fn mark_retired(&self) {
        self.retired.store(true, AtomicOrdering::Release);
    }
}

// EXO-LIF-F03~07：高层同时消费原活动核心和凭据；下层Core不反向依赖
// 活动类型/操作结果。调用方直接传已有核心，不在IO热路径查表或访问资格槽。
async fn acquire_cross_process_operation_owner(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
) -> pi_result::Result<CoordinatedResourceLease> {
    let pending = coordination.try_reserve_coordinated_operation(authority)?;
    CoordinatedResourceLease::acquire(authority, pending).await
}

async fn acquire_cross_process_read_operation(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
) -> pi_result::Result<CrossProcessReadLease> {
    let active_owner = acquire_cross_process_operation_owner(authority, coordination).await?;
    let binding = authority.binding.clone();
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
async fn acquire_cross_process_shared_content_operation(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
) -> pi_result::Result<CrossProcessSharedContentLease> {
    let active_owner = acquire_cross_process_operation_owner(authority, coordination).await?;
    let content_path = authority.binding.layout.stable_content_gate.clone();
    let content = bounded_blocking::unblock_result(move || {
        try_lock_sidecar_shared(&content_path)
    })
    .await?;
    Ok(CrossProcessSharedContentLease {
        _content: content,
        _active_owner: active_owner,
    })
}

async fn acquire_cross_process_append_operation(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
) -> pi_result::Result<CrossProcessAppendLease> {
    let active_owner = acquire_cross_process_operation_owner(authority, coordination).await?;
    let content_path = authority.binding.layout.stable_content_gate.clone();
    let append_path = authority.binding.layout.append_admission_gate.clone();
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

async fn acquire_cross_process_exclusive_operation(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
) -> pi_result::Result<CrossProcessExclusiveLease> {
    let active_owner = acquire_cross_process_operation_owner(authority, coordination).await?;
    let binding = authority.binding.clone();
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

async fn acquire_cross_process_mapping_operation(
    authority: &Arc<CrossProcessAuthorityCore>,
    coordination: &Arc<FileCoordinationState>,
    start: u64,
    end_exclusive: u64,
) -> pi_result::Result<CrossProcessMappingLease> {
    let active_owner = acquire_cross_process_operation_owner(authority, coordination).await?;
    let binding = authority.binding.clone();
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

impl CoordinatedResourceLease {
    // EXO-LIF-F02：原生计数成功后立即由RAII承接，随后才promote；错误会
    // 逆向撤销计数及预约。沿原Arc链拥有资格，不新增IO资格引用计数。
    async fn acquire(
        authority: &Arc<CrossProcessAuthorityCore>,
        pending: PendingCoordinatedResource,
    ) -> pi_result::Result<Self> {
        authority.acquire_lifecycle_owner_count().await?;
        let mut lease = Self { authority: Arc::clone(authority), process_lease: None };
        lease.process_lease = Some(pending.promote()?);
        Ok(lease)
    }
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
    // EXO-IDQ-U01：仅传递已验证的快照，不重查/转移资源；本层函数见
    // file_identity_query::identity_query_tests，正式 L02 调用图 IQV-CALL-001。
    let identity = crate::file_identity_query::identity_from_metadata(&metadata)?;
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
    // EXO-IDQ-W01：原文件在同步调用期间保活，借用不保存/关闭；本层函数见
    // file_identity_query::identity_query_tests，正式 L02 调用图 IQV-CALL-001。
    let identity = crate::file_identity_query::identity_from_handle(file.as_handle())?;
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

    // EXO-RCF-M01/F02~03：仅改配置出口归属；原freeze_root条件与调用时机保持。
    // 观察None不是资格证明；本次不把新增打开期规则夹带进旧布局。
    // 合同/RCF-T05/A02与ORG/DEP/CALL图：.scratch/pi-async-fs-mainline/
    // EXCLUSIVE_COORDINATION_ROOT_CONFIG_LAYER_PLAN_2026-10-10.md#exclusive-root-config-layer-calls。
    let configured_root = if freeze_root {
        Some(crate::coordination_root_config::frozen_cross_process_coordination_root())
    } else {
        crate::coordination_root_config::configured_cross_process_coordination_root()
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
    // EXO-IDQ-U01：原目录/链接策略保持，直接使用已取得快照；本层函数见
    // file_identity_query::identity_query_tests，正式 L02 调用图 IQV-CALL-001。
    crate::file_identity_query::identity_from_metadata(&metadata)
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
    // EXO-IDQ-W01：原目录句柄保活至同步返回，不新增拥有者；本层函数见
    // file_identity_query::identity_query_tests，正式 L02 调用图 IQV-CALL-001。
    crate::file_identity_query::identity_from_handle(file.as_handle())
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
            Some(authority) => Some(acquire_cross_process_read_operation(&authority, coordination).await?),
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

// 新增能力错误沿thiserror/pi_result分类，OS失败保留实际Interop来源。
#[derive(Debug, pi_result::thiserror::Error)]
enum OpenAcquisitionError {
    #[error("exclusive opening requires a custom coordination root")]
    CommonRootRequired,
    #[error("native identity pin is unsupported on this platform")]
    UnsupportedPlatform,
}

impl pi_result::ClassifyErrorKind for OpenAcquisitionError {
    fn classify_error_kind(&self) -> pi_result::ErrorKind { pi_result::ErrorKind::Unsupported }
}

// EXO-ACQ-T01：数据文件先于活动核心释放；资格只归原核心，不新建资格Arc链。
// 合同/三图/相关验证见EXCLUSIVE_OPEN_ACQUISITION_LAYER_2026-10-12.md。
struct AdmittedLocalFile {
    file: async_fs::File,
    coordination: Arc<FileCoordinationState>,
}

impl core::fmt::Debug for AdmittedLocalFile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("AdmittedLocalFile").finish_non_exhaustive()
    }
}

impl core::fmt::Display for AdmittedLocalFile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("AdmittedLocalFile")
    }
}

// EXO-ACQ-F01：只接收已按角色非破坏性打开的候选；保护实际对象而非路径。
// 普通入口在PSA切片消费本出口；原公开IO/角色不变，独占owner装配另闭合。
fn acquire_opened_local_file(
    locator: PathBuf,
    file: async_fs::File,
    coordination: Arc<FileCoordinationState>,
    kind: OpenAdmissionKind,
) -> impl Future<Output = pi_result::Result<AdmittedLocalFile>> + Send + 'static {
    // 从调用即成组持有；未poll的Future也先关闭file、后释放活动核心。
    let candidate = (file, coordination);
    async move {
        let root_path = match crate::coordination_root_config::frozen_cross_process_coordination_root() {
            crate::CrossProcessCoordinationRoot::Custom(path) => path.clone(),
            crate::CrossProcessCoordinationRoot::FileParent => {
                if matches!(&kind, OpenAdmissionKind::Shared) {
                    let (file, coordination) = candidate;
                    return Ok(AdmittedLocalFile { file, coordination });
                }
                return Err(OpenAcquisitionError::CommonRootRequired).into_classified_error();
            }
        };
        // 取消只是停止等待；这份真实完成链保活候选至准备及ensure都结束。
        // 只在Custom冷取得提交任务，不给任何数据操作热路径增加资格工作。
        let task = async_global_executor::spawn(async move {
            let (candidate, prepared) = match bounded_blocking::unblock_with_input(candidate, move |candidate| {
                let prepared = (|| {
                    validate_coordination_filesystem(&locator)?;
                    validate_coordination_filesystem(&root_path)?;
                    let root = Arc::new(CoordinationRootAnchor::open(root_path)?);
                    // 仅复制身份保活引用，不增加数据权限、不按路径重开创建对象。
                    // 这个引用永远不读写/seek，不能用它建立任何追加数据通道。
                    #[cfg(unix)]
                    let pin = {
                        use std::os::unix::io::AsFd;
                        std::fs::File::from(candidate.0.as_fd().try_clone_to_owned().into_classified_error()?)
                    };
                    #[cfg(windows)]
                    let pin = std::fs::File::from(candidate.0.as_handle().try_clone_to_owned().into_classified_error()?);
                    #[cfg(not(any(unix, windows)))]
                    let pin: std::fs::File = {
                        return Err(OpenAcquisitionError::UnsupportedPlatform).into_classified_error();
                    };
                    Ok::<_, pi_result::Error>((root, pin))
                })();
                (candidate, prepared)
            }).await {
                Ok(value) => value,
                Err((error, _candidate)) => return Err(error),
            };
            let (root, pin) = prepared?;
            candidate.1.open_admission.get_or_init(|| {
                OpenAdmissionState::new(candidate.1.identity.clone())
            }).ensure(root, pin, kind).await?;
            let (file, coordination) = candidate;
            Ok(AdmittedLocalFile { file, coordination })
        });
        DetachOnDrop::new(task).await
    }
}

// PSA-F01/PSA-DIAG-01：整个冷打开由同一拥有型完成者推进。名称预约不得
// 留在可取消的外层Future，否则Custom候选准备尚未结束时同名已可再次修改。
// 原角色、实际身份、旧域拒绝和登记顺序不变；只在资源取得时分离任务。
async fn open_local_file(
    core: &'static LocalNamespaceCore,
    locator: PathBuf,
    access: FileAccessMode,
    kind: OpenAdmissionKind,
) -> pi_result::Result<LocalFile> {
    let entry_reservation = acquire_namespace_entry_reservation(core, &locator)
        .await?;
    let path_metadata = async_fs::metadata(&locator).await.into_classified_error()?;
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

        // 防止路径在预检后被替换成FIFO等会阻塞open的特殊对象。
        // 对常规文件，O_NONBLOCK不改变后续读写语义。
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
    let protocol_locator = locator.clone();
    bounded_blocking::unblock_result(move || {
        reject_persisted_cross_process_domain(&protocol_locator)
    })
    .await?;
    let coordination = core.registry.coordination_for(identity).await?;
    let candidate = acquire_opened_local_file(
        locator, file, coordination, kind,
    ).await?;
    let resource_id = core.allocate_resource_id()?;
    candidate.coordination.register_ordinary_resource()?;

    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(candidate.file))),
        access,
        read_position: 0,
        coordination: candidate.coordination,
        coordinated_lease: None,
        resource_id,
    })
}

// CMP-F03：只消费已成功取得EX的内部资源能力，不向下传公开owner类型。
// owner的冷引用覆盖整个真实完成链，尤其保留旧协议原活动租约，直到独立
// 通道接管；不能只克隆coordination，否则owner先Drop可能提前撤销旧生命周期锁。
// 每次真实独立open，绝不复制/共享追加数据通道；资格槽不再次竞争或升级。
// 本出口自己接管拥有型完成任务；后续公开派生直接await，不叠加相同任务。
async fn open_local_file_from_exclusive_resource(
    namespace: &'static LocalNamespaceCore,
    owner: Arc<LocalFile>,
    locator: PathBuf,
    access: FileAccessMode,
) -> pi_result::Result<LocalFile> {
    let task = async_global_executor::spawn(async move {
        open_local_file_from_exclusive_resource_inner(namespace, owner, locator, access).await
    });
    DetachOnDrop::new(task).await
}

// 冷派生叶子只供上面真实拥有型任务调用，不能直接交付给可取消的外部Future。
async fn open_local_file_from_exclusive_resource_inner(
    namespace: &'static LocalNamespaceCore,
    owner: Arc<LocalFile>,
    locator: PathBuf,
    access: FileAccessMode,
) -> pi_result::Result<LocalFile> {
    if !core::ptr::eq(owner.coordination.registry, &namespace.registry) {
        return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict));
    }
    let entry_reservation = acquire_namespace_entry_reservation(namespace, &locator).await?;
    let authority = owner.cross_process_authority();
    let protocol_locator = locator.clone();
    let binding = authority.as_ref().map(|authority| authority.binding.clone());
    bounded_blocking::unblock_result(move || match binding {
        Some(binding) => validate_published_protocol_at(&protocol_locator, &binding),
        None => reject_persisted_cross_process_domain(&protocol_locator),
    }).await?;
    if !async_fs::metadata(&locator).await.into_classified_error()?.is_file() {
        return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Unsupported));
    }
    let mut options = async_fs::OpenOptions::new();
    match &access {
        FileAccessMode::Read | FileAccessMode::ReadMmap => { options.read(true); }
        FileAccessMode::Append => { options.append(true); }
        FileAccessMode::Overwrite | FileAccessMode::Truncate => { options.write(true); }
        FileAccessMode::ReadWriteMmap => { options.read(true).write(true); }
    }
    #[cfg(unix)]
    {
        use async_fs::unix::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(&locator).await.into_classified_error()?;
    let metadata = file.metadata().await.into_classified_error()?;
    if !metadata.is_file() {
        return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Unsupported));
    }
    let (file, identity) = stable_file_identity(file, &metadata).await?;
    if identity != owner.coordination.identity {
        return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict));
    }
    let coordination = Arc::clone(&owner.coordination);
    let coordinated_lease = match authority {
        Some(authority) => Some(CoordinatedResourceLease::acquire(
            &authority, coordination.try_reserve_coordinated_resource(&authority)?,
        ).await?),
        None => None,
    };
    let resource_id = namespace.allocate_resource_id()?;
    if coordinated_lease.is_none() {
        coordination.register_ordinary_resource()?;
    }
    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(file))),
        access,
        read_position: 0,
        coordination,
        coordinated_lease,
        resource_id,
    })
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
    // PSA-F02：真实新建文件参与SH；后续失败仍报告已经创建，不盲删目标。
    let candidate = acquire_opened_local_file(
        locator.clone(), file, coordination, OpenAdmissionKind::Shared,
    ).await.map_err(post_create_failure)?;
    let resource_id = core
        .allocate_resource_id()
        .map_err(post_create_failure)?;
    candidate.coordination
        .register_ordinary_resource()
        .map_err(post_create_failure)?;

    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(candidate.file))),
        access,
        read_position: 0,
        coordination: candidate.coordination,
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
    // 原创建句柄保活同一新对象；即使资格失败，仍保留原Published/创建证据。
    let candidate = acquire_opened_local_file(
        locator.clone(), creation_file, coordination, OpenAdmissionKind::Shared,
    ).await.map_err(after_create_failure)?;
    let initialization = candidate.coordination
        .try_begin_authority_initialization()
        .map_err(after_create_failure)?;
    let namespace_identity = namespace as *const LocalNamespaceCore as usize;
    let authority = if let Some(existing) =
        candidate.coordination.existing_cross_process_core()
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
            ))
        }
    } else {
        Arc::new(CrossProcessAuthorityCore::new(
            namespace_identity,
            locator.clone(),
            binding,
        ))
    };
    initialization
        .complete(&authority)
        .map_err(after_create_failure)?;
    let coordinated_lease = CoordinatedResourceLease::acquire(
        &authority, candidate.coordination.try_reserve_coordinated_resource(&authority).map_err(after_create_failure)?,
    )
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

    drop(candidate.file);
    drop(bootstrap);
    drop(entry_reservation);
    let file = LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(file))),
        access,
        read_position: 0,
        coordination: candidate.coordination,
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
            ))
        }
    } else {
        Arc::new(CrossProcessAuthorityCore::new(
            namespace_identity,
            locator,
            binding,
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
    kind: OpenAdmissionKind,
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
    if authority.retired.load(AtomicOrdering::Acquire) {
        return Err(pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict));
    }
    let coordination = namespace.registry.coordination_for(authority.binding.target_identity.clone()).await?;
    let authority = coordination.attach_cross_process_authority(authority)?;
    let coordinated_lease = CoordinatedResourceLease::acquire(
        &authority, coordination.try_reserve_coordinated_resource(&authority)?,
    ).await?;

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
    let candidate = acquire_opened_local_file(
        locator, file, coordination, kind,
    ).await?;
    let resource_id = namespace.allocate_resource_id()?;
    drop(entry_reservation);
    Ok(LocalFile {
        file: Arc::new(async_lock::Mutex::new(Some(candidate.file))),
        access,
        read_position: 0,
        coordination: candidate.coordination,
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
    let mut plan = bounded_blocking::unblock_result(move || {
        prepare_authorized_removal(planning_authority)
    })
    .await
    .map_err(remove_failure)?;
    // 这里只在cold删除恢复凭据到活动的联系；完成者不再从凭据反取核心。
    let coordination = namespace.registry.coordination_for(authority.binding.target_identity.clone())
        .await.map_err(remove_failure)?;
    let authority = coordination.attach_cross_process_authority(authority).map_err(remove_failure)?;
    plan.authority = Arc::clone(&authority);
    let process_removal = coordination.try_acquire_coordinated_removal(&authority)
        .map_err(remove_failure)?;
    let result = match bounded_blocking::unblock(move || {
        finish_authorized_removal(plan)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Err(remove_failure(error)),
    };
    if result.is_ok() {
        coordination.clear_cross_process_binding(&authority.binding);
    }
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
    if let Some(mut file) = file {
        // PSA-LIF-01：异步File的Drop不能抢占已进入同步write的后台任务。
        // 先等待Writing归位，再关闭数据句柄；原身份清理和资格租约仍在外层。
        // 即使收尾flush失败，下层也已取回真实写者；保留原transfer首错及双证据，
        // 不把失败暂存内容持久化作为新合同。真实公开复现见同名问题条目与
        // test_local_public_acquisition_copy_failure_closes_real_writer_before_shared_release。
        let _ = file.flush().await;
        drop(file);
    }
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
        // PSA-F03：私有暂存也是实际内容资源，先SH后原排它写许可；资格
        // 失败已关闭候选，继续只按已确认身份清理，不吞掉真实创建证据。
        let candidate = match acquire_opened_local_file(
            path.clone(), file, coordination, OpenAdmissionKind::Shared,
        ).await {
            Ok(candidate) => candidate,
            Err(error) => {
                let evidence = cleanup_local_copy_staging_resource(path, identity, None).await;
                return Err((error, evidence));
            }
        };
        let exclusive_lease = match candidate.coordination.try_acquire_staging_exclusive() {
            Ok(lease) => lease,
            Err(error) => {
                let evidence = cleanup_local_copy_staging_resource(
                    path,
                    identity,
                    Some(candidate.file),
                )
                .await;
                return Err((error, evidence));
            }
        };

        return Ok(LocalCopyStaging {
            path,
            identity,
            file: Some(candidate.file),
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
    owner: Option<Arc<LocalFile>>,
) -> pi_result::RawResult<CopyOutcome, CopyFailure> {
    // CMP-F04：这里只继承已取得EX的显式内部能力；普通调用为None。
    // 临时owner引用保留原协议活动租约到真实复制完成，不在传输循环查资格。
    if owner.as_ref().is_some_and(|owner| !core::ptr::eq(owner.coordination.registry, &core.registry)) {
        return Err(local_copy_failure(
            pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict),
            CopyStagingEvidence::NotCreatedByOperation,
        ));
    }
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
    let source_authority = owner.as_ref().and_then(|owner| owner.cross_process_authority());
    let source_binding = source_authority.as_ref().map(|authority| authority.binding.clone());
    bounded_blocking::unblock_result(move || {
        match source_binding {
            Some(binding) => validate_published_protocol_at(&protocol_source, &binding),
            None => reject_persisted_cross_process_domain(&protocol_source),
        }
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
    let (source_file, source_identity) = stable_file_identity(
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
    let source_coordination = match owner.as_ref() {
        Some(owner) => {
            if source_identity != owner.coordination.identity {
                return Err(local_copy_failure(
                    pi_result::error_stack::Report::new(pi_result::ErrorKind::Conflict),
                    CopyStagingEvidence::NotCreatedByOperation,
                ));
            }
            Arc::clone(&owner.coordination)
        }
        None => core.registry.coordination_for(source_identity).await.map_err(|error| {
            local_copy_failure(error, CopyStagingEvidence::NotCreatedByOperation)
        })?,
    };
    // 整组源候选覆盖预读停止；不能只保留读许可而提前拆掉实际数据文件。
    let mut source_candidate = if owner.is_some() {
        AdmittedLocalFile { file: source_file, coordination: source_coordination }
    } else {
        acquire_opened_local_file(
            source.clone(), source_file, source_coordination, OpenAdmissionKind::Shared,
        ).await.map_err(|error| local_copy_failure(error, CopyStagingEvidence::NotCreatedByOperation))?
    };
    let source_lease = if owner.is_some() {
        source_candidate.coordination.try_acquire_content_read()
    } else {
        source_candidate.coordination.try_acquire_uncoordinated_content_read()
    }.map_err(|error| {
            local_copy_failure(
                error,
                CopyStagingEvidence::NotCreatedByOperation,
            )
        })?;
    let source_cross_process_lease = match source_authority {
        Some(authority) => Some(acquire_cross_process_read_operation(&authority, &source_candidate.coordination)
            .await.map_err(|error| local_copy_failure(error, CopyStagingEvidence::NotCreatedByOperation))?),
        None => None,
    };
    drop(source_reservation);

    let content_byte_len = match source_candidate.file.metadata().await.into_classified_error() {
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
            drop(source_candidate);
            drop(source_lease);
            drop(source_cross_process_lease);
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
                &mut source_candidate.file,
                staging_file,
                content_byte_len,
                &cancelled,
            )
            .await,
            Err(error) => Err(error),
        }
    };
    let quiesce_result = source_candidate.file
        .seek(SeekFrom::Current(0))
        .await
        .map(|_| ())
        .into_classified_error();
    drop(source_candidate);
    drop(source_lease);
    drop(source_cross_process_lease);

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

// CMP-F04-O01：普通和显式EX来源共用唯一生产取消/完成边界。这里只移入
// 原copy_new既有任务和标志；取消不提前撤owner，未提交发布继续按原标志拒绝。
async fn copy_local_file_owned(
    core: &'static LocalNamespaceCore,
    source: PathBuf,
    destination: PathBuf,
    owner: Option<Arc<LocalFile>>,
) -> pi_result::RawResult<CopyOutcome, CopyFailure> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let task_cancelled = Arc::clone(&cancelled);
    let task = async_global_executor::spawn(async move {
        copy_local_file(core, source, destination, task_cancelled, owner).await
    });
    CancelCopyOnDrop::new(task, cancelled).await
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

/// 一个已有本地文件的、不透明的文件独占所有者。
///
/// 由[`FileNamespace::open_exclusive`]或
/// [`FileNamespace::open_exclusive_with_cross_process_authority`]成功返回。
/// 取得时验证目标的读写权限，但不创建、截断或修改内容。它本身不实现
/// [`FileIo`]；内容访问必须通过[`FileNamespace::open_with_exclusive_owner`]
/// 显式派生一个具有唯一访问角色的独立文件资源。源复制使用
/// [`FileNamespace::copy_new_with_exclusive_owner`]。
///
/// # 独占范围与使用前提
///
/// 所有合作参与者必须在首次文件资源操作前设置同一物理`Custom`协调根，
/// 并保持协调位置可信且可用。独占存在期间，本库协调域内其它普通或独占
/// 打开会失败，包括同进程、同线程、另一个命名空间句柄及同对象的硬链接。
/// 它不强制阻止绕过本库协议的原生访问，不永久占有路径名称，不自动授予
/// 跨进程安全映射，也不改变改名、非协调删除的既有边界。
///
/// # 所有权、并发与释放
///
/// 本类型自动满足`Send + Sync`，可在线程间移动或共享借用，但不实现
/// `Clone`、`Copy`、`Default`、判等、排序、哈希或序列化。无公开构造、
/// 裸句柄、解包或提前解锁方法。`Debug`与`Display`只输出脱敏类型标签。
///
/// 释放此值不会使已派生文件、映射或实际在途操作失效；独占资格持续到
/// 最后一个实际拥有者释放。Drop不隐式刷新、修改、删除文件，也不保证
/// 物理页缓存立即归还。独占与访问角色正交，原有映射相交、覆盖、截断及
/// 安全删除等限制仍然适用，不能借独占绕过它们。
///
/// 类型自身只向输出端写入固定标签，工作量和额外空间为`O(1)`，不主动查询
/// 外部状态；调用方提供的格式化输出端可以执行代码、I/O或返回错误，其成本
/// 和副作用另计。取得、
/// 派生及复制的副作用、取消和成本见对应方法；测试入口为
/// `tests/local_exclusive_open.rs`。合法安全使用不引入未定义行为，调用
/// `unsafe`映射入口时仍必须满足该入口自己的安全前提。
///
/// # 类型约束示例
///
/// 所有者可移动和共享借用，但不能复制资源、构造默认值或直接执行文件I/O。
/// 下列编译期例子不打开文件，也不修改全局配置。
///
/// ```
/// use pi_async_fs::{ExclusiveFileOwner, UnsupportedExclusiveFileOwner};
/// fn thread_owned<T: Send + Sync + 'static>() {}
/// thread_owned::<ExclusiveFileOwner>();
/// thread_owned::<UnsupportedExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_copy<T: Copy>() {}
/// requires_copy::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_default<T: Default>() {}
/// requires_default::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_equality<T: Eq>() {}
/// requires_equality::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_order<T: Ord>() {}
/// requires_order::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::ExclusiveFileOwner;
/// fn requires_hash<T: std::hash::Hash>() {}
/// requires_hash::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::{ExclusiveFileOwner, FileIo};
/// fn requires_io<T: FileIo>() {}
/// requires_io::<ExclusiveFileOwner>();
/// ```
///
/// ```compile_fail
/// use pi_async_fs::ExclusiveFileOwner;
/// let _ = ExclusiveFileOwner {};
/// ```
pub struct ExclusiveFileOwner {
    // PUB-T01：封装已验收CMP原EX资源；不公开FileIo或内部解锁能力，
    // 不向原内容热路径加入资格字段。实际派生完成者在冷入口临时保活此资源。
    resource: Arc<LocalFile>,
}

impl core::fmt::Debug for ExclusiveFileOwner {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("ExclusiveFileOwner").finish_non_exhaustive()
    }
}

impl core::fmt::Display for ExclusiveFileOwner {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("ExclusiveFileOwner(<redacted>)")
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
                    Some(acquire_cross_process_shared_content_operation(&authority, &self.coordination).await?)
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
                Some(authority) => match acquire_cross_process_append_operation(&authority, &self.coordination).await {
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
                // LIF-DEF-01：先归还/释放数据槽，再撤活动；取消且外部已Drop时
                // 这里关闭最后数据资源，不让资格早于真实文件槽释放。
                drop(file_guard);
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
                Some(authority) => match acquire_cross_process_exclusive_operation(&authority, &self.coordination).await {
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
                // LIF-DEF-01：已有析构前移，不新增资格工作；详见本层寿命归档。
                drop(file_guard);
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
                    acquire_cross_process_exclusive_operation(&authority, &self.coordination).await?,
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
                // LIF-DEF-01：实际截断结束并归还数据槽后，才结束活动租约。
                drop(file_guard);
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
            let cross_process_mapping = acquire_cross_process_mapping_operation(
                &authority, &self.coordination, start, end_exclusive,
            ).await?;
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
            let cross_process_mapping = acquire_cross_process_mapping_operation(
                &authority, &self.coordination, start, end_exclusive,
            ).await?;
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
                    acquire_cross_process_shared_content_operation(&authority, &self.coordination).await?,
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
                // LIF-DEF-01：同步已真实结束，先释放数据槽、后释放刷新/协调活动。
                drop(file_guard);
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
    type ExclusiveOwner = ExclusiveFileOwner;

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
            // PSA-F01：首次资源尝试即固定策略；失败也不能在稍后另启资格域。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                open_local_file(core, locator, access, OpenAdmissionKind::Shared).await
            });
            DetachOnDrop::new(task).await
        }
    }

    fn open_exclusive<'a>(
        &'a self,
        locator: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::Result<Self::ExclusiveOwner>> + Send + 'a {
        async move {
            // PUB-F01：公开首poll固定策略；原拥有任务覆盖完整CMP-F01。
            // owner只包装已完成的EX资源，普通文件不能被外部伪装成所有者。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            let core = self.core;
            let locator = locator.clone();
            let task = async_global_executor::spawn(async move {
                open_local_file(core, locator, FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive).await
            });
            let resource = DetachOnDrop::new(task).await?;
            Ok(ExclusiveFileOwner { resource: Arc::new(resource) })
        }
    }

    fn open_exclusive_with_cross_process_authority<'a>(
        &'a self,
        authority: &'a Self::CrossProcessAuthority,
    ) -> impl Future<Output = pi_result::Result<Self::ExclusiveOwner>> + Send + 'a {
        async move {
            // PUB-F02：原凭据核验与活动租约仍由CMP-F02负责；公开壳只拥有
            // 输入并提交完整冷任务，不把凭据本身当独占或安全映射的新增授权。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            let namespace = self.core;
            let authority = Arc::clone(authority.local_core());
            let task = async_global_executor::spawn(async move {
                open_local_file_with_cross_process_authority(
                    namespace, authority, FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive,
                ).await
            });
            let resource = DetachOnDrop::new(task).await?;
            Ok(ExclusiveFileOwner { resource: Arc::new(resource) })
        }
    }

    fn open_with_exclusive_owner<'a>(
        &'a self,
        owner: &'a Self::ExclusiveOwner,
        locator: &'a Self::Locator,
        access: FileAccessMode,
    ) -> impl Future<Output = pi_result::Result<Self::File>> + Send + 'a {
        async move {
            // PUB-F03：只从不透明owner取得原EX资源；CMP-F03自己拥有完整
            // 冷完成任务，禁止此处重复spawn或只把coordination传给异步叶子。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            open_local_file_from_exclusive_resource(
                self.core, Arc::clone(&owner.resource), locator.clone(), access,
            ).await
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
            // PSA-F02：公开首poll固定，不让内部任务调度延后配置线性化。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
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
            // PSA-F03：复制源/暂存参与同一域，首次poll就固定唯一根策略。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            let core = self.core;
            let source = source.clone();
            let destination = destination.clone();
            copy_local_file_owned(core, source, destination, None).await
        }
    }

    fn copy_new_with_exclusive_owner<'a>(
        &'a self,
        owner: &'a Self::ExclusiveOwner,
        source: &'a Self::Locator,
        destination: &'a Self::Locator,
    ) -> impl Future<Output = pi_result::RawResult<CopyOutcome, CopyFailure>> + Send + 'a {
        async move {
            // PUB-F04：共同复制完成入口已有取消标志/拥有任务；只在冷壳
            // 显式传原EX资源，原源身份/长度/暂存/取消/发布/双证据全由其负责。
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            copy_local_file_owned(
                self.core, source.clone(), destination.clone(), Some(Arc::clone(&owner.resource)),
            ).await
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
            crate::coordination_root_config::frozen_cross_process_coordination_root();
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
            crate::coordination_root_config::frozen_cross_process_coordination_root();
            let namespace = self.core;
            let authority = Arc::clone(authority.local_core());
            let task = async_global_executor::spawn(async move {
                open_local_file_with_cross_process_authority(
                    namespace,
                    authority,
                    access,
                    OpenAdmissionKind::Shared,
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

#[cfg(all(test, any(target_os = "linux", windows)))]
mod exclusive_open_tests {
    use super::*;
    use crate::open_admission_native::{CoordinationRootAnchor, OpenAdmissionKind, OwnedOpenAdmission};
    use futures_lite::future::block_on;

    // 真实小夹具不代理生产功能；Linux磁盘临时区自动清理，Windows项目target。
    fn fixture() -> tempfile::TempDir {
        #[cfg(target_os = "linux")]
        { tempfile::Builder::new().prefix("pi-async-fs-acquire-").tempdir().unwrap() }
        #[cfg(windows)]
        {
            let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("exclusive-open-fixtures");
            std::fs::create_dir_all(&directory).unwrap();
            tempfile::Builder::new().prefix("acquire-").tempdir_in(directory).unwrap()
        }
    }

    // 每用例只隔离配置，不重置生产全局状态；失败/超时都回收子进程。
    fn isolated(test_name: &str) -> bool {
        const ENVIRONMENT: &str = "PI_ASYNC_FS_EXCLUSIVE_OPEN_TEST_CHILD";
        if std::env::var(ENVIRONMENT).as_deref() == Ok(test_name) { return false; }
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(format!("local::exclusive_open_tests::{test_name}"))
            .arg("--exact").arg("--nocapture")
            .env(ENVIRONMENT, test_name)
            .env("BLOCKING_MAX_THREADS", "1")
            .stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::piped())
            .spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while child.try_wait().unwrap().is_none() {
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("exclusive child timed out: {}", String::from_utf8_lossy(&output.stderr));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.status.success(), "child failed\n{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("1 passed; 0 failed; 0 ignored"));
        assert!(stdout.contains("EXCLUSIVE_CHILD_VERIFIED"));
        print!("{stdout}");
        true
    }

    // 只准备既有打开结果/真实身份/实际注册表对象，供获准私有接缝使用。
    async fn opened(path: &Path) -> (async_fs::File, Arc<FileCoordinationState>) {
        let file = async_fs::File::open(path).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        let (file, identity) = stable_file_identity(file, &metadata).await.unwrap();
        let namespace = LocalFileNamespace::new();
        let coordination = namespace.core.registry.coordination_for(identity).await.unwrap();
        (file, coordination)
    }

    // 按冻结六角色准备真实拥有型输入，不实现公开open或任何文件IO行为。
    async fn opened_role(path: &Path, access: FileAccessMode) -> (async_fs::File, Arc<FileCoordinationState>) {
        let mut options = async_fs::OpenOptions::new();
        match access {
            FileAccessMode::Read | FileAccessMode::ReadMmap => { options.read(true); }
            FileAccessMode::Append => { options.append(true); }
            FileAccessMode::Overwrite | FileAccessMode::Truncate => { options.write(true); }
            FileAccessMode::ReadWriteMmap => { options.read(true).write(true); }
        }
        let file = options.open(path).await.unwrap();
        let metadata = file.metadata().await.unwrap();
        let (file, identity) = stable_file_identity(file, &metadata).await.unwrap();
        let namespace = LocalFileNamespace::new();
        let coordination = namespace.core.registry.coordination_for(identity).await.unwrap();
        (file, coordination)
    }

    // ACQ-T01：候选输出持有真实资格，独立原生EX探针被拒绝；候选最后
    // Drop后原核心回收/EX可取得。文件内容和固定空载体独立核验。
    #[test]
    fn test_local_open_acquisition_owns_qualification_until_candidate_drop() {
        if isolated("test_local_open_acquisition_owns_qualification_until_candidate_drop") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"acquisition-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let (file, coordination) = block_on(opened(&path));
        let weak = Arc::downgrade(&coordination);
        let candidate = block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap();
        let root = Arc::new(CoordinationRootAnchor::open(root_path.clone()).unwrap());
        assert_eq!(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::read(&path).unwrap(), b"acquisition-original");
        drop(candidate);
        assert!(weak.upgrade().is_none(), "no inactive credential or history keeps this object alive");
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("candidate drop releases qualification"));
        let entries = std::fs::read_dir(&root_path).unwrap().collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].as_ref().unwrap().metadata().unwrap().len(), 0);
        println!("EXCLUSIVE_CHILD_VERIFIED candidate-drop");
    }

    // ACQ-T04/T06：仅候选接缝的未poll不固定策略；默认普通取得不创建
    // 协调资产，实际首次取得固定默认，之后不能改Custom。默认独占明确拒绝。
    #[test]
    fn test_local_open_acquisition_default_shared_is_lazy_and_exclusive_is_unsupported() {
        if isolated("test_local_open_acquisition_default_shared_is_lazy_and_exclusive_is_unsupported") { return; }
        fn traits<T: Send + Sync + 'static>() {}
        traits::<AdmittedLocalFile>();
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"default-original").unwrap();
        let (file, coordination) = block_on(opened(&path));
        drop(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared));
        assert!(crate::coordination_root_config::configured_cross_process_coordination_root().is_none());
        let (file, coordination) = block_on(opened(&path));
        let candidate = block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared))
            .expect("default ordinary candidate preserves prior behavior");
        assert_eq!(format!("{candidate:?}"), "AdmittedLocalFile { .. }");
        assert_eq!(format!("{candidate}"), "AdmittedLocalFile");
        println!("ACQUISITION_LAYOUT coordination={} inline_slot={} candidate={}",
            core::mem::size_of::<FileCoordinationState>(),
            core::mem::size_of::<OnceLock<OpenAdmissionState>>(),
            core::mem::size_of::<AdmittedLocalFile>());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        assert_eq!(crate::coordination_root_config::configured_cross_process_coordination_root(), Some(&crate::CrossProcessCoordinationRoot::FileParent));
        assert_eq!(crate::set_cross_process_coordination_root(
            crate::CrossProcessCoordinationRoot::Custom(temporary.path().join("late-root")),
        ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        let (file, coordination) = block_on(opened(&path));
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Exclusive))
            .unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), b"default-original");
        drop(candidate);
        println!("EXCLUSIVE_CHILD_VERIFIED default-and-lazy");
    }

    // ACQ-T02：独占/共享拒绝覆盖同路径及跨目录硬链接；错误真实文件
    // 不能套用别的核心。每种失败都保持原所有者/内容，最后Drop后真实可重试。
    #[test]
    fn test_local_open_acquisition_conflicts_follow_actual_objects_and_reject_wrong_core() {
        if isolated("test_local_open_acquisition_conflicts_follow_actual_objects_and_reject_wrong_core") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let other = temporary.path().join("other.bin");
        let alias_directory = temporary.path().join("aliases");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::create_dir(&alias_directory).unwrap();
        let alias = alias_directory.join("alias.bin");
        std::fs::write(&path, b"owned-original").unwrap();
        std::fs::write(&other, b"other-original").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let (file, coordination) = block_on(opened_role(&path, FileAccessMode::ReadWriteMmap));
        let exclusive = block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Exclusive)).unwrap();
        for locator in [&path, &alias] {
            for kind in [OpenAdmissionKind::Shared, OpenAdmissionKind::Exclusive] {
                let (file, coordination) = block_on(opened(locator));
                assert_eq!(block_on(acquire_opened_local_file(locator.clone(), file, coordination, kind))
                    .unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
            }
        }
        drop(exclusive);
        let (file, coordination) = block_on(opened(&alias));
        let first_shared = block_on(acquire_opened_local_file(alias.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap();
        let (file, coordination) = block_on(opened(&path));
        let second_shared = block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap();
        let (wrong_file, wrong_coordination) = block_on(opened(&other));
        let rejected = block_on(acquire_opened_local_file(other.clone(), wrong_file, Arc::clone(&first_shared.coordination), OpenAdmissionKind::Shared))
            .unwrap_err();
        assert_eq!(rejected.current_context(), &pi_result::ErrorKind::Conflict);
        drop(wrong_coordination);
        drop(first_shared);
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        assert_eq!(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(second_shared);
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("last shared candidate release"));
        assert_eq!(std::fs::read(&path).unwrap(), b"owned-original");
        assert_eq!(std::fs::read(&other).unwrap(), b"other-original");
        println!("EXCLUSIVE_CHILD_VERIFIED aliases-and-conflicts");
    }

    // ACQ-T03：六种真实数据打开权限都能取得SH，保持同对象资格至最后
    // 候选释放；输入均独立open，测试不克隆数据通道、不执行模拟文件IO。
    #[test]
    fn test_local_open_acquisition_accepts_all_existing_role_permissions() {
        if isolated("test_local_open_acquisition_accepts_all_existing_role_permissions") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"roles-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let mut candidates = Vec::new();
        for role in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
            FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap] {
            let (file, coordination) = block_on(opened_role(&path, role));
            candidates.push(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap());
        }
        let root = Arc::new(CoordinationRootAnchor::open(root_path.clone()).unwrap());
        for _ in 0..5 {
            drop(candidates.pop().unwrap());
            assert_eq!(OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        }
        drop(candidates);
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("last of all role candidates releases qualification"));
        assert_eq!(std::fs::read(&path).unwrap(), b"roles-original");
        assert_eq!(std::fs::read_dir(root_path).unwrap().count(), 1);
        println!("EXCLUSIVE_CHILD_VERIFIED role-permissions");
    }

    // ACQ-T03：真实Linux权限0222使新只读打开失败，但只追加打开可用；
    // 候选取得必须不再申请读权限。先恢复权限再断言，避免失败污染夹具清理。
    #[cfg(target_os = "linux")]
    #[test]
    fn test_local_open_acquisition_write_only_file_requires_no_additional_read_permission() {
        if isolated("test_local_open_acquisition_write_only_file_requires_no_additional_read_permission") { return; }
        use std::os::unix::fs::PermissionsExt;
        let temporary = fixture();
        let path = temporary.path().join("write-only.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"write-only-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let permissions = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o222)).unwrap();
        let read_attempt = std::fs::File::open(&path);
        let acquired = block_on(async {
            let (file, coordination) = opened_role(&path, FileAccessMode::Append).await;
            acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared).await
        });
        std::fs::set_permissions(&path, permissions).unwrap();
        assert_eq!(read_attempt.unwrap_err().kind(), std::io::ErrorKind::PermissionDenied,
            "test requires a real unprivileged process and denied read permission");
        drop(acquired.expect("append-only candidate must not request additional read access"));
        assert_eq!(std::fs::read(&path).unwrap(), b"write-only-original");
        println!("EXCLUSIVE_CHILD_VERIFIED no-extra-read-permission");
    }

    // ACQ-T05：同一Custom配置经过不存在→普通文件→真实目录；错误不回滚
    // 配置、不回退、不改源文件。非空载体拒绝且原样保留，离线修复后可重试。
    #[test]
    fn test_local_open_acquisition_root_and_carrier_failures_are_retryable_without_fallback() {
        if isolated("test_local_open_acquisition_root_and_carrier_failures_are_retryable_without_fallback") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::write(&path, b"failure-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let (file, coordination) = block_on(opened(&path));
        let retained = Arc::clone(&coordination);
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared))
            .unwrap_err().current_context(), &pi_result::ErrorKind::NotFound);
        assert!(!root_path.exists());
        std::fs::write(&root_path, b"not-a-directory").unwrap();
        let file = block_on(async_fs::File::open(&path)).unwrap();
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, Arc::clone(&retained), OpenAdmissionKind::Shared))
            .unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        assert_eq!(std::fs::read(&root_path).unwrap(), b"not-a-directory");
        std::fs::remove_file(&root_path).unwrap();
        std::fs::create_dir(&root_path).unwrap();
        let file = block_on(async_fs::File::open(&path)).unwrap();
        drop(block_on(acquire_opened_local_file(path.clone(), file, retained, OpenAdmissionKind::Shared))
            .expect("same object retries after directory correction"));
        let carriers = std::fs::read_dir(&root_path).unwrap().map(|entry| entry.unwrap().path()).collect::<Vec<_>>();
        assert_eq!(carriers.len(), 1);
        std::fs::write(&carriers[0], b"corrupt-carrier").unwrap();
        let (file, coordination) = block_on(opened(&path));
        let retained = Arc::clone(&coordination);
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared))
            .unwrap_err().current_context(), &pi_result::ErrorKind::Corrupted);
        assert_eq!(std::fs::read(&carriers[0]).unwrap(), b"corrupt-carrier");
        std::fs::write(&carriers[0], []).unwrap();
        let file = block_on(async_fs::File::open(&path)).unwrap();
        drop(block_on(acquire_opened_local_file(path.clone(), file, retained, OpenAdmissionKind::Shared))
            .expect("same state retries after offline carrier correction"));
        assert_eq!(std::fs::read(&path).unwrap(), b"failure-original");
        assert_eq!(std::fs::metadata(&carriers[0]).unwrap().len(), 0);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 2);
        assert_eq!(crate::coordination_root_config::configured_cross_process_coordination_root(),
            Some(&crate::CrossProcessCoordinationRoot::Custom(root_path)));
        println!("EXCLUSIVE_CHILD_VERIFIED root-and-carrier-failures");
    }

    // ACQ-T05：Linux真实procfs/Windows明确不支持的UNC根在建立资格前
    // 拒绝；源目录不出现替代载体，不能偷偷回退同级目录以伪装成功。
    #[test]
    fn test_local_open_acquisition_unsupported_root_never_falls_back() {
        if isolated("test_local_open_acquisition_unsupported_root_never_falls_back") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"unsupported-original").unwrap();
        #[cfg(target_os = "linux")]
        let unsupported = PathBuf::from("/proc");
        #[cfg(windows)]
        let unsupported = PathBuf::from(r"\\pi-async-fs-no-server\not-a-share\coordination");
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(unsupported)).unwrap();
        let (file, coordination) = block_on(opened(&path));
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared))
            .unwrap_err().current_context(), &pi_result::ErrorKind::Unsupported);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), b"unsupported-original");
        println!("EXCLUSIVE_CHILD_VERIFIED unsupported-no-fallback");
    }

    // ACQ-T05：真实容量一耗尽时，候选未交付/未建载体；原对象仍可在容量
    // 恢复后取得资格。只有真实同步占用工作，不用测试替代生产准入/状态。
    #[test]
    fn test_local_open_acquisition_capacity_failure_does_not_publish_and_can_retry() {
        if isolated("test_local_open_acquisition_capacity_failure_does_not_publish_and_can_retry") { return; }
        crate::set_local_blocking_capacity(NonZeroUsize::new(1).unwrap()).unwrap();
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"capacity-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let (file, coordination) = block_on(opened(&path));
        let retained = Arc::clone(&coordination);
        let (started_sender, started_receiver) = std::sync::mpsc::channel();
        let (release_sender, release_receiver) = std::sync::mpsc::channel();
        let occupied = async_global_executor::spawn(bounded_blocking::unblock(move || {
            started_sender.send(()).unwrap();
            release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
        }));
        started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
        assert_eq!(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared))
            .unwrap_err().current_context(), &pi_result::ErrorKind::ResourceExhausted);
        assert_eq!(std::fs::read_dir(&root_path).unwrap().count(), 0);
        release_sender.send(()).unwrap();
        block_on(occupied).unwrap();
        let file = block_on(async_fs::File::open(&path)).unwrap();
        let admitted = block_on(acquire_opened_local_file(path.clone(), file, retained, OpenAdmissionKind::Shared))
            .expect("same object retries after capacity returns");
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        assert_eq!(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(admitted);
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("no failed candidate keeps qualification"));
        assert_eq!(std::fs::read(&path).unwrap(), b"capacity-original");
        println!("EXCLUSIVE_CHILD_VERIFIED capacity-retry");
    }

    // ACQ-T05：取消排队中的真实候选准备，必须由实际完成链继续持有文件和
    // 原活动核心。分别覆盖尚无资格和已有SH；后者用独立原生EX确认不能早释。
    #[test]
    fn test_local_open_acquisition_cancelled_preparation_retains_the_actual_candidate() {
        if isolated("test_local_open_acquisition_cancelled_preparation_retains_the_actual_candidate") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let mut observations = Vec::new();
        for published in [false, true] {
            let path = temporary.path().join(if published { "published.bin" } else { "unpublished.bin" });
            std::fs::write(&path, b"cancel-original").unwrap();
            let initial = if published {
                let (file, coordination) = block_on(opened(&path));
                Some(block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap())
            } else { None };
            let (file, coordination) = block_on(opened(&path));
            let weak = Arc::downgrade(&coordination);
            drop(initial);
            let (started_sender, started_receiver) = std::sync::mpsc::channel();
            let (release_sender, release_receiver) = std::sync::mpsc::channel();
            let occupied = async_global_executor::spawn(blocking::unblock(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            }));
            started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            let mut acquisition = Box::pin(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared));
            assert!(block_on(futures_lite::future::poll_once(acquisition.as_mut())).is_none());
            drop(acquisition);
            let retained_before_completion = weak.upgrade().is_some();
            let native_probe = OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            );
            let qualification_retained = match native_probe {
                Ok(admission) => { drop(admission); false }
                Err(error) => {
                    assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                    true
                }
            };
            // 先排空真实任务再断言中间观测，失败也不留下阻塞worker或脏夹具。
            release_sender.send(()).unwrap();
            block_on(occupied);
            block_on(blocking::unblock(|| ()));
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while weak.upgrade().is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(weak.upgrade().is_none(), "actual completion must eventually release the candidate core");
            drop(OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).expect("actual completion releases the last qualification"));
            assert_eq!(std::fs::read(&path).unwrap(), b"cancel-original");
            observations.push((published, retained_before_completion, qualification_retained));
        }
        println!("CANCELLATION_OBSERVATIONS {observations:?}");
        for (published, retained, qualification_retained) in observations {
            assert!(retained, "cancelled preparation must retain its actual candidate core; published={published}");
            if published { assert!(qualification_retained, "existing qualification must outlive the actual preparation"); }
        }
        println!("EXCLUSIVE_CHILD_VERIFIED cancelled-owned-preparation");
    }

    // CMP-T01：生产冷取得必须真实申请EX；普通SH先到时拒绝，EX存活时
    // 六角色/硬链接的独立申请均拒绝，末尾释放后恢复普通访问且原内容不变。
    #[test]
    fn test_local_exclusive_composition_exclusive_acquisition_preserves_data_and_excludes_ordinary_roles() {
        if isolated("test_local_exclusive_composition_exclusive_acquisition_preserves_data_and_excludes_ordinary_roles") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let alias = temporary.path().join("alias.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"owner-original").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let namespace = LocalFileNamespace::new();
        let ordinary = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
        assert_eq!(block_on(open_local_file(namespace.core, alias.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
            .err().expect("existing shared resource excludes EX").current_context(), &pi_result::ErrorKind::Conflict);
        drop(ordinary);
        let owner = block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
            .expect("cold EX acquisition");
        assert_eq!(block_on(owner.byte_len()).unwrap(), 14);
        for locator in [&path, &alias] {
            for access in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
                FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap] {
                assert_eq!(block_on(namespace.open(locator, access)).err().expect("EX excludes every independent ordinary role")
                    .current_context(), &pi_result::ErrorKind::Conflict);
            }
        }
        assert_eq!(block_on(open_local_file(namespace.core, alias, FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
            .err().expect("EX is not implicitly reentrant").current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::read(&path).unwrap(), b"owner-original");
        drop(owner);
        drop(block_on(namespace.open(&path, FileAccessMode::Read)).expect("last EX release admits ordinary resources"));
        assert_eq!(std::fs::read(&path).unwrap(), b"owner-original");
        println!("EXCLUSIVE_CHILD_VERIFIED cold-exclusive-acquisition");
    }

    // CMP-T02：显式派生保留原六角色；映射/追加/刷新合法共存，普通读/
    // 覆盖/截断/同文件映射来源仍冲突。最后映射克隆跨越owner/文件Drop保活EX。
    #[test]
    fn test_local_exclusive_composition_derived_roles_preserve_mapping_matrix_and_last_owner_lifetime() {
        if isolated("test_local_exclusive_composition_derived_roles_preserve_mapping_matrix_and_last_owner_lifetime") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let alias = temporary.path().join("alias.bin");
        let other = temporary.path().join("other.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"0123456789abcdef").unwrap();
        std::fs::write(&other, b"other").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let namespace = LocalFileNamespace::new();
        let owner = Arc::new(block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap());
        assert_eq!(block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), other.clone(), FileAccessMode::Read))
            .err().expect("wrong object cannot inherit EX").current_context(), &pi_result::ErrorKind::Conflict);
        let derive = |access| block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), alias.clone(), access)).unwrap();
        let reader = derive(FileAccessMode::Read);
        let mut append = derive(FileAccessMode::Append);
        let mut append_second = derive(FileAccessMode::Append);
        let mut overwrite = derive(FileAccessMode::Overwrite);
        let mut truncate = derive(FileAccessMode::Truncate);
        let read_mapping = derive(FileAccessMode::ReadMmap);
        let mut write_mapping = derive(FileAccessMode::ReadWriteMmap);
        let mut bytes = [0_u8; 16];
        block_on(reader.read_exact_at(0, &mut bytes, ReadTargetRegion::full())).unwrap();
        assert_eq!(&bytes, b"0123456789abcdef");
        // SAFETY: 真实独占夹具的所有访问均经本库；两个映射不相交且无人截断。
        let mapping = unsafe { block_on(read_mapping.map_read_only_uncoordinated(crate::MmapRange::new(0, 4).unwrap())) }.unwrap();
        // SAFETY: 与前一映射不相交，后续仅允许本库严格尾部追加和刷新。
        let mut writable = unsafe { block_on(write_mapping.map_read_write_uncoordinated(crate::MmapRange::new(4, 8).unwrap())) }.unwrap();
        writable.as_bytes_mut().copy_from_slice(b"WXYZ");
        block_on(writable.flush()).unwrap();
        block_on(append.append(Vec::from(&b"+"[..]))).unwrap();
        block_on(append_second.append(bytes::Bytes::from_static(b"+"))).unwrap();
        block_on(append.flush(crate::FileFlushMode::Data)).unwrap();
        block_on(write_mapping.flush(crate::FileFlushMode::DataAndMetadata)).unwrap();
        assert_eq!(mapping.as_bytes(), b"0123");
        assert_eq!(writable.as_bytes(), b"WXYZ");
        assert_eq!(block_on(reader.read_exact_at(0, &mut bytes, ReadTargetRegion::full())).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(block_on(overwrite.overwrite_all(Vec::from(&b"blocked"[..]))).unwrap_err().error().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(block_on(truncate.truncate(0)).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        let failure = block_on(append.append(mapping.clone())).expect_err("same-file mapping remains an invalid append source");
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::Conflict);
        drop(failure);
        assert_eq!(block_on(namespace.remove_file(&path)).unwrap_err().error().current_context(), &pi_result::ErrorKind::Conflict);
        let last_mapping = mapping.clone();
        drop(mapping);
        drop(writable);
        drop(owner);
        drop(reader);
        drop(append);
        drop(append_second);
        drop(overwrite);
        drop(truncate);
        drop(read_mapping);
        drop(write_mapping);
        assert_eq!(block_on(namespace.open(&path, FileAccessMode::Read)).err().expect("last mapping still owns EX").current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(last_mapping.as_bytes(), b"0123");
        drop(last_mapping);
        drop(block_on(namespace.open(&path, FileAccessMode::Read)).expect("last real derived owner releases EX"));
        assert_eq!(std::fs::read(&path).unwrap(), b"0123WXYZ89abcdef++");
        assert_eq!(std::fs::read(&other).unwrap(), b"other");
        println!("EXCLUSIVE_CHILD_VERIFIED explicit-derived-matrix");
    }

    // CMP-T03：旧凭据独活不留SH，EX明确加入旧协议且派生安全映射/追加；
    // owner先释放不撤销最后映射的活动保护，旧凭据也不能删除同名新对象。
    #[test]
    fn test_local_exclusive_composition_authorized_owner_preserves_binding_and_last_mapping() {
        if isolated("test_local_exclusive_composition_authorized_owner_preserves_binding_and_last_mapping") { return; }
        let temporary = fixture();
        let path = temporary.path().join("managed.bin");
        let alias = temporary.path().join("alias.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"managed-original").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let namespace = LocalFileNamespace::new();
        // SAFETY: 独立真实夹具仅由本测试使用本库同一协调域访问。
        let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
        let ordinary = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read)).unwrap();
        assert_eq!(block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
            .err().expect("authorized SH excludes new EX").current_context(), &pi_result::ErrorKind::Conflict);
        drop(ordinary);
        let owner = Arc::new(block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap());
        assert_eq!(block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Append))
            .err().expect("credential alone does not bypass EX").current_context(), &pi_result::ErrorKind::Conflict);
        assert!(block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), alias, FileAccessMode::Read)).is_err(), "object qualification must not waive the old name binding");
        let mapper = block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), path.clone(), FileAccessMode::ReadMmap)).unwrap();
        let mut append = block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), path.clone(), FileAccessMode::Append)).unwrap();
        let mapping = block_on(mapper.map_read_only(crate::MmapRange::new(0, 7).unwrap())).unwrap();
        block_on(append.append(Vec::from(&b"+"[..]))).unwrap();
        block_on(append.flush(crate::FileFlushMode::DataAndMetadata)).unwrap();
        assert_eq!(mapping.as_bytes(), b"managed");
        assert_eq!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap_err().error().current_context(), &pi_result::ErrorKind::Conflict);
        drop(owner);
        drop(append);
        drop(mapper);
        assert_eq!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap_err().error().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::read(&path).unwrap(), b"managed-original+");
        drop(mapping);
        block_on(namespace.remove_file_with_cross_process_authority(&authority)).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        assert!(block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).is_err());
        assert!(block_on(namespace.remove_file_with_cross_process_authority(&authority)).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        println!("EXCLUSIVE_CHILD_VERIFIED authorized-exclusive-lifetime");
    }

    // CMP-T04：显式EX来源复制只继承同对象资格；普通位置复制仍拒绝，
    // 受管/普通源均复用原长度、暂存、严格不替换及双失败证据，不复制目标状态。
    #[test]
    fn test_local_exclusive_composition_copy_owner_validates_source_and_preserves_failure_evidence() {
        if isolated("test_local_exclusive_composition_copy_owner_validates_source_and_preserves_failure_evidence") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let namespace = LocalFileNamespace::new();
        let contents = (0..READ_TRANSFER_CHUNK_BYTES + 1).map(|index| (index % 251) as u8).collect::<Vec<_>>();
        for managed in [false, true] {
            let source = temporary.path().join(format!("source-{managed}.bin"));
            let wrong = temporary.path().join(format!("wrong-{managed}.bin"));
            let destination = temporary.path().join(format!("target-{managed}.bin"));
            let rejected = temporary.path().join(format!("rejected-{managed}.bin"));
            std::fs::write(&source, &contents).unwrap();
            std::fs::write(&wrong, b"wrong").unwrap();
            let owner = if managed {
                // SAFETY: 独立真实夹具只由同版本协议访问，没有旁路内容操作。
                let authority = unsafe { block_on(namespace.establish_cross_process_authority(&source)) }.unwrap();
                Arc::new(block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap())
            } else {
                Arc::new(block_on(open_local_file(namespace.core, source.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap())
            };
            let plain = block_on(namespace.copy_new(&source, &destination)).expect_err("location alone cannot inherit EX");
            assert_eq!(plain.error().current_context(), &pi_result::ErrorKind::Conflict);
            assert_eq!(plain.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(plain.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            let mismatch = block_on(copy_local_file(namespace.core, wrong.clone(), rejected.clone(), Arc::new(AtomicBool::new(false)), Some(Arc::clone(&owner))))
                .expect_err("copy owner must match its actual source");
            assert!(matches!(mismatch.error().current_context(), pi_result::ErrorKind::Conflict | pi_result::ErrorKind::NotFound));
            assert_eq!(mismatch.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(mismatch.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            assert!(!rejected.exists());
            let outcome = block_on(copy_local_file(namespace.core, source.clone(), destination.clone(), Arc::new(AtomicBool::new(false)), Some(Arc::clone(&owner))))
                .expect("explicit owner source copy succeeds");
            assert_eq!(outcome.content_byte_len(), contents.len() as u64);
            assert_eq!(std::fs::read(&source).unwrap(), contents);
            assert_eq!(std::fs::read(&destination).unwrap(), contents);
            let duplicate = block_on(copy_local_file(namespace.core, source.clone(), destination.clone(), Arc::new(AtomicBool::new(false)), Some(Arc::clone(&owner))))
                .expect_err("owner grants no target replacement");
            assert_eq!(duplicate.error().current_context(), &pi_result::ErrorKind::AlreadyExists);
            assert_eq!(duplicate.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
            assert_eq!(duplicate.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            let mapper = block_on(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), source.clone(), FileAccessMode::ReadMmap)).unwrap();
            let mapping = if managed {
                block_on(mapper.map_read_only(crate::MmapRange::new(0, 1).unwrap())).unwrap()
            } else {
                // SAFETY: 夹具的稳定性由当前测试保证，仅合法独立通道访问。
                unsafe { block_on(mapper.map_read_only_uncoordinated(crate::MmapRange::new(0, 1).unwrap())) }.unwrap()
            };
            let conflict = block_on(copy_local_file(namespace.core, source, rejected.clone(), Arc::new(AtomicBool::new(false)), Some(Arc::clone(&owner))))
                .expect_err("explicit owner does not waive mapping/read exclusion");
            assert_eq!(conflict.error().current_context(), &pi_result::ErrorKind::Conflict);
            assert_eq!(conflict.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
            assert!(!rejected.exists());
            assert_eq!(mapping.as_bytes(), &[0]);
            drop(mapping);
            drop(mapper);
            drop(owner);
            assert_eq!(std::fs::read(&wrong).unwrap(), b"wrong");
            assert_eq!(std::fs::read(&destination).unwrap(), contents);
        }
        println!("EXCLUSIVE_CHILD_VERIFIED explicit-owner-copy");
    }

    // CMP-T01：默认根不偷换成按路径独占；失败无内容改变或新协调资产。
    #[test]
    fn test_local_exclusive_composition_default_root_is_unsupported_without_assets() {
        if isolated("test_local_exclusive_composition_default_root_is_unsupported_without_assets") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        std::fs::write(&path, b"default-owner").unwrap();
        let namespace = LocalFileNamespace::new();
        assert_eq!(block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
            .err().expect("no common root must reject EX").current_context(), &pi_result::ErrorKind::Unsupported);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        assert_eq!(std::fs::read(&path).unwrap(), b"default-owner");
        drop(block_on(namespace.open(&path, FileAccessMode::Read)).unwrap());
        println!("EXCLUSIVE_CHILD_VERIFIED default-exclusive-unsupported");
    }

    // CMP-T01：独占必须先取得真实RW权限；只有R权限的合法普通源仍能读取，
    // EX失败不遗留资格、不隐式放宽owner能力；权限夹具明确要求非root。
    #[cfg(target_os = "linux")]
    #[test]
    fn test_local_exclusive_composition_exclusive_requires_read_write_permission() {
        if isolated("test_local_exclusive_composition_exclusive_requires_read_write_permission") { return; }
        use std::os::unix::fs::PermissionsExt;
        // SAFETY: 只查询当前有效用户标识，不涉及内存或资源所有权。
        assert_ne!(unsafe { libc::geteuid() }, 0, "permission observation requires a non-root user");
        let temporary = fixture();
        let path = temporary.path().join("read-only.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"readable").unwrap();
        let original_permissions = std::fs::metadata(&path).unwrap().permissions();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path)).unwrap();
        let namespace = LocalFileNamespace::new();
        let result = block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive));
        std::fs::set_permissions(&path, original_permissions).unwrap();
        assert_eq!(result.err().expect("owner must actually request RW permission").current_context(), &pi_result::ErrorKind::PermissionDenied);
        let owner = block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap();
        assert_eq!(block_on(owner.byte_len()).unwrap(), 8);
        drop(owner);
        assert_eq!(std::fs::read(&path).unwrap(), b"readable");
        println!("EXCLUSIVE_CHILD_VERIFIED owner-rw-permission");
    }

    // CMP-T05：真实worker暂停冷派生；取消等待并释放最后外部owner后，生产
    // 完成者仍须握住EX及原协议生命周期。测试只驱动生产Future，不代装任务。
    #[test]
    fn test_local_exclusive_composition_cancelled_derivation_retains_owner_until_real_completion() {
        if isolated("test_local_exclusive_composition_cancelled_derivation_retains_owner_until_real_completion") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        for managed in [false, true] {
            let path = temporary.path().join(format!("cancel-derived-{managed}.bin"));
            std::fs::write(&path, b"cancel-owner").unwrap();
            let authority = if managed {
                // SAFETY: 独立真实夹具仅通过同版本本库协议访问。
                Some(unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap())
            } else { None };
            let owner = Arc::new(match authority.as_ref() {
                Some(authority) => block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap(),
                None => block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap(),
            });
            let weak = Arc::downgrade(&owner);
            let (started_sender, started_receiver) = std::sync::mpsc::channel();
            let (release_sender, release_receiver) = std::sync::mpsc::channel();
            let occupied = async_global_executor::spawn(blocking::unblock(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            }));
            started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            let mut derivation = Box::pin(open_local_file_from_exclusive_resource(namespace.core, Arc::clone(&owner), path.clone(), FileAccessMode::Append));
            assert!(block_on(futures_lite::future::poll_once(derivation.as_mut())).is_none());
            drop(owner);
            drop(derivation);
            let retained = weak.upgrade().is_some();
            let qualification_held = match OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared) {
                Ok(qualification) => { drop(qualification); false }
                Err(error) => { assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict); true }
            };
            let lifecycle_held = authority.as_ref().map(|authority| {
                match try_lock_sidecar_exclusive(&authority.local_core().binding.layout.lifecycle_gate) {
                    Ok(lock) => { drop(lock); false }
                    Err(error) => { assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict); true }
                }
            });
            // 先排空真实生产工作，再报告中间失败；任何红线都不留下占用worker。
            release_sender.send(()).unwrap();
            block_on(occupied);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while weak.upgrade().is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(weak.upgrade().is_none(), "actual derivation completion releases its temporary owner");
            // 原owner值销毁不等于未交付新资源的字段析构已经全部结束；最终
            // 仍用真实原生仲裁成功证明释放，不用Weak/调度间隙冒充内核完成。
            loop {
                match OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared) {
                    Ok(qualification) => { drop(qualification); break; }
                    Err(error) => {
                        assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                        assert!(std::time::Instant::now() < deadline, "actual derivation must release EX after completion");
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
            if let Some(authority) = authority.as_ref() {
                loop {
                    match try_lock_sidecar_exclusive(&authority.local_core().binding.layout.lifecycle_gate) {
                        Ok(lock) => { drop(lock); break; }
                        Err(error) => {
                            assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                            assert!(std::time::Instant::now() < deadline, "actual derivation must release original lifecycle protection");
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                }
            }
            assert_eq!(std::fs::read(&path).unwrap(), b"cancel-owner");
            println!("DERIVATION_CANCEL_OBSERVATION managed={managed} owner_retained={retained} qualification_held={qualification_held} lifecycle_held={lifecycle_held:?}");
            assert!(retained && qualification_held && lifecycle_held != Some(false), "cancelled cold derivation must retain its actual owner until completion");
        }
        println!("EXCLUSIVE_CHILD_VERIFIED cancelled-derived-owner");
    }

    // CMP-T04/T05：复制共同生产入口接管owner和取消标志，取消停等且最后
    // 外部owner释放后，EX/旧生命周期保护仍须持续到实际未发布清理结束。
    #[test]
    fn test_local_exclusive_composition_cancelled_copy_retains_owner_and_never_publishes() {
        if isolated("test_local_exclusive_composition_cancelled_copy_retains_owner_and_never_publishes") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        for managed in [false, true] {
            let path = temporary.path().join(format!("cancel-copy-{managed}.bin"));
            let destination = temporary.path().join(format!("cancel-target-{managed}.bin"));
            std::fs::write(&path, b"copy-owner").unwrap();
            let authority = if managed {
                // SAFETY: 所有参与者都是当前测试的同版本本库，没有外部内容修改。
                Some(unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap())
            } else { None };
            let owner = Arc::new(match authority.as_ref() {
                Some(authority) => block_on(open_local_file_with_cross_process_authority(namespace.core, Arc::clone(authority.local_core()), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap(),
                None => block_on(open_local_file(namespace.core, path.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap(),
            });
            let weak = Arc::downgrade(&owner);
            let (started_sender, started_receiver) = std::sync::mpsc::channel();
            let (release_sender, release_receiver) = std::sync::mpsc::channel();
            let occupied = async_global_executor::spawn(blocking::unblock(move || {
                started_sender.send(()).unwrap();
                release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            }));
            started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            let mut copying = Box::pin(copy_local_file_owned(namespace.core, path.clone(), destination.clone(), Some(Arc::clone(&owner))));
            assert!(block_on(futures_lite::future::poll_once(copying.as_mut())).is_none());
            drop(owner);
            drop(copying);
            let retained = weak.upgrade().is_some();
            let qualification_held = match OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared) {
                Ok(qualification) => { drop(qualification); false }
                Err(error) => { assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict); true }
            };
            let lifecycle_held = authority.as_ref().map(|authority| {
                match try_lock_sidecar_exclusive(&authority.local_core().binding.layout.lifecycle_gate) {
                    Ok(lock) => { drop(lock); false }
                    Err(error) => { assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict); true }
                }
            });
            release_sender.send(()).unwrap();
            block_on(occupied);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                match OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Shared) {
                    Ok(qualification) => { drop(qualification); break; }
                    Err(error) => {
                        assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                        assert!(std::time::Instant::now() < deadline, "real cancelled copy must finish and release EX");
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
            if let Some(authority) = authority.as_ref() {
                loop {
                    match try_lock_sidecar_exclusive(&authority.local_core().binding.layout.lifecycle_gate) {
                        Ok(lock) => { drop(lock); break; }
                        Err(error) => {
                            assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                            assert!(std::time::Instant::now() < deadline, "real cancelled copy must release original lifecycle protection");
                            std::thread::sleep(std::time::Duration::from_millis(1));
                        }
                    }
                }
            }
            assert!(weak.upgrade().is_none());
            assert!(!destination.exists());
            assert!(std::fs::read_dir(temporary.path()).unwrap().all(|entry| !entry.unwrap().file_name().to_string_lossy().ends_with(".copy")));
            assert_eq!(std::fs::read(&path).unwrap(), b"copy-owner");
            println!("COPY_CANCEL_OBSERVATION managed={managed} owner_retained={retained} qualification_held={qualification_held} lifecycle_held={lifecycle_held:?}");
            assert!(retained && qualification_held && lifecycle_held != Some(false));
        }
        println!("EXCLUSIVE_CHILD_VERIFIED cancelled-owner-copy");
    }

    // CMP-T01：新增EX冷编排对不存在/目录的拒绝保持原分类；不创建目标，
    // 失败不建立资格载体，之后同配置的真实普通文件仍可正常取得EX。
    #[test]
    fn test_local_exclusive_composition_missing_and_directory_fail_without_admission_assets() {
        if isolated("test_local_exclusive_composition_missing_and_directory_fail_without_admission_assets") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        let missing = temporary.path().join("missing.bin");
        let directory = temporary.path().join("directory");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::create_dir(&directory).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        for (path, expected) in [(missing.clone(), pi_result::ErrorKind::NotFound), (directory, pi_result::ErrorKind::Unsupported)] {
            assert_eq!(block_on(open_local_file(namespace.core, path, FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive))
                .err().expect("unsupported or missing object cannot grant EX").current_context(), &expected);
        }
        assert!(!missing.exists());
        assert_eq!(std::fs::read_dir(&root_path).unwrap().count(), 0);
        std::fs::write(&missing, b"created-later").unwrap();
        drop(block_on(open_local_file(namespace.core, missing.clone(), FileAccessMode::ReadWriteMmap, OpenAdmissionKind::Exclusive)).unwrap());
        assert_eq!(std::fs::read(&missing).unwrap(), b"created-later");
        println!("EXCLUSIVE_CHILD_VERIFIED exclusive-invalid-targets");
    }

    // PSA-T01：通过原公开open取得SH，独立原生EX必须冲突；所有公开角色
    // 都不得越过已持有EX。真实硬链接与精确文件内容同时核验，不代理公开准入。
    #[test]
    fn test_local_public_acquisition_ordinary_open_participates_in_shared_qualification() {
        if isolated("test_local_public_acquisition_ordinary_open_participates_in_shared_qualification") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let alias = temporary.path().join("alias.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"public-original").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let first = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
        let second = block_on(namespace.clone().open(&alias, FileAccessMode::Append)).unwrap();
        let probe = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive);
        let was_excluded = probe.as_ref().err().is_some_and(|error| error.current_context() == &pi_result::ErrorKind::Conflict);
        drop(probe);
        assert!(was_excluded, "public ordinary resources must exclude native exclusive admission");
        drop(first);
        assert_eq!(OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive)
            .unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        drop(second);
        let exclusive = OwnedOpenAdmission::try_acquire(root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive).unwrap();
        for locator in [&path, &alias] {
            for access in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
                FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap] {
                let error = block_on(namespace.open(locator, access)).err().expect("every ordinary role must respect existing EX");
                assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
            }
        }
        drop(exclusive);
        let reader = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
        let mut contents = [0_u8; 15];
        block_on(reader.read_exact_at(0, &mut contents, ReadTargetRegion::full())).unwrap();
        assert_eq!(&contents, b"public-original");
        drop(reader);
        assert_eq!(std::fs::read(&path).unwrap(), b"public-original");
        println!("EXCLUSIVE_CHILD_VERIFIED public-shared-open");
    }

    // PSA-T02：六角色普通/协调创建及凭据重开都必须由公开入口持SH；
    // 只剩凭据时独立EX可取得，失败/释放不可由测试侧手工补资格。
    #[test]
    fn test_local_public_acquisition_create_and_authority_open_hold_only_active_shared_qualification() {
        if isolated("test_local_public_acquisition_create_and_authority_open_hold_only_active_shared_qualification") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let probe = |path: &Path| OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(path).unwrap(), OpenAdmissionKind::Exclusive,
        );
        for (index, access) in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
            FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap].into_iter().enumerate() {
            let path = temporary.path().join(format!("ordinary-{index}.bin"));
            let resource = block_on(namespace.create_new(&path, access)).unwrap();
            assert_eq!(probe(&path).expect_err("public created resource must hold SH").current_context(), &pi_result::ErrorKind::Conflict);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
            drop(resource);
            drop(probe(&path).expect("last ordinary creation resource releases SH"));
        }
        for (index, access) in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite,
            FileAccessMode::Truncate, FileAccessMode::ReadMmap, FileAccessMode::ReadWriteMmap].into_iter().enumerate() {
            let path = temporary.path().join(format!("coordinated-{index}.bin"));
            // SAFETY: 真实夹具只由此测试经同一协议访问；探针只持身份/资格，不修改内容。
            let created = unsafe { block_on(namespace.create_new_coordinated(path.clone(), access)) }.unwrap();
            let (authority, resource) = created.into_parts();
            assert_eq!(probe(&path).expect_err("coordinated creation resource must hold SH").current_context(), &pi_result::ErrorKind::Conflict);
            drop(resource);
            let exclusive = probe(&path).expect("idle credential must not retain SH");
            assert_eq!(block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read))
                .err().expect("existing EX also rejects credential-based ordinary open").current_context(), &pi_result::ErrorKind::Conflict);
            drop(exclusive);
            let reader = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read)).unwrap();
            assert_eq!(probe(&path).expect_err("credential-based resource must hold SH").current_context(), &pi_result::ErrorKind::Conflict);
            assert_eq!(block_on(reader.byte_len()).unwrap(), 0);
            drop(reader);
            drop(probe(&path).expect("idle credential after reopen releases SH"));
            drop(authority);
        }
        println!("EXCLUSIVE_CHILD_VERIFIED public-create-authority");
    }

    // PSA-T03：原公开位置复制不得读取已持EX的源；失败不创建暂存/目标。
    // 释放后跨三个传输窗口成功，最后源/目标资格都可取得，精确核对全部字节。
    #[test]
    fn test_local_public_acquisition_copy_source_respects_exclusive_qualification() {
        if isolated("test_local_public_acquisition_copy_source_respects_exclusive_qualification") { return; }
        let temporary = fixture();
        let source = temporary.path().join("source.bin");
        let destination = temporary.path().join("destination.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        let contents = vec![0x4b; READ_TRANSFER_CHUNK_BYTES * 2 + 1];
        std::fs::write(&source, &contents).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let exclusive = OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&source).unwrap(), OpenAdmissionKind::Exclusive).unwrap();
        let failure = block_on(namespace.copy_new(&source, &destination)).expect_err("copy must not bypass source exclusive admission");
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
        assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 2);
        assert_eq!(std::fs::read(&source).unwrap(), contents);
        drop(exclusive);
        assert_eq!(block_on(namespace.copy_new(&source, &destination)).unwrap().content_byte_len(), contents.len() as u64);
        assert_eq!(std::fs::read(&destination).unwrap(), contents);
        for path in [&source, &destination] {
            drop(OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(path).unwrap(), OpenAdmissionKind::Exclusive)
                .expect("completed copy releases all content resource qualifications"));
        }
        println!("EXCLUSIVE_CHILD_VERIFIED public-copy-source");
    }

    // PSA-T03：真实暂存生产出口持SH至原清理完成；对空暂存的独立EX
    // 探针和身份安全删除证据共同验证，测试不手工建立/撤销暂存资格。
    #[test]
    fn test_local_public_acquisition_copy_staging_holds_shared_until_cleanup() {
        if isolated("test_local_public_acquisition_copy_staging_holds_shared_until_cleanup") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        let destination = temporary.path().join("destination.bin");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let staging = block_on(create_local_copy_staging(namespace.core, &destination, &AtomicBool::new(false)))
            .unwrap_or_else(|(error, evidence)| panic!("staging failed: {error}; {evidence:?}"));
        let staging_path = staging.path.clone();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        assert_eq!(OwnedOpenAdmission::try_acquire(root, std::fs::File::open(&staging_path).unwrap(), OpenAdmissionKind::Exclusive)
            .unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::metadata(&staging_path).unwrap().len(), 0);
        assert!(!destination.exists());
        assert_eq!(block_on(staging.cleanup()), CopyStagingEvidence::CreatedThenRemovedByOperation);
        assert!(!staging_path.exists());
        assert!(!destination.exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        println!("EXCLUSIVE_CHILD_VERIFIED copy-staging-qualification");
    }

    // PSA-T04：从未poll的所有普通入口不固定根；非内容查询也不固定。
    // 失败的首次open仍固定Default，拒绝后来切换域，且不留下文件/协调资产。
    #[test]
    fn test_local_public_acquisition_unpolled_calls_are_lazy_and_failed_open_freezes_default() {
        if isolated("test_local_public_acquisition_unpolled_calls_are_lazy_and_failed_open_freezes_default") { return; }
        let temporary = fixture();
        let missing = temporary.path().join("missing.bin");
        let other = temporary.path().join("other.bin");
        let namespace = LocalFileNamespace::new();
        drop(namespace.open(&missing, FileAccessMode::Read));
        drop(namespace.create_new(&missing, FileAccessMode::Read));
        drop(namespace.copy_new(&missing, &other));
        // SAFETY: Future不poll；夹具独占，同一协议前提在全部生命周期成立。
        drop(unsafe { namespace.create_new_coordinated(missing.clone(), FileAccessMode::Read) });
        assert!(!block_on(namespace.try_exists(&missing)).unwrap());
        assert!(block_on(namespace.metadata(&temporary.path().to_path_buf())).is_ok());
        assert!(crate::coordination_root_config::configured_cross_process_coordination_root().is_none());
        assert_eq!(block_on(namespace.open(&missing, FileAccessMode::Read)).err().unwrap().current_context(), &pi_result::ErrorKind::NotFound);
        assert_eq!(crate::coordination_root_config::configured_cross_process_coordination_root(), Some(&crate::CrossProcessCoordinationRoot::FileParent));
        assert_eq!(crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(temporary.path().join("late")))
            .unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
        println!("EXCLUSIVE_CHILD_VERIFIED lazy-and-first-failure");
    }

    // PSA-T04：首次create失败也固定Default，目标未创建证据与实际磁盘一致。
    #[test]
    fn test_local_public_acquisition_failed_create_freezes_default_before_parent_resolution() {
        if isolated("test_local_public_acquisition_failed_create_freezes_default_before_parent_resolution") { return; }
        let temporary = fixture();
        let missing = temporary.path().join("missing-parent").join("data.bin");
        let namespace = LocalFileNamespace::new();
        let failure = block_on(namespace.create_new(&missing, FileAccessMode::Read)).err().unwrap();
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::NotFound);
        assert_eq!(failure.target_evidence(), &crate::CreateTargetEvidence::NotCreatedByOperation);
        assert_eq!(crate::coordination_root_config::configured_cross_process_coordination_root(), Some(&crate::CrossProcessCoordinationRoot::FileParent));
        assert!(!missing.exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
        println!("EXCLUSIVE_CHILD_VERIFIED create-first-failure");
    }

    // PSA-T04：首次copy的目标前置失败固定Default，但两项副作用证据仍为未产生。
    #[test]
    fn test_local_public_acquisition_failed_copy_freezes_default_without_assets() {
        if isolated("test_local_public_acquisition_failed_copy_freezes_default_without_assets") { return; }
        let temporary = fixture();
        let source = temporary.path().join("source.bin");
        let destination = temporary.path().join("missing-parent").join("destination.bin");
        let failure = block_on(LocalFileNamespace::new().copy_new(&source, &destination)).unwrap_err();
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::NotFound);
        assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
        assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::NotCreatedByOperation);
        assert_eq!(crate::coordination_root_config::configured_cross_process_coordination_root(), Some(&crate::CrossProcessCoordinationRoot::FileParent));
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
        println!("EXCLUSIVE_CHILD_VERIFIED copy-first-failure");
    }

    // PSA-T02/T04：Custom根缺失在真正创建后被候选核验拒绝；真实新空文件
    // 必须保留且明确CreatedByOperation。修复外部根后可以正常打开，不自动重建根。
    #[test]
    fn test_local_public_acquisition_post_create_root_failure_preserves_created_evidence() {
        if isolated("test_local_public_acquisition_post_create_root_failure_preserves_created_evidence") { return; }
        let temporary = fixture();
        let path = temporary.path().join("created.bin");
        let root_path = temporary.path().join("missing-root");
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let failure = block_on(namespace.create_new(&path, FileAccessMode::Read)).err().expect("missing Custom root must not downgrade");
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::NotFound);
        assert_eq!(failure.target_evidence(), &crate::CreateTargetEvidence::CreatedByOperation);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert!(!root_path.exists());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 1);
        std::fs::create_dir(&root_path).unwrap();
        let resource = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
        assert_eq!(block_on(resource.byte_len()).unwrap(), 0);
        drop(resource);
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        drop(OwnedOpenAdmission::try_acquire(root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive).unwrap());
        println!("EXCLUSIVE_CHILD_VERIFIED post-create-evidence");
    }

    // PSA-T04/PSA-DIAG-01：逐个真实worker门推进普通open，停在候选已经
    // 移交、资格准备尚未结束的窗口。取消等待不能提前撤同名预约；原注册表
    // 的真实try_reserve只作独立冲突观测，不替代任何打开/资格/清理逻辑。
    #[test]
    fn test_local_public_acquisition_cancelled_open_retains_name_until_real_completion() {
        if isolated("test_local_public_acquisition_cancelled_open_retains_name_until_real_completion") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let renamed = temporary.path().join("renamed.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"cancel-open-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let (probe_file, coordination) = block_on(opened(&path));
        drop(probe_file);
        assert_eq!(Arc::strong_count(&coordination), 1);
        let key = namespace_entry_key_sync(&path).unwrap().unwrap();
        let mut future = Box::pin(namespace.open(&path, FileAccessMode::Read));
        let mut held_gate = None;
        for _ in 0..32 {
            let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            let mut gate = Box::pin(blocking::unblock(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            }));
            assert!(block_on(futures_lite::future::poll_once(&mut gate)).is_none());
            entered_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
            let pending = block_on(futures_lite::future::poll_once(&mut future)).is_none();
            assert!(pending, "worker-gated open must not finish before its actual preparation");
            std::thread::yield_now();
            if Arc::strong_count(&coordination) > 1 {
                held_gate = Some((release_tx, gate));
                break;
            }
            release_tx.send(()).unwrap();
            block_on(gate);
        }
        let (release, gate) = held_gate.expect("real open must reach its candidate handoff within bounded steps");
        let before_cancel = namespace.core.namespace_entries.try_reserve(
            namespace_entry_key_sync(&path).unwrap().unwrap(), namespace.core.allocate_resource_id().unwrap(),
        );
        let name_retained_before_cancel = before_cancel.as_ref().err().is_some_and(|error| error.current_context() == &pi_result::ErrorKind::Conflict);
        drop(before_cancel);
        drop(future);
        let reservation = namespace.core.namespace_entries.try_reserve(key, namespace.core.allocate_resource_id().unwrap());
        let name_retained = reservation.as_ref().err().is_some_and(|error| error.current_context() == &pi_result::ErrorKind::Conflict);
        drop(reservation);
        let candidate_retained = Arc::strong_count(&coordination) > 1;
        release.send(()).unwrap();
        block_on(gate);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while Arc::strong_count(&coordination) != 1 {
            assert!(std::time::Instant::now() < deadline, "cancelled open must reach actual resource disposal");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(coordination);
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        loop {
            match OwnedOpenAdmission::try_acquire(Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive) {
                Ok(exclusive) => { drop(exclusive); break; }
                Err(error) if error.current_context() == &pi_result::ErrorKind::Conflict => {
                    assert!(std::time::Instant::now() < deadline, "cancelled open must release native admission");
                    std::thread::yield_now();
                }
                Err(error) => panic!("unexpected release probe: {error}"),
            }
        }
        block_on(namespace.rename(&path, &renamed)).unwrap();
        assert_eq!(std::fs::read(&renamed).unwrap(), b"cancel-open-original");
        println!("CANCELLED_OPEN_OBSERVATIONS before_cancel={name_retained_before_cancel} name_retained={name_retained} candidate_retained={candidate_retained}");
        assert!(name_retained_before_cancel, "the same real key must conflict before cancellation");
        assert!(candidate_retained, "the real candidate is still owned while its worker is gated");
        assert!(name_retained, "cancellation must retain the actual name reservation until preparation stops");
        println!("EXCLUSIVE_CHILD_VERIFIED cancelled-open-reservation");
    }

    // PSA-LIF-01：Linux真实系统调用专项，不模拟IO。公开copy冻结两字节，
    // 暂存真实创建后旁路缩短独占夹具源至一字节，驱动原意外EOF失败链。
    // strace只延迟实际暂存write，不替换其返回；两个worker让清理可与写者
    // 真实竞争。原FD身份核验避免编号复用，最后等待真实关闭再断言/清理。
    // 此专项需要主机已有strace和对子进程的ptrace权限，不以提前return跳过。
    #[cfg(target_os = "linux")]
    #[test]
    fn test_local_public_acquisition_copy_failure_closes_real_writer_before_shared_release() {
        use std::io::{BufRead as _, Read as _, Write as _};
        use std::os::unix::fs::MetadataExt;
        const CHILD: &str = "PI_ASYNC_FS_COPY_WRITER_TRACE_CHILD";
        const NAME: &str = "test_local_public_acquisition_copy_failure_closes_real_writer_before_shared_release";
        if std::env::var(CHILD).as_deref() != Ok(NAME) {
            // 父方持有夹具；子方被强制终止时，也在两个进程回收后正常清理。
            let temporary = fixture();
            // 观测器只拥有测试子进程；任意断言失败也不能留下tracer或被暂停worker。
            struct Processes { child: std::process::Child, tracer: Option<std::process::Child> }
            impl Drop for Processes {
                fn drop(&mut self) {
                    let _ = self.child.kill();
                    if let Some(tracer) = self.tracer.as_mut() {
                        let _ = tracer.kill();
                    }
                    let _ = self.child.wait();
                    if let Some(tracer) = self.tracer.as_mut() {
                        let _ = tracer.wait();
                    }
                }
            }
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg(format!("local::exclusive_open_tests::{NAME}"))
                .arg("--exact").arg("--nocapture").arg("--test-threads=1")
                .env(CHILD, NAME).env("BLOCKING_MAX_THREADS", "2")
                .env("PI_ASYNC_FS_COPY_WRITER_TRACE_DIRECTORY", temporary.path())
                .stdin(std::process::Stdio::piped()).stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped()).spawn().unwrap();
            let mut processes = Processes { child, tracer: None };
            let stdout = processes.child.stdout.take().unwrap();
            let stderr = processes.child.stderr.take().unwrap();
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let reader = std::thread::spawn(move || {
                let mut output = String::new();
                for line in std::io::BufReader::new(stdout).lines() {
                    let line = line.unwrap();
                    if let Some(path) = line.strip_prefix("PSA_TRACE_READY ") {
                        ready_tx.send(PathBuf::from(path)).unwrap();
                    }
                    output.push_str(&line);
                    output.push('\n');
                }
                output
            });
            let error_reader = std::thread::spawn(move || {
                let mut output = String::new();
                std::io::BufReader::new(stderr).read_to_string(&mut output).unwrap();
                output
            });
            let path = match ready_rx.recv_timeout(std::time::Duration::from_secs(10)) {
                Ok(path) => path,
                Err(error) => {
                    drop(processes);
                    panic!("trace child failed before readiness: {error}\n{}\n{}", reader.join().unwrap(), error_reader.join().unwrap());
                }
            };
            let tracer = std::process::Command::new("strace")
                .arg("-f").arg("-yy").arg("-e").arg("trace=write")
                .arg("-e").arg("inject=write:delay_enter=5s")
                .arg("-P").arg(&path).arg("-p").arg(processes.child.id().to_string())
                .stdout(std::process::Stdio::null()).stderr(std::process::Stdio::piped())
                .spawn().expect("the Linux syscall lifetime test requires strace");
            processes.tracer = Some(tracer);
            let trace = processes.tracer.as_mut().unwrap().stderr.take().unwrap();
            let (attached_tx, attached_rx) = std::sync::mpsc::channel();
            let trace_reader = std::thread::spawn(move || {
                let mut output = String::new();
                let mut attached = false;
                for line in std::io::BufReader::new(trace).lines() {
                    let line = line.unwrap();
                    if !attached && line.contains("attached") {
                        attached_tx.send(()).unwrap();
                        attached = true;
                    }
                    output.push_str(&line);
                    output.push('\n');
                }
                output
            });
            if let Err(error) = attached_rx.recv_timeout(std::time::Duration::from_secs(5)) {
                drop(processes);
                panic!("strace must attach before real IO is released: {error}\n{}\n{}\n{}", reader.join().unwrap(), error_reader.join().unwrap(), trace_reader.join().unwrap());
            }
            processes.child.stdin.take().unwrap().write_all(b"trace-ready\n").unwrap();
            let released_at = std::time::Instant::now();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(25);
            let status = loop {
                if let Some(status) = processes.child.try_wait().unwrap() { break status; }
                assert!(std::time::Instant::now() < deadline, "traced copy must finish within its functional budget");
                std::thread::sleep(std::time::Duration::from_millis(10));
            };
            while processes.tracer.as_mut().unwrap().try_wait().unwrap().is_none() {
                assert!(std::time::Instant::now() < deadline, "the syscall tracer must also finish within the functional budget");
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let output = reader.join().unwrap();
            let errors = error_reader.join().unwrap();
            let trace = trace_reader.join().unwrap();
            print!("{output}\n{errors}\n{trace}");
            // strace5.5没有新版DELAYED标记。真实路径write记录、子方暂停状态
            // 及显式五秒故障窗口共同证明生效；这不是文件性能阈值或SLA。
            assert!(trace.contains("write(") && trace.contains(&path.display().to_string()), "the real staging syscall must be traced: {trace}");
            assert!(released_at.elapsed() >= std::time::Duration::from_secs(4), "the configured syscall delay must actually take effect");
            assert!(status.success(), "traced copy child failed\n{output}\n{errors}\n{trace}");
            assert!(output.contains("1 passed; 0 failed; 0 ignored"));
            return;
        }

        // SAFETY: 只允许本用例父进程及其tracer子进程调试此隔离进程，不改变
        // 生产文件逻辑或其它进程的权限；进程退出后该调试资格自动消失。
        assert_eq!(unsafe { libc::prctl(libc::PR_SET_PTRACER, libc::getppid(), 0, 0, 0) }, 0);
        let temporary = PathBuf::from(std::env::var_os("PI_ASYNC_FS_COPY_WRITER_TRACE_DIRECTORY").unwrap());
        let source = temporary.join("source.bin");
        let destination = temporary.join("destination.bin");
        let root_path = temporary.join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&source, b"AB").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        type Gate = (std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>, Pin<Box<dyn Future<Output = ()> + Send>>);
        fn queue_gate() -> Gate {
            let (entered_tx, entered_rx) = std::sync::mpsc::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let future = blocking::unblock(move || {
                entered_tx.send(()).unwrap();
                release_rx.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            });
            (release_tx, entered_rx, Box::pin(future))
        }
        fn enter(gates: &[Gate; 2]) {
            for (_, entered, _) in gates {
                entered.recv_timeout(std::time::Duration::from_secs(10)).unwrap();
            }
        }
        fn release(gates: [Gate; 2]) {
            for (sender, _, future) in gates {
                sender.send(()).unwrap();
                block_on(future);
            }
        }
        let mut gates = [queue_gate(), queue_gate()];
        enter(&gates);
        let mut copy = Box::pin(namespace.copy_new(&source, &destination));
        assert!(block_on(futures_lite::future::poll_once(&mut copy)).is_none());
        let staging_deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
        let staging_path = loop {
            if let Some(path) = std::fs::read_dir(&temporary).unwrap()
                .map(|entry| entry.unwrap().path())
                .find(|path| path.extension().is_some_and(|extension| extension == "copy"))
            {
                break path;
            }
            assert!(std::time::Instant::now() < staging_deadline, "real copy must create its staging before transfer");
            if let Some(result) = block_on(futures_lite::future::poll_once(&mut copy)) {
                panic!("copy finished before the staging observation: {result:?}");
            }
            let next = [queue_gate(), queue_gate()];
            release(gates);
            // 门只控制真实worker；给独立拥有型完成者调度机会，不把固定32次
            // 观测轮数误当为生产阶段上限。业务仍只有一次两字节复制。
            std::thread::sleep(std::time::Duration::from_millis(1));
            enter(&next);
            gates = next;
        };
        assert_eq!(std::fs::metadata(&staging_path).unwrap().len(), 0);
        let staging_pin = std::fs::File::open(&staging_path).unwrap();
        let expected = staging_pin.metadata().unwrap();
        std::fs::OpenOptions::new().write(true).open(&source).unwrap().set_len(1).unwrap();
        // libtest的用例名前缀可能尚未换行；协议标记必须独占一行。
        println!("\nPSA_TRACE_READY {}", staging_path.display());
        std::io::stdout().flush().unwrap();
        let mut ready = String::new();
        std::io::stdin().read_line(&mut ready).unwrap();
        assert_eq!(ready, "trace-ready\n");
        release(gates);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        let data_fd = loop {
            let found = std::fs::read_dir("/proc/self/task").unwrap().find_map(|entry| {
                let entry = entry.unwrap();
                let call = std::fs::read_to_string(entry.path().join("syscall")).ok()?;
                let mut fields = call.split_whitespace();
                if fields.next()? != libc::SYS_write.to_string() { return None; }
                let fd = i32::from_str_radix(fields.next()?.trim_start_matches("0x"), 16).ok()?;
                let metadata = std::fs::metadata(format!("/proc/self/fd/{fd}")).ok()?;
                let status = std::fs::read_to_string(entry.path().join("status")).ok()?;
                let paused = status.lines().any(|line| line.starts_with("State:\tt"));
                (paused && metadata.dev() == expected.dev() && metadata.ino() == expected.ino()).then_some(fd)
            });
            if let Some(fd) = found { break fd; }
            assert!(std::time::Instant::now() < deadline, "the real staging write must reach the traced syscall");
            std::thread::sleep(std::time::Duration::from_millis(1));
        };
        let failure = block_on(copy).unwrap_err();
        assert_eq!(failure.error().current_context(), &pi_result::ErrorKind::Corrupted);
        assert_eq!(failure.target_evidence(), &CopyTargetEvidence::NotPublishedByOperation);
        assert_eq!(failure.staging_evidence(), &CopyStagingEvidence::CreatedThenRemovedByOperation);
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let exclusive = OwnedOpenAdmission::try_acquire(root, staging_pin, OpenAdmissionKind::Exclusive)
            .expect("completed copy must release the staging shared qualification");
        fn data_file_is_open(fd: i32, expected: &std::fs::Metadata) -> bool {
            match std::fs::metadata(format!("/proc/self/fd/{fd}")) {
                Ok(metadata) => metadata.dev() == expected.dev() && metadata.ino() == expected.ino(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
                Err(error) => panic!("native data FD observation failed: {error}"),
            }
        }
        let file_closed_at_return = !data_file_is_open(data_fd, &expected);
        // 不把编号失效即外推成物理页回收；这里只证明原数据文件打开已结束。
        while data_file_is_open(data_fd, &expected) {
            assert!(std::time::Instant::now() < deadline, "real paused writer must eventually close");
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(exclusive);
        assert!(!staging_path.exists());
        assert!(!destination.exists());
        assert_eq!(std::fs::read(&source).unwrap(), b"A");
        println!("COPY_WRITER_CLOSE_OBSERVATIONS qualification_released=true file_closed={file_closed_at_return}");
        assert!(file_closed_at_return, "shared qualification must not be released before the real staging writer closes");
        println!("EXCLUSIVE_CHILD_VERIFIED copy-writer-close");
    }

    // LIF-T01/T02：真实旧授权独活不能握住新SH资格；先记录最后候选释放
    // 的独立锁/弱引用观测，再验证同凭据仍可重开。收尾完毕后断言，失败不遗留资源。
    #[test]
    fn test_local_authority_lifetime_idle_credential_releases_activity_and_can_reopen() {
        if isolated("test_local_authority_lifetime_idle_credential_releases_activity_and_can_reopen") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"credential-original").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        // SAFETY: 全部路径为本用例独占的真实夹具，没有旁路修改/进程，旧协议前提持续满足。
        let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
        let (file, coordination) = block_on(opened(&path));
        let weak = Arc::downgrade(&coordination);
        let admitted = block_on(acquire_opened_local_file(path.clone(), file, coordination, OpenAdmissionKind::Shared)).unwrap();
        drop(admitted);
        let activity_released = weak.upgrade().is_none();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let native_probe = OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        );
        let qualification_released = match native_probe {
            Ok(admission) => { drop(admission); true }
            Err(error) => {
                assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                false
            }
        };
        let reader = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::Read))
            .expect("idle valid credential must remain usable after activity is reclaimed");
        assert_eq!(block_on(reader.byte_len()).unwrap(), 19);
        drop(reader);
        drop(authority);
        assert!(weak.upgrade().is_none(), "final credential cleanup must not retain old activity");
        drop(OwnedOpenAdmission::try_acquire(
            root, std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).unwrap());
        assert_eq!(std::fs::read(&path).unwrap(), b"credential-original");
        println!("AUTHORITY_IDLE_OBSERVATIONS activity_released={activity_released} qualification_released={qualification_released}");
        assert!(activity_released, "an idle credential is not an activity owner");
        assert!(qualification_released, "an idle credential must not retain open qualification");
        println!("EXCLUSIVE_CHILD_VERIFIED idle-credential");
    }

    // LIF-T02/T04：私有候选的真实资格与既有公共文件/RO克隆共享原核心。
    // 最后映射释放后凭据仍可重开/汇合；两个旧凭据都不能访问或删除新代次。
    // 此处只验证获准内外接缝的现有保活链；公开取得的组合证据由SI-069/071分别提供。
    #[test]
    fn test_local_authority_lifetime_mapping_clones_and_independent_credentials_compose() {
        if isolated("test_local_authority_lifetime_mapping_clones_and_independent_credentials_compose") { return; }
        let temporary = fixture();
        let path = temporary.path().join("data.bin");
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        std::fs::write(&path, b"0123456789abcdef").unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        // SAFETY: 全部调用及映射来自同一协议，真实夹具独占且无旁路修改。
        let first = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
        // SAFETY: 与first相同真实实例和稳定名称，重复建立不改变合作域。
        let second = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
        let resource = block_on(namespace.open_with_cross_process_authority(&first, FileAccessMode::ReadMmap)).unwrap();
        let file = block_on(async_fs::File::open(&path)).unwrap();
        let candidate = block_on(acquire_opened_local_file(path.clone(), file, Arc::clone(&resource.coordination), OpenAdmissionKind::Shared)).unwrap();
        let weak = Arc::downgrade(&candidate.coordination);
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let mapping = block_on(resource.map_read_only(MmapRange::new(0, 8).unwrap())).unwrap();
        let cloned = mapping.clone();
        drop(candidate);
        drop(resource);
        for current in [Some(mapping), None] {
            assert_eq!(cloned.as_bytes(), b"01234567");
            drop(current);
            assert_eq!(OwnedOpenAdmission::try_acquire(
                Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
            ).unwrap_err().current_context(), &pi_result::ErrorKind::Conflict);
        }
        drop(cloned);
        assert!(weak.upgrade().is_none());
        drop(OwnedOpenAdmission::try_acquire(
            Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
        ).expect("idle credentials must not retain the last mapping qualification"));
        let mut appender = block_on(namespace.open_with_cross_process_authority(&first, FileAccessMode::Append)).unwrap();
        let reader = block_on(namespace.open_with_cross_process_authority(&second, FileAccessMode::Read)).unwrap();
        block_on(appender.append(Vec::from(&b"-tail"[..]))).unwrap();
        block_on(appender.flush(FileFlushMode::DataAndMetadata)).unwrap();
        assert_eq!(block_on(reader.byte_len()).unwrap(), 21);
        assert_eq!(std::fs::read(&path).unwrap(), b"0123456789abcdef-tail");
        drop((appender, reader));
        block_on(namespace.remove_file_with_cross_process_authority(&second)).unwrap();
        block_on(namespace.remove_file_with_cross_process_authority(&first)).expect("other valid credential shares removed-instance idempotence");
        assert!(!path.exists());
        // SAFETY: 旧实例已通过协议退役；只有协调式创建发布新实例。
        let replacement = unsafe { block_on(namespace.create_new_coordinated(path.clone(), FileAccessMode::Read)) }.unwrap();
        let (replacement_authority, replacement_file) = replacement.into_parts();
        for stale in [&first, &second] {
            assert_eq!(block_on(namespace.open_with_cross_process_authority(stale, FileAccessMode::Read))
                .err().expect("stale credential cannot open the replacement").current_context(), &pi_result::ErrorKind::Conflict);
            assert_eq!(block_on(namespace.remove_file_with_cross_process_authority(stale))
                .unwrap_err().error().current_context(), &pi_result::ErrorKind::Conflict);
        }
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        drop(replacement_file);
        block_on(namespace.remove_file_with_cross_process_authority(&replacement_authority)).unwrap();
        assert!(!path.exists());
        println!("EXCLUSIVE_CHILD_VERIFIED mapping-and-credentials");
    }

    // LIF-T04：真实取消后的追加/覆盖/截断/刷新任务必须先释放无人持有的文件槽，
    // 再结束活动所有者。只用真实worker门及原生计数锁扩大观测窗口，不代补IO。
    #[test]
    fn test_local_authority_lifetime_cancelled_completion_closes_file_before_activity_release() {
        if isolated("test_local_authority_lifetime_cancelled_completion_closes_file_before_activity_release") { return; }
        let temporary = fixture();
        let root_path = temporary.path().join("coordination");
        std::fs::create_dir(&root_path).unwrap();
        crate::set_cross_process_coordination_root(crate::CrossProcessCoordinationRoot::Custom(root_path.clone())).unwrap();
        let namespace = LocalFileNamespace::new();
        let root = Arc::new(CoordinationRootAnchor::open(root_path).unwrap());
        let mut observations = Vec::new();
        for operation in ["append", "overwrite", "truncate", "flush"] {
            let path = temporary.path().join(format!("{operation}.bin"));
            std::fs::write(&path, b"original").unwrap();
            // SAFETY: 用例独占真实夹具，全部参与者使用同一协议，无旁路修改。
            let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
            let role = match operation {
                "overwrite" => FileAccessMode::Overwrite,
                "truncate" => FileAccessMode::Truncate,
                _ => FileAccessMode::Append,
            };
            let mut file = block_on(namespace.open_with_cross_process_authority(&authority, role)).unwrap();
            let coordination = Arc::clone(&file.coordination);
            let weak_coordination = Arc::downgrade(&coordination);
            let candidate_file = block_on(async_fs::File::open(&path)).unwrap();
            drop(block_on(acquire_opened_local_file(path.clone(), candidate_file, Arc::clone(&coordination), OpenAdmissionKind::Shared)).unwrap());
            let native_core = file.cross_process_authority().unwrap();
            let weak_file_slot = Arc::downgrade(&file.file);
            let (first_started_sender, first_started_receiver) = std::sync::mpsc::channel();
            let (first_release_sender, first_release_receiver) = std::sync::mpsc::channel();
            let first_worker = blocking::unblock(move || {
                first_started_sender.send(()).unwrap();
                first_release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            });
            first_started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            let mut pending: Pin<Box<dyn Future<Output = ()> + Send + '_>> = match operation {
                "append" => Box::pin(async { file.append(Vec::from(&b"-tail"[..])).await.unwrap(); }),
                "overwrite" => Box::pin(async { file.overwrite_all(Vec::from(&b"new"[..])).await.unwrap(); }),
                "truncate" => Box::pin(async { file.truncate(3).await.unwrap(); }),
                "flush" => Box::pin(async { file.flush(FileFlushMode::DataAndMetadata).await.unwrap(); }),
                _ => unreachable!(),
            };
            assert!(block_on(futures_lite::future::poll_once(pending.as_mut())).is_none());
            let (second_started_sender, second_started_receiver) = std::sync::mpsc::channel();
            let (second_release_sender, second_release_receiver) = std::sync::mpsc::channel();
            let second_worker = blocking::unblock(move || {
                second_started_sender.send(()).unwrap();
                second_release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            });
            first_release_sender.send(()).unwrap();
            block_on(first_worker);
            second_started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
            assert!(block_on(futures_lite::future::poll_once(pending.as_mut())).is_none());
            // 截断在提交前读取长度，第三个真实执行门只让这一步先结束，
            // 然后保留真正set_len任务；不能把取消提交前准备冒充完成链验证。
            let (last_release_sender, last_worker) = if operation == "truncate" {
                let (third_started_sender, third_started_receiver) = std::sync::mpsc::channel();
                let (third_release_sender, third_release_receiver) = std::sync::mpsc::channel();
                let third_worker = blocking::unblock(move || {
                    third_started_sender.send(()).unwrap();
                    third_release_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
                });
                second_release_sender.send(()).unwrap();
                block_on(second_worker);
                third_started_receiver.recv_timeout(std::time::Duration::from_secs(15)).unwrap();
                assert!(block_on(futures_lite::future::poll_once(pending.as_mut())).is_none());
                (third_release_sender, third_worker)
            } else {
                (second_release_sender, second_worker)
            };
            drop(pending);
            drop(file);
            // 已提交的真实任务独自拥有剩余活动；若仍在准备阶段，此断言显式失败。
            assert_eq!(lock_unpoisoned(&coordination.data).coordinated_resources, 1);
            let native_state = lock_unpoisoned(&native_core.lifecycle_state);
            last_release_sender.send(()).unwrap();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while lock_unpoisoned(&coordination.data).coordinated_resources != 0
                && std::time::Instant::now() < deadline
            {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let activity_releasing = lock_unpoisoned(&coordination.data).coordinated_resources == 0;
            let file_closed_before_release = weak_file_slot.upgrade().is_none();
            drop(native_state);
            block_on(last_worker);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while weak_file_slot.upgrade().is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(weak_file_slot.upgrade().is_none(), "real completion must release the file slot");
            drop(coordination);
            // 文件已先关闭不等于后续租约析构已结束；等待真实完成，不把解除
            // 观测锁或Weak变空当成同步完成/原生资格已经释放的证据。
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
            while weak_coordination.upgrade().is_some() && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(weak_coordination.upgrade().is_none());
            loop {
                match OwnedOpenAdmission::try_acquire(
                    Arc::clone(&root), std::fs::File::open(&path).unwrap(), OpenAdmissionKind::Exclusive,
                ) {
                    Ok(admission) => { drop(admission); break; }
                    Err(error) => {
                        assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                        assert!(std::time::Instant::now() < deadline, "real completion must release native qualification");
                        std::thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
            }
            let expected: &[u8] = match operation {
                "append" => b"original-tail", "overwrite" => b"new", "truncate" => b"ori", "flush" => b"original", _ => unreachable!(),
            };
            assert_eq!(std::fs::read(&path).unwrap(), expected);
            observations.push((operation, activity_releasing, file_closed_before_release));
        }
        println!("COMPLETION_CLOSE_OBSERVATIONS {observations:?}");
        for (operation, activity_releasing, file_closed) in observations {
            assert!(activity_releasing, "actual task must reach activity release; operation={operation}");
            assert!(file_closed, "file must close before activity release; operation={operation}");
        }
        println!("EXCLUSIVE_CHILD_VERIFIED cancelled-completion-file-close");
    }
}
