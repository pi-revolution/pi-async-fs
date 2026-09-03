#![doc = include_str!("../README.md")]

#![feature(associated_type_defaults)]

// 失败资产访问器与拆解方法刻意重申 `must_use`：即使其当前返回类型自身已经
// 带该属性，公开合同仍要求调用方注意不能静默丢弃错误、buffer 或恢复证据。
#![allow(clippy::double_must_use)]

// 第三批次远端定位符声明仍保留占位体，其参数和私有字段暂时不会被读取。
// 本地实现不依赖这些占位入口；远端批次完成后必须删除本临时豁免。
#![allow(dead_code, unused_variables)]

mod available_space;
mod bounded_blocking;
mod copy;
mod create;
mod directory;
mod cross_process_coordination;
mod file_access;
mod file_flush;
mod file_io;
mod growable_read_buffer;
mod locator;
mod local;
mod metadata;
mod mmap_handle;
mod mmap_range;
mod namespace;
mod overwrite;
mod read_growth;
mod read_target;
mod rename;
mod remove;
mod transfer;
mod walk;
mod write_buffer;
mod write_buffer_impls;

pub use available_space::AvailableSpace;
pub use bounded_blocking::set_local_blocking_capacity;
pub use copy::{
    CopyFailure, CopyOutcome, CopyStagingEvidence, CopyTargetEvidence,
};
pub use create::{
    CreateDirectoriesFailure, CreateFailure, CreateTargetEvidence,
    CrossProcessCreateSuccess,
};
pub use directory::{DirectoryEntry, EntryName};
pub use cross_process_coordination::{
    set_cross_process_coordination_root, CrossProcessCoordinationRoot,
    CrossProcessFileAuthority,
};
pub use file_access::FileAccessMode;
pub use file_flush::FileFlushMode;
pub use file_io::FileIo;
pub use growable_read_buffer::GrowableReadBuffer;
pub use locator::{FileLocator, RemoteLocator, RemoteLocatorError};
pub use local::{LocalFile, LocalFileNamespace};
pub use metadata::{FileMetadata, FileTimes, FileType, PortablePermissions, ResourceVersion};
pub use mmap_handle::{ReadMmapHandle, ReadWriteMmapHandle};
pub use mmap_range::{MmapRange, MmapRangeError};
pub use namespace::{BoxDirectoryStream, BoxWalkStream, FileNamespace};
pub use overwrite::{OverwriteFailure, OverwriteTargetEvidence};
pub use read_growth::{ReadGrowthLimit, ReadGrowthLimitError, ReadGrowthOutcome};
pub use read_target::{ReadTargetRegion, ReadTargetRegionError};
pub use rename::{RenameCommitEvidence, RenameFailure};
pub use remove::{RemoveFailure, RemoveTargetEvidence};
pub use transfer::{BufferFailure, TransferProgress};
pub use walk::{WalkDepthLimit, WalkEntry, WalkOptions};
pub use write_buffer::DetachableWriteBuffer;
