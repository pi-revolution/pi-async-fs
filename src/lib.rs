//! 运行时无关的异步文件系统公共合同。
//!
//! 本 crate 面向本地文件系统与远端对象存储，公开接口不绑定具体异步运行时
//! 或后端类型。第一批次只冻结并声明公共合同；当前版本仅用于接口评审，
//! 尚不提供真实文件操作。
//! 当前声明使用关联类型默认值，因此需要 nightly Rust；语言特性稳定后可在
//! 不改变合同语义的前提下支持 stable Rust。
//!
//! 当前主要公共模型包括：
//!
//! - 文件位置：[`RemoteLocator`]、[`FileLocator`]；
//! - 元信息：[`FileType`]、[`PortablePermissions`]、[`FileTimes`]、
//!   [`ResourceVersion`]、[`FileMetadata`]；
//! - 目录枚举：[`EntryName`]、[`DirectoryEntry`]；
//! - 固定读取目标内存窗口：[`ReadTargetRegion`]；
//! - 增长型读取资源上限与成功结果：[`ReadGrowthLimit`]、
//!   [`ReadGrowthOutcome`]；
//! - 文件内容传输进度，以及仅用于写侧普通错误的原始
//!   缓冲区恢复载体：[`TransferProgress`]、[`BufferFailure`]；
//! - 写入源按需脱离能力：[`DetachableWriteBuffer`]；
//! - 增长型读取承载能力：[`GrowableReadBuffer`]；
//! - 普通文件强刷新最低保证等级：[`FileFlushMode`]；
//! - 已打开文件资源的单一访问角色：[`FileAccessMode`]；
//! - 全文件覆盖失败时的目标状态证据与恢复载体：
//!   [`OverwriteTargetEvidence`]、[`OverwriteFailure`]。
//! - 单目标与递归目录层级创建的成功/失败结果：
//!   [`CrossProcessCreateSuccess`]、[`CreateTargetEvidence`]、[`CreateFailure`]、
//!   [`CreateDirectoriesFailure`]。
//! - 单目标 namespace 移除的提交证据与失败载体：
//!   [`RemoveTargetEvidence`]、[`RemoveFailure`]。
//! - 严格不替换改名的提交证据与失败载体：
//!   [`RenameCommitEvidence`]、[`RenameFailure`]；
//! - 原子不替换复制的成功结果、目标/暂存证据与失败载体：
//!   [`CopyOutcome`]、[`CopyTargetEvidence`]、[`CopyStagingEvidence`]、
//!   [`CopyFailure`]；
//! - 内存映射范围及透明句柄：[`MmapRange`]、[`ReadMmapHandle`]、
//!   [`ReadWriteMmapHandle`]；
//! - 递归遍历配置与结果：[`WalkDepthLimit`]、[`WalkOptions`]、[`WalkEntry`]；
//! - 本地跨进程协调的全局根位置策略与不透明授权：
//!   [`CrossProcessCoordinationRoot`]、[`CrossProcessFileAuthority`]；
//! - 定位符所在存储作用域的当前候选可用空间快照：[`AvailableSpace`]。
//!
//! 当前第一批次只声明 [`FileNamespace`] 与 [`FileIo`] 两组运行时无关异步
//! 能力合同。原规划中的 `FileWatch` 已明确延期：本批次不设计或声明监控
//! helper、事件流、关联类型与方法。文件监控将在后续独立完成需求和 API
//! 对齐后加入。

#![feature(associated_type_defaults)]

// 第一批次要求所有方法体仅使用 `todo!()`，因此参数和私有字段在声明阶段
// 必然尚未被读取。开始批次二真实实现时必须删除本临时豁免，让相同 lint
// 重新成为实现质量门禁；其它 unused 类别和全部 Clippy 规则保持启用。
#![allow(dead_code, unused_variables)]

mod available_space;
mod copy;
mod create;
mod directory;
mod cross_process_coordination;
mod file_access;
mod file_flush;
mod file_io;
mod growable_read_buffer;
mod locator;
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
