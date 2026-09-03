//! 运行时无关的异步文件系统公共合同。
//!
//! 本 crate 面向本地文件系统与远端对象存储，公开接口不绑定具体异步运行时
//! 或后端类型。公共合同已经完成首轮冻结；当前版本仍在持续开发，尚未进入
//! 最终稳定发布状态。
//! 当前声明使用关联类型默认值，因此需要 nightly Rust；语言特性稳定后可在
//! 不改变合同语义的前提下支持 stable Rust。
//!
//! # 跨线程安全的依赖图前置条件
//!
//! 最终应用不得为 `pi_atom` 使用的同一份 `pi_share` 启用 `rc` feature。
//! 该组合不在本 crate 的支持范围内，可能使安全 Rust 中的并发 `Atom` 克隆或
//! 释放形成数据竞争。Cargo 会合并同版本依赖的 feature，本 crate 无法只从自己
//! 的 feature 表禁止下游启用该组合；应用必须在依赖、feature、目标平台或锁定
//! 版本变化后审计最终解析图，例如运行
//! `cargo tree -e features -i pi_share`。这项前置条件适用于 [`EntryName::Remote`]
//! 及所有包含或产生远端目录项名称的值、Future 和 Stream。
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
//! - 本地受控阻塞工作的进程级背压配置：
//!   [`set_local_blocking_capacity`]。
//!
//! [`FileNamespace`] 与 [`FileIo`] 构成两组运行时无关异步能力合同。原规划
//! 中的 `FileWatch` 已明确延期：当前不设计或声明监控
//! helper、事件流、关联类型与方法。文件监控将在后续独立完成需求和 API
//! 对齐后加入。

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
