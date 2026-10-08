# pi_async_fs

`pi_async_fs` 是一个运行时无关的异步目录、文件与字节 I/O 库。公共接口只使用
标准 `Future`、`futures_core::Stream`、拥有型资源和明确的缓冲区合同；调用方可以
在自己的异步执行环境中等待这些 Future 和推动这些 Stream，而不需要把某一种
异步运行时的专用类型写入业务接口。

当前版本提供可直接使用的本地文件系统适配器 `LocalFileNamespace` 和
`LocalFile`，支持 Windows、Linux 以及 Linux `tmpfs`。远端定位符和统一定位符的
类型形状已经公开，但远端文件系统适配器尚未提供；不要在生产代码中调用
`RemoteLocator` 的占位方法，也不要把 `FileLocator::Remote` 交给本地适配器。

## 主要功能

- 查询文件、目录、符号链接以及可移植元信息；
- 查询路径所属 Windows 卷或 Linux 挂载文件系统的候选可用空间；
- 惰性枚举目录直接子项，以及按深度限制递归遍历目录后代；
- 严格创建目录、递归创建目录层级、严格创建文件；
- 进程内安全删除文件或空目录，以及显式的未协调删除；
- 严格不替换改名和严格不替换复制；
- 固定窗口随机读取、固定窗口顺序读取和有上限的增长式读取；
- 严格文件尾部追加、全文件覆盖和只允许缩短的截断；
- 指定非空半开范围的只读或读写内存映射（MMAP）；
- 普通文件刷新和读写映射刷新；
- 同一进程内、跨独立文件资源和线程的冲突操作协调；
- 显式、合作式的跨进程协调能力；
- 写入失败时返还原始缓冲区，并同时报告可证明的传输进度和目标状态。

当前版本不提供文件变化监听、`remove_dir_all`、替换式改名、顺序写、任意偏移的
普通写、目录树事务或远端对象存储能力。`remove_dir` 只删除空目录。

## 依赖

基本依赖可以写为：

```toml
[dependencies]
pi_async_fs = "0.1.0"
pi_result = "0.1.1"
```

若调用方需要使用本文的 `BytesMut` 或 Stream 扩展示例，再直接加入对应依赖：

```toml
bytes = "1.12"
futures-lite = "2.6"
```

### 跨线程安全前置条件

最终应用的完整依赖图不得为 `pi_atom` 使用的同一份 `pi_share` 启用 `rc` feature。
该组合不在本库支持范围内，可能使安全 Rust 中并发使用远端名称类型失去线程安全
保证。Cargo 会合并同版本依赖的 feature，本库不能仅通过自己的 feature 表阻止
下游启用它；依赖、feature、目标平台或锁定版本发生变化后，请检查实际解析图：

```text
cargo tree -e features -i pi_share
```

受支持的依赖图只允许相应的 `pi_share/default`，不能包含 `pi_share/rc`。

## 使用模型

本库把“通过位置管理名称空间”和“操作一个已打开文件”分成两个公共接口：

1. `FileNamespace` 接收定位符，负责查询、枚举、创建、删除、改名、复制和打开；
2. `FileIo` 操作已经打开的文件，负责读取、追加、覆盖、截断、映射和刷新；
3. `FileAccessMode` 在打开或创建时为资源指定唯一角色；
4. 同一文件需要不同角色时，分别打开多个独立、不可克隆的文件资源；
5. 完成写入后，需要何种持久性保证由调用方通过显式刷新表达。

`LocalFileNamespace::new()` 返回轻量、可克隆且 `Send + Sync` 的本地 namespace
入口。同一进程中它的所有实例和克隆遵守相同的公开协调语义。`LocalFile` 是拥有型
活动资源，不实现 `Clone`、`Copy` 或 `Default`；需要第二个资源时必须再次调用
`open`，不能复制一个已经打开的资源。

所有异步方法返回 `Send` Future。Future 可以在其借用生命周期内在线程之间迁移，
但接口不会替调用方启动或选择异步运行时。从未被轮询的 Future 不产生文件系统
副作用；一个操作开始后再丢弃 Future 不等于撤销或回滚该操作。

## 快速开始：追加、刷新和读取

下面的函数严格新建文件，追加完整内容，显式刷新，再通过独立读取资源读取文件头。
函数本身可以由调用方选择的任意兼容执行器驱动。

```rust
use std::path::PathBuf;

use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace,
    LocalFileNamespace, ReadTargetRegion,
};

async fn write_then_read(path: PathBuf) -> pi_result::Result<[u8; 5]> {
    let namespace = LocalFileNamespace::new();
    let mut appender = match namespace
        .create_new(&path, FileAccessMode::Append)
        .await
    {
        Ok(file) => file,
        Err(failure) => return Err(failure.into_parts().0),
    };

    match appender.append(b"hello".to_vec()).await {
        Ok(()) => {}
        Err(failure) => return Err(failure.into_parts().0),
    }
    appender.flush(FileFlushMode::DataAndMetadata).await?;
    drop(appender);

    let reader = namespace.open(&path, FileAccessMode::Read).await?;
    let mut header = [0_u8; 5];
    reader
        .read_exact_at(0, &mut header, ReadTargetRegion::full())
        .await?;
    Ok(header)
}
```

## 公共类型总览

### 核心接口、适配器与异步流

| 类型 | 中文名称 | 作用与使用位置 |
| --- | --- | --- |
| `FileNamespace` | 文件命名空间接口 | 通过定位符查询和修改名称空间，并创建独立文件资源；包含 5 个关联类型和 22 个异步方法 |
| `FileIo` | 已打开文件输入输出接口 | 操作一个已经绑定访问角色的文件资源；包含 15 个异步方法，没有关联类型 |
| `LocalFileNamespace` | 本地文件命名空间 | 内置的 `FileNamespace<Locator = PathBuf, File = LocalFile>` 适配器；可克隆、可跨线程共享 |
| `LocalFile` | 本地文件资源 | 只能由 `LocalFileNamespace` 交付的不可克隆文件资源；满足 `FileIo + Send + Sync` |
| `BoxDirectoryStream<'a, L>` | 装箱目录项流 | 默认的 `Send` 浅层目录流，每项为 `pi_result::Result<DirectoryEntry<L>>` |
| `BoxWalkStream<'a, L>` | 装箱递归遍历流 | 默认的 `Send` 递归流，每项为 `pi_result::Result<WalkEntry<L>>` |

### 定位符、目录项和元信息

| 类型 | 中文名称 | 作用与关键边界 |
| --- | --- | --- |
| `RemoteLocator` | 远端定位符 | 拥有型、受校验的层级 URL；当前仅保留公共类型，行为等待远端批次 |
| `RemoteLocatorError` | 远端定位符错误 | 表达 URL 无效、非层级、本地协议、凭据、查询串或片段等输入问题 |
| `FileLocator` | 文件定位符 | `NativePath(PathBuf)` 或 `Remote(RemoteLocator)` 的统一位置枚举；本地适配器直接使用 `PathBuf` |
| `EntryName` | 目录项名称 | `Native(OsString)` 无损保存本地名称，`Remote(Atom)` 保存远端名称 |
| `DirectoryEntry<L>` | 目录项 | 包含直接名称、完整定位符和可选类型提示；不是打开文件，也不会自动加载完整元信息 |
| `FileType` | 文件类型 | 表达普通文件、目录、符号链接、远端对象、设备、FIFO、套接字、其它或未知 |
| `PortablePermissions` | 可移植权限摘要 | 用 `Option<bool>` 表达只读和可执行状态；未知不等于 `false`，也不替代 ACL 或平台权限模型 |
| `FileTimes` | 文件时间快照 | 分别保存可选的创建、修改、访问和元信息变化时间；仅表示观察值，不证明因果顺序 |
| `ResourceVersion<L>` | 资源版本令牌 | 把定位符与后端给出的不透明字节令牌绑定；不是时间戳，也不能跨 namespace 比较新旧 |
| `FileMetadata<L>` | 文件元信息快照 | 聚合类型、可选长度、权限、时间和可选版本；返回后可以立即因外部变化而过期 |
| `AvailableSpace` | 可用空间快照 | 一次查询得到的候选可用字节数；不是配额、预留或后续写入成功保证 |

`DirectoryEntry` 的公开字段是：

- `name: EntryName`：相对于被枚举目录的直接子项名称；
- `locator: L`：可交回同一逻辑 namespace 的完整定位符；
- `file_type_hint: Option<FileType>`：枚举时得到的轻量提示，可能缺失或过期。

完整元信息必须通过 `metadata(&entry.locator)` 或
`symlink_metadata(&entry.locator)` 延迟查询。不要把 `file_type_hint` 当作后续操作的
原子前置条件。

`FileMetadata` 的公开字段是：

- `file_type: FileType`；
- `byte_len: Option<u64>`，其中 `Some(0)` 是已知空内容，`None` 是未知或不适用；
- `permissions: PortablePermissions`；
- `times: FileTimes`；
- `version: Option<ResourceVersion<L>>`。

其它拥有公开字段的数据类型如下：

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `PortablePermissions::read_only` | `Option<bool>` | 明确只读、明确非只读或未知/不适用 |
| `PortablePermissions::executable` | `Option<bool>` | 明确可执行、明确不可执行或未知/不适用 |
| `FileTimes::created` | `Option<SystemTime>` | 后端可靠提供的创建或出生时间；Unix `ctime` 不属于本字段 |
| `FileTimes::modified` | `Option<SystemTime>` | 内容最后修改时间快照 |
| `FileTimes::accessed` | `Option<SystemTime>` | 后端可靠提供的最后访问时间快照 |
| `FileTimes::metadata_changed` | `Option<SystemTime>` | 元信息最后变化时间快照，与创建时间不同 |
| `WalkOptions::depth_limit` | `WalkDepthLimit` | 本次递归遍历允许产生的最大后代深度 |
| `WalkEntry::entry` | `DirectoryEntry<L>` | 当前递归遍历项目 |
| `WalkEntry::depth` | `NonZeroUsize` | 相对遍历根的深度，直接子项为 1，根本身不产生 |

### 主要值枚举

| 类型 | 公开变体 | 使用说明 |
| --- | --- | --- |
| `EntryName` | `Native(OsString)`、`Remote(Atom)` | 本地名称不要求 UTF-8；远端名称不是跨进程序列化格式 |
| `FileLocator` | `NativePath(PathBuf)`、`Remote(RemoteLocator)` | 只表示位置种类，不表示目标存在或已经打开 |
| `FileType` | `RegularFile`、`Directory`、`SymbolicLink`、`RemoteObject`、`BlockDevice`、`CharacterDevice`、`Fifo`、`Socket`、`Other`、`Unknown` | `Other` 表示已知但未单列的种类；`Unknown` 表示无法可靠分类 |
| `FileFlushMode` | `Data`、`DataAndMetadata` | 后者请求更强的普通文件刷新等级；两者都不刷新父目录，也不替代映射专属刷新保证 |
| `WalkDepthLimit` | `Unlimited`、`Limited { max_depth }` | `max_depth` 是包含式上限；零表示不产生任何后代 |
| `CrossProcessCoordinationRoot` | `FileParent`、`Custom(PathBuf)` | `Custom` 必须使用非空绝对路径，并在首次协调操作前配置 |

输入校验错误枚举均为 `#[non_exhaustive]`：

| 类型 | 当前公开变体 |
| --- | --- |
| `RemoteLocatorError` | `InvalidUrl { source }`、`NonHierarchical`、`FileScheme`、`Credentials`、`Query`、`Fragment` |
| `MmapRangeError` | `Empty { at }`、`Reversed { start, end_exclusive }` |
| `ReadTargetRegionError` | `ExcludedStartOverflow`、`IncludedEndOverflow`、`Reversed { start, end_exclusive }`、`OutOfBounds { start, end_exclusive, initialized_len }` |
| `ReadGrowthLimitError` | `FinalLengthOverflow { current_len, max_additional_bytes }` |

### 访问模式、范围和读取结果

| 类型 | 中文名称 | 作用与关键边界 |
| --- | --- | --- |
| `FileAccessMode` | 文件访问模式 | 为一个文件资源选择唯一角色，防止读取、追加、覆盖、截断和映射权限被含糊组合 |
| `FileFlushMode` | 文件刷新模式 | `Data` 请求数据刷新；`DataAndMetadata` 还请求文件自身必要元信息刷新 |
| `MmapRange` | 内存映射范围 | 经过校验的非空文件半开区间 `[start, end)` |
| `MmapRangeError` | 映射范围错误 | `Empty` 表示空范围，`Reversed` 表示起点大于排他终点 |
| `ReadTargetRegion` | 读取目标区域 | 指定固定读取要覆盖的、已经初始化的目标缓冲区范围 |
| `ReadTargetRegionError` | 读取目标区域错误 | 表达端点换算溢出、反向范围或超出已初始化目标长度 |
| `ReadGrowthLimit` | 读取增长上限 | 限制一次增长读取最多向目标尾部新增多少有效字节 |
| `ReadGrowthLimitError` | 读取增长上限错误 | 表达当前长度与新增上限相加发生 `usize` 溢出 |
| `ReadGrowthOutcome` | 读取增长结果 | `EndOfFile` 表示已观察到 EOF；`LimitReached` 表示本轮预算用尽 |
| `WalkDepthLimit` | 遍历深度上限 | `Unlimited` 或包含式 `Limited { max_depth }`；零深度产生空流 |
| `WalkOptions` | 遍历选项 | 当前公开字段 `depth_limit` 控制递归深度 |
| `WalkEntry<L>` | 递归遍历项 | 把 `DirectoryEntry<L>` 与相对根的非零深度绑定；直接子项深度为 1 |

`ReadTargetRegion` 描述的是目标缓冲区范围，不是文件范围。它支持 Rust 常见的
`Range`、`RangeInclusive`、`RangeFrom`、`RangeTo`、`RangeToInclusive` 和
`RangeFull` 转换；`full()` 表示目标当前全部已初始化字节。固定读取不会把
`Vec` 或 `BytesMut` 的备用容量视为已初始化内存，也不会自动扩大目标长度。

### 缓冲区能力和映射句柄

| 类型 | 中文名称 | 作用与关键边界 |
| --- | --- | --- |
| `DetachableWriteBuffer` | 可脱离写入缓冲区 | 写入源能力；提供连续只读字节视图，并能在需要时得到独立存活的 `Detached` 及配对 `Recovery` |
| `GrowableReadBuffer` | 可增长读取缓冲区 | 增长读取目标的语义标记；当前内置支持 `Vec<u8>` 和 `bytes::BytesMut` |
| `ReadMmapHandle` | 只读映射句柄 | 可克隆、`Send + Sync` 的透明只读范围守卫；通过 `as_bytes()` 取得零字节复制视图 |
| `ReadWriteMmapHandle` | 读写映射句柄 | 不可克隆、`Send` 但非 `Sync`；通过独占借用修改并显式刷新映射内容 |

`DetachableWriteBuffer` 不要求 `Clone`、`Sync` 或外层值为 `'static`。它具有：

```text
pub trait DetachableWriteBuffer: AsRef<[u8]> + Sized {
    type Detached: AsRef<[u8]> + 'static;
    type Recovery;

    fn try_detach(
        self,
    ) -> pi_result::RawResult<
        (Self::Detached, Self::Recovery),
        BufferFailure<Self>,
    >;

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self;
}
```

`GrowableReadBuffer` 没有新增方法或关联类型。它在
`AsRef<[u8]> + bytes::BufMut` 之上承诺：旧内容保持为前缀，新读取的字节只提交到
逻辑尾部，提交后 `AsRef<[u8]>` 能看到完整已初始化内容。

### 失败、进度和状态证据

| 类型 | 中文名称 | 作用 |
| --- | --- | --- |
| `TransferProgress` | 传输进度证据 | 表达精确字节数、可靠下界或未知进度 |
| `BufferFailure<B>` | 缓冲区可恢复失败 | 追加失败时返回统一错误、原始 `B` 和传输进度 |
| `OverwriteTargetEvidence` | 覆盖目标状态证据 | 表达目标未变、是精确输入前缀、已开始变化或未知 |
| `OverwriteFailure<B>` | 覆盖写失败 | 返回错误、原始 buffer、进度和目标状态证据 |
| `CreateTargetEvidence` | 创建目标状态证据 | 表达本操作未创建、已经创建或无法确认 |
| `CrossProcessCreateSuccess<A, F>` | 协调创建成功结果 | 同时拥有跨进程 authority 和首个文件资源 |
| `CreateFailure` | 单目标创建失败 | 返回错误和目标创建状态证据 |
| `CreateDirectoriesFailure<L>` | 目录层级创建失败 | 返回错误、已确认创建的定位符和状态不确定的定位符 |
| `RemoveTargetEvidence` | 删除目标状态证据 | 表达名称未被本操作移除、已移除或未知 |
| `RemoveFailure` | 删除失败 | 返回错误和删除状态证据 |
| `RenameCommitEvidence` | 改名提交证据 | 表达名称切换未提交、已提交或未知 |
| `RenameFailure` | 改名失败 | 返回错误和改名提交证据 |
| `CopyTargetEvidence` | 复制目标发布证据 | 表达最终目标确定未发布，或发布状态未知及对应内容长度 |
| `CopyStagingEvidence` | 复制临时资产证据 | 表达本操作未创建、已清理、已知仍存在或状态未知 |
| `CopyOutcome` | 复制成功结果 | 保存成功发布内容的字节长度 |
| `CopyFailure` | 复制失败 | 返回错误、最终目标证据和复制临时资产证据 |

状态和进度枚举的当前公开变体如下。它们均为 `#[non_exhaustive]`，调用方必须保留
通配分支，以便兼容未来新增的、更精确的证据：

| 类型 | 公开变体及含义 |
| --- | --- |
| `TransferProgress` | `Exact { bytes }`：精确完成量；`AtLeast { bytes }`：可靠下界；`Unknown`：没有可靠下界 |
| `OverwriteTargetEvidence` | `Unchanged`：本操作未改变；`ExactInputPrefix { bytes }`：完整目标是输入的精确前缀；`MutationStarted`：已经开始变化；`Unknown`：无法证明 |
| `CreateTargetEvidence` | `NotCreatedByOperation`、`CreatedByOperation`、`Unknown` |
| `RemoveTargetEvidence` | `NotRemovedByOperation`、`RemovedByOperation`、`Unknown` |
| `RenameCommitEvidence` | `NotRenamedByOperation`、`RenamedByOperation`、`Unknown` |
| `CopyTargetEvidence` | `NotPublishedByOperation`；`PublicationUnknown { content_byte_len }` |
| `CopyStagingEvidence` | `NotCreatedByOperation`、`CreatedThenRemovedByOperation`、`KnownPresentAtCompletion`、`Unknown` |
| `ReadGrowthOutcome` | `EndOfFile { appended_bytes }`；`LimitReached { appended_bytes }` |

证据只表示“本次操作返回时能够可靠证明什么”，不是对外部世界的永久快照。
`Unknown` 不表示副作用一定发生，也不表示一定没有发生；调用方必须重新查询相关
位置。所有标为 `#[non_exhaustive]` 的公开枚举都应使用通配分支匹配。

### 跨进程协调类型

| 类型或函数 | 中文名称 | 作用与关键边界 |
| --- | --- | --- |
| `CrossProcessCoordinationRoot` | 跨进程协调根策略 | `FileParent` 使用目标父目录策略；`Custom(PathBuf)` 指定所有协调操作共同使用的绝对根位置 |
| `CrossProcessFileAuthority` | 跨进程文件授权 | 不透明、拥有型能力值，绑定一个具体目标实例；不是操作系统访问权限，也不能序列化后交给其它进程 |
| `set_cross_process_coordination_root` | 设置协调根策略 | 在首次协调操作前设置一次；相同值重复设置成功，不同值冲突，不提供重置 |

跨进程保证是合作式合同：所有会影响目标身份、长度、内容、映射或名称的相关进程
都必须遵守同一套公共协调入口，并选择实际指向同一协调位置的根策略。库外句柄、
不合作程序、不同库副本、硬链接别名旁路或不一致的根配置不受 authority 强制约束。

## `FileAccessMode` 能力矩阵

每个文件资源在完整生命周期内只有一个模式。打开 `Overwrite` 或 `Truncate` 资源
本身不会修改文件；只有显式调用相应方法才产生副作用。

| 模式 | 允许的主要 `FileIo` 方法 | 明确不允许 |
| --- | --- | --- |
| `Read` | `byte_len`、六种固定或增长读取 | 追加、覆盖、截断、映射、普通文件刷新 |
| `Append` | `byte_len`、`append`、`flush` | 普通读取、覆盖、截断、映射 |
| `Overwrite` | `byte_len`、`overwrite_all`、`flush` | 普通读取、追加、截断、映射 |
| `ReadMmap` | `byte_len`、只读映射创建 | 普通读取、写入、截断、读写映射、普通文件刷新 |
| `ReadWriteMmap` | `byte_len`、只读或读写映射创建、`flush` | 普通读取、普通写入、截断 |
| `Truncate` | `byte_len`、`truncate`、`flush` | 读取、追加、覆盖、映射、扩容 |

访问模式不表示跨进程协调等级。同一模式既可用于普通资源，也可用于通过 authority
打开的协调资源；具体应调用哪组映射方法由资源来源决定。

## `FileIo` 方法说明

| 方法签名摘要 | 中文名称 | 成功结果与使用语义 |
| --- | --- | --- |
| `byte_len(&self) -> Result<u64>` | 查询字节长度 | 返回一次 `u64` 长度快照；不是版本、锁或后续读取保证 |
| `read_at(&self, offset, &mut B, target) -> Result<usize>` | 固定随机短读 | 从绝对偏移读入目标窗口，允许短读，返回实际字节数；不改变顺序游标 |
| `read_exact_at(&self, offset, &mut B, target) -> Result<()>` | 固定随机精确读 | 只有完整填满目标窗口才成功；不改变顺序游标 |
| `read_to_end_at(&self, offset, &mut B, limit) -> Result<ReadGrowthOutcome>` | 增长随机读 | 从绝对偏移向目标尾部追加，直到 EOF 或本次上限；不改变顺序游标 |
| `read(&mut self, &mut B, target) -> Result<usize>` | 固定顺序短读 | 从当前逻辑游标读取，允许短读，并按成功提交量推进游标 |
| `read_exact(&mut self, &mut B, target) -> Result<()>` | 固定顺序精确读 | 填满目标窗口才成功，并以本次方法合同提交游标变化 |
| `read_to_end(&mut self, &mut B, limit) -> Result<ReadGrowthOutcome>` | 增长顺序读 | 从当前逻辑游标向目标尾部增长，直到 EOF 或本次上限 |
| `append(&mut self, B) -> RawResult<(), BufferFailure<B>>` | 严格尾部追加 | 成功时把输入全部追加到调用时的文件尾；普通失败返回原始 `B` 和进度 |
| `overwrite_all(&mut self, B) -> RawResult<(), OverwriteFailure<B>>` | 全文件覆盖 | 成功后文件内容和长度与输入完全一致；空输入产生空文件 |
| `truncate(&mut self, new_len) -> Result<()>` | 缩短文件 | 只允许 `new_len` 不大于操作时观察到的当前长度；不写入内容，也不允许扩容 |
| `unsafe map_read_only_uncoordinated(&self, range) -> Result<ReadMmapHandle>` | 未协调只读映射 | 仅在调用方自行满足所有库外并发安全前提时使用 |
| `map_read_only(&self, range) -> Result<ReadMmapHandle>` | 协调式只读映射 | 只适用于通过有效 authority 打开的文件资源 |
| `unsafe map_read_write_uncoordinated(&self, range) -> Result<ReadWriteMmapHandle>` | 未协调读写映射 | 在调用方承担外部前提后返回独占可写透明句柄 |
| `map_read_write(&self, range) -> Result<ReadWriteMmapHandle>` | 协调式读写映射 | 只适用于通过有效 authority 打开的文件资源 |
| `flush(&mut self, mode) -> Result<()>` | 刷新普通文件 | 等待请求的普通文件刷新边界，可与映射及独立追加并存；不替代映射专属刷新，不刷新父目录 |

固定读取的 `B` 满足：

```text
B: AsMut<[u8]> + Send + ?Sized + 'a
```

增长读取的 `B` 满足：

```text
B: GrowableReadBuffer + Send + 'a
```

追加和覆盖的 `B` 满足：

```text
B: DetachableWriteBuffer + Send + 'a
B::Detached: Send
B::Recovery: Send + 'a
```

## `FileNamespace` 关联类型和方法说明

### 关联类型

| 关联类型 | 默认类型 | 作用 |
| --- | --- | --- |
| `Locator` | `FileLocator` | namespace 接受的位置值；要求可克隆、可调试、判等、排序、哈希及 `Send + Sync + 'static` |
| `CrossProcessAuthority` | `CrossProcessFileAuthority` | 本地或后端定义的不透明跨进程授权；要求 `Debug + Display + Send + Sync + 'static` |
| `File` | 无 | 每次成功创建或打开交付的独立 `FileIo + 'static` 资源 |
| `DirectoryStream<'a>` | `BoxDirectoryStream<'a, Locator>` | `read_dir` 的流；每项为 `Result<DirectoryEntry<Locator>>`，流为 `Send + 'a` |
| `WalkStream<'a>` | `BoxWalkStream<'a, Locator>` | `walk` 的流；每项为 `Result<WalkEntry<Locator>>`，流为 `Send + 'a` |

### 查询和流

| 方法 | 中文名称 | 成功结果与使用语义 |
| --- | --- | --- |
| `metadata(&locator) -> Result<FileMetadata<Locator>>` | 跟随链接查询元信息 | 返回最终解析目标的拥有型快照；不存在和无法查询是错误 |
| `symlink_metadata(&locator) -> Result<FileMetadata<Locator>>` | 不跟随末级链接查询元信息 | 返回最终名称条目自身的快照，可用于观察悬空链接 |
| `try_exists(&locator) -> Result<bool>` | 探测目标存在性 | 只有权威确认不存在才返回 `Ok(false)`；权限、路径或后端错误返回 `Err` |
| `available_space(&locator) -> Result<AvailableSpace>` | 查询候选可用空间 | 返回目标所属存储作用域的一次字节快照；要求定位符当前存在且可访问 |
| `read_dir(locator) -> Result<DirectoryStream<'a>>` | 枚举直接子项 | 按值取得根位置，返回惰性目录流；根本身不产生，顺序和快照均不保证 |
| `walk(locator, options) -> Result<WalkStream<'a>>` | 递归遍历后代 | 返回带深度的惰性流；根不产生，不跟随目录符号链接递归，不保证遍历顺序 |

### 创建、删除、改名和复制

| 方法 | 中文名称 | 成功结果与使用语义 |
| --- | --- | --- |
| `create_dir(&locator) -> RawResult<(), CreateFailure>` | 严格创建单层目录 | 父目录必须存在，最终位置必须不存在；失败返回创建证据 |
| `create_dir_all(&locator) -> RawResult<(), CreateDirectoriesFailure<Locator>>` | 递归创建目录层级 | 已存在目录可复用；失败不自动回滚此前已创建的目录，并返回两类位置清单 |
| `remove_file(&locator) -> RawResult<(), RemoveFailure>` | 进程内安全删除文件 | 有活动文件资源、映射或冲突操作时拒绝；成功只解除名称绑定 |
| `remove_file_uncoordinated(&locator) -> RawResult<(), RemoveFailure>` | 未协调删除文件 | 不检查或等待本库中的活动资源；仍遵守所在平台的删除结果 |
| `remove_dir(&locator) -> RawResult<(), RemoveFailure>` | 进程内安全删除空目录 | 只删除空目录，并与同一名称条目的冲突 namespace 操作协调 |
| `remove_dir_uncoordinated(&locator) -> RawResult<(), RemoveFailure>` | 未协调删除空目录 | 不取得本库的进程内安全保证；仍只允许删除空目录 |
| `rename(&source, &destination) -> RawResult<(), RenameFailure>` | 严格不替换改名 | 目标必须不存在；不降级为复制再删除，也不提供 replace |
| `copy_new(&source, &destination) -> RawResult<CopyOutcome, CopyFailure>` | 严格不替换复制 | 目标必须不存在；成功目标具有完整复制内容，不复制权限、时间、ACL 或扩展属性 |

### 打开和跨进程协调

| 方法 | 中文名称 | 成功结果与使用语义 |
| --- | --- | --- |
| `open(&locator, access) -> Result<File>` | 打开现有文件 | 返回绑定唯一访问模式的独立文件资源；打开覆盖或截断模式本身不修改内容 |
| `create_new(&locator, access) -> RawResult<File, CreateFailure>` | 严格新建文件 | 仅在最终位置不存在时创建空文件并返回首个资源；失败返回创建证据 |
| `unsafe create_new_coordinated(locator, access) -> RawResult<CrossProcessCreateSuccess<Authority, File>, CreateFailure>` | 协调式严格新建 | 在调用方承担跨进程合作前提后，返回 authority 和首个独立文件资源 |
| `unsafe establish_cross_process_authority(&locator) -> Result<Authority>` | 为现有文件建立授权 | 在调用方承担合作前提后，为当前目标实例返回不透明 authority |
| `open_with_cross_process_authority(&authority, access) -> Result<File>` | 使用授权打开文件 | 不再接收 locator；返回继续受同一跨进程合同约束的独立文件资源 |
| `remove_file_with_cross_process_authority(&authority) -> RawResult<(), RemoveFailure>` | 使用授权删除文件 | 只删除 authority 精确绑定的目标实例；有合作式冲突时立即拒绝 |
| `unsafe resume_coordinated_file_removal(&locator) -> RawResult<(), RemoveFailure>` | 恢复未完成的协调删除 | 只用于所有内存 authority 已消失后的恢复路径；不能用来开始一次普通新删除 |

### Namespace 组合示例

下面的示例覆盖递归创建目录、严格新建文件、查询元信息和空间、严格改名，以及
进程内安全删除。每个专用失败载体都必须先拆出统一错误，不能当作普通
`pi_result::Error` 静默丢弃其状态证据。

```rust
use std::path::PathBuf;

use pi_async_fs::{FileAccessMode, FileNamespace, LocalFileNamespace};

async fn namespace_lifecycle(
    root: PathBuf,
) -> pi_result::Result<(u64, Option<u64>)> {
    let namespace = LocalFileNamespace::new();
    let directory = root.join("records");
    let source = directory.join("pending.bin");
    let destination = directory.join("ready.bin");

    if let Err(failure) = namespace.create_dir_all(&directory).await {
        let (error, _confirmed_created, _uncertain_targets) = failure.into_parts();
        return Err(error);
    }

    let file = match namespace
        .create_new(&source, FileAccessMode::Append)
        .await
    {
        Ok(file) => file,
        Err(failure) => return Err(failure.into_parts().0),
    };
    drop(file);

    let metadata = namespace.metadata(&source).await?;
    let available = namespace.available_space(&source).await?;

    if let Err(failure) = namespace.rename(&source, &destination).await {
        return Err(failure.into_parts().0);
    }
    if let Err(failure) = namespace.remove_file(&destination).await {
        return Err(failure.into_parts().0);
    }
    if let Err(failure) = namespace.remove_dir(&directory).await {
        return Err(failure.into_parts().0);
    }

    Ok((available.available_bytes(), metadata.byte_len))
}
```

## 公共构造器与访问器说明

下表覆盖当前所有公共固有函数、固有方法和两个公共自由函数。失败载体的
`into_parts` 均消费自身并按照对应 `new` 的参数顺序返还全部资产。

| 类型或函数 | 公共成员 | 说明 |
| --- | --- | --- |
| `LocalFileNamespace` | `new()` | 取得本地 namespace；不打开文件，也不执行文件操作 |
| `set_local_blocking_capacity` | `(NonZeroUsize) -> pi_result::Result<()>` | 设置进程级本地阻塞准入容量；相同值可重复设置，不同值冲突 |
| `set_cross_process_coordination_root` | `(CrossProcessCoordinationRoot) -> pi_result::Result<()>` | 设置一次性跨进程协调根策略；不提供重置 |
| `AvailableSpace` | `new`、`available_bytes` | 构造快照或读取其 `u64` 字节数 |
| `MmapRange` | `new`、`start`、`end_exclusive`、`len` | 校验、检查非空半开文件范围；也支持 `TryFrom<Range<u64>>` |
| `ReadTargetRegion` | `new`、`full`、`start`、`end_exclusive`、`resolve` | 构造逻辑目标区域，并按具体缓冲区初始化长度解析为安全 `Range<usize>` |
| `ReadGrowthLimit` | `new`、`max_additional_bytes`、`checked_final_len` | 构造增长预算、读取预算、受检计算最大最终长度 |
| `ReadGrowthOutcome` | `appended_bytes`、`is_end_of_file`、`is_limit_reached` | 统一读取成功结果中的新增量和停止原因 |
| `RemoteLocator` | `new`、`as_url`、`as_str`、`into_url` | 校验构造、借用或取回 URL；当前行为等待远端批次 |
| `DirectoryEntry<L>` | `new(name, locator, file_type_hint)` | 按公开字段顺序构造拥有型目录项 |
| `ResourceVersion<L>` | `new`、`locator`、`token`、`into_parts` | 绑定位置和不透明 `Arc<[u8]>` 令牌，或借用/拆解它们 |
| `FileMetadata<L>` | `new(file_type, byte_len, permissions, times, version)` | 按公开字段顺序构造元信息快照 |
| `WalkDepthLimit` | `maximum_depth` | `Unlimited` 返回 `None`，有限深度返回 `Some(max_depth)` |
| `WalkOptions` | `new(depth_limit)` | 显式构造遍历选项；没有隐式默认深度 |
| `WalkEntry<L>` | `new(entry, depth)`、`into_parts` | 组合或拆解拥有型目录项与非零相对深度 |
| `TransferProgress` | `minimum_bytes`、`exact_bytes`、`is_exact`、`is_uncertain` | 在不误解下界或未知值的情况下读取进度证据 |
| `BufferFailure<B>` | `new`、`error`、`buffer`、`progress`、`into_parts` | 借用失败信息，或一次性取回错误、原始 buffer 和进度 |
| `OverwriteFailure<B>` | `new`、`error`、`buffer`、`progress`、`target_evidence`、`into_parts` | 借用或取回覆盖错误的全部四项资产 |
| `CrossProcessCreateSuccess<A, F>` | `new`、`authority`、`file`、`file_mut`、`into_parts` | 同时管理 authority 和首个文件资源，或消费后分别取得所有权 |
| `CreateFailure` | `new`、`error`、`target_evidence`、`into_parts` | 借用或取回创建错误和目标证据 |
| `CreateDirectoriesFailure<L>` | `new`、`error`、`confirmed_created`、`uncertain_targets`、`into_parts` | 借用或取回递归创建的错误及两组位置清单 |
| `RemoveFailure` | `new`、`error`、`target_evidence`、`into_parts` | 借用或取回删除错误和目标证据 |
| `RenameFailure` | `new`、`error`、`commit_evidence`、`into_parts` | 借用或取回改名错误和提交证据 |
| `CopyOutcome` | `new`、`content_byte_len` | 构造成功值或读取已发布内容长度 |
| `CopyFailure` | `new`、`error`、`target_evidence`、`staging_evidence`、`into_parts` | 借用或取回复制错误及两类状态证据 |
| `ReadMmapHandle` | `range`、`len`、`as_bytes` | 查询逻辑范围和长度，或借用只读映射字节 |
| `ReadWriteMmapHandle` | `range`、`len`、`as_bytes`、`as_bytes_mut`、`flush` | 查询范围、读取、独占修改和刷新映射内容 |

`PortablePermissions`、`FileTimes`、`WalkOptions`、`WalkEntry`、`DirectoryEntry` 和
`FileMetadata` 的字段本身就是公共数据面；读取这些字段不执行 I/O。具体资源类
型、权限、时间和版本仍然只是对应查询时的快照。

### 常用转换和能力 trait

| 类型 | 公共转换或能力 |
| --- | --- |
| `EntryName` | `From<OsString>`、`From<&OsStr>`、`From<pi_atom::Atom>` |
| `RemoteLocator` | `AsRef<Url>`、`TryFrom<Url>`、`FromStr`、`From<RemoteLocator> for Url`；当前行为等待远端批次 |
| `FileLocator` | `From<PathBuf>`、`From<&Path>`、`From<RemoteLocator>`、`TryFrom<Url>` |
| `MmapRange` | `TryFrom<Range<u64>>` |
| `ReadTargetRegion` | `TryFrom<Range<usize>>`、`TryFrom<RangeInclusive<usize>>`、`From<RangeFrom<usize>>`、`From<RangeTo<usize>>`、`TryFrom<RangeToInclusive<usize>>`、`From<RangeFull>` |
| `ReadMmapHandle` | `AsRef<[u8]>`、`DetachableWriteBuffer`、`Clone + Send + Sync` |
| `ReadWriteMmapHandle` | `AsMut<[u8]> + Send`，明确不实现 `Clone` 和 `Sync` |
| `CrossProcessCoordinationRoot` | `Default`，默认值为 `FileParent` |

值类型按各自字段能力提供 `Debug`、`Display`、判等、排序或哈希；失败载体和泛型
记录只有在其类型参数满足相应约束时才获得条件实现。格式化文本只供人阅读，不是
稳定序列化协议；排序只为集合和确定性输出服务，不表示权限强弱、版本新旧或操作
优先级。

## Buffer 支持矩阵

### 写入源

| 调用时的 `B` | 是否内置支持 | 需要复制字节的典型情况 |
| --- | ---: | --- |
| `Vec<u8>` | 是 | 否 |
| `Box<[u8]>` | 是 | 否 |
| `Arc<[u8]>` | 是 | 否 |
| `Cow<'a, [u8]>` | 是 | borrowed 变体在需要脱离借用期时需要复制 |
| `bytes::Bytes` | 是 | 否 |
| `bytes::BytesMut` | 是 | 否 |
| `&[u8]`、`&Vec<u8>`、`&Box<[u8]>` | 是 | 在需要脱离借用期时复制 |
| `&Arc<[u8]>`、`&bytes::Bytes` | 是 | 不复制字节，但会取得临时共享所有权 |
| `&Cow<'_, [u8]>`、`&bytes::BytesMut` | 是 | 在需要脱离借用期时复制 |
| `ReadMmapHandle`、`&ReadMmapHandle` | 是 | 不复制映射字节；不能作为自身底层文件的写入源 |

数组没有单独实现 `DetachableWriteBuffer`；可以把数组借用为 `&[u8]`。第三方类型
不会仅因为实现 `AsRef<[u8]>` 自动成为合法写入源，还必须可靠实现
`DetachableWriteBuffer` 的脱离、配对恢复和字节等价合同。

### 读取目标

| 类型 | 固定读取目标 | 增长读取目标 | 备注 |
| --- | ---: | ---: | --- |
| `[u8]`、`&mut [u8]`、`[u8; N]` | 是 | 否 | 固定读取只覆盖已初始化范围 |
| `Vec<u8>` | 是 | 是 | 固定读取不改变长度；增长读取向尾部追加 |
| `Box<[u8]>` | 是 | 否 | 固定长度 |
| `bytes::BytesMut` | 是 | 是 | 固定读取覆盖当前初始化内容，增长读取可以扩容 |
| `ReadWriteMmapHandle` | 是 | 否 | 不能把同一文件的可写映射作为该文件普通读取目标 |
| `Arc<[u8]>`、`bytes::Bytes`、`ReadMmapHandle` | 否 | 否 | 只读视图不能作为读取目标 |

## 读取示例

### 固定随机读取和有上限增长读取

```rust
use std::path::PathBuf;

use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace,
    ReadGrowthLimit, ReadGrowthOutcome, ReadTargetRegion,
};

async fn read_two_ways(
    path: PathBuf,
) -> pi_result::Result<([u8; 4], Vec<u8>, ReadGrowthOutcome)> {
    let namespace = LocalFileNamespace::new();
    let file = namespace.open(&path, FileAccessMode::Read).await?;

    let mut header = [0_u8; 4];
    file.read_exact_at(0, &mut header, ReadTargetRegion::full())
        .await?;

    let mut remainder = b"prefix:".to_vec();
    let outcome = file
        .read_to_end_at(4, &mut remainder, ReadGrowthLimit::new(1024 * 1024))
        .await?;

    Ok((header, remainder, outcome))
}
```

`ReadGrowthOutcome::LimitReached` 不证明文件还有下一字节，也不证明已经 EOF；它只
证明本轮允许新增的字节预算已经用尽。若调用方要继续读取，应使用新的明确上限
再次调用，而不是改成无界增长。

随机读取不会改变文件资源的顺序游标。顺序读取使用 `&mut self`，每个独立打开的
资源都有自己的逻辑读取位置；不要从一个资源的读取位置推断另一个资源的位置。

## 目录流示例

```rust
use std::path::PathBuf;

use futures_lite::stream::StreamExt;
use pi_async_fs::{
    DirectoryEntry, FileNamespace, LocalFileNamespace,
    WalkDepthLimit, WalkEntry, WalkOptions,
};

async fn list_children(
    root: PathBuf,
) -> pi_result::Result<Vec<DirectoryEntry<PathBuf>>> {
    let namespace = LocalFileNamespace::new();
    let mut stream = namespace.read_dir(root).await?;
    let mut entries = Vec::new();

    while let Some(item) = stream.next().await {
        entries.push(item?);
    }
    Ok(entries)
}

async fn walk_two_levels(
    root: PathBuf,
) -> pi_result::Result<Vec<WalkEntry<PathBuf>>> {
    let namespace = LocalFileNamespace::new();
    let options = WalkOptions::new(WalkDepthLimit::Limited { max_depth: 2 });
    let mut stream = namespace.walk(root, options).await?;
    let mut entries = Vec::new();

    while let Some(item) = stream.next().await {
        entries.push(item?);
    }
    Ok(entries)
}
```

目录流的创建错误由建立流的 Future 返回；建立成功后发现的错误作为流项目返回。
一个流只允许由一个消费者按标准 `Stream` 规则推动，不保证 `Sync`、`Unpin`、
固定顺序或目录快照。枚举期间并发创建、删除或改名不会由目录流自动互斥，因此
项目可能反映不同观察时刻；需要一致快照的业务必须在更高层建立相应协议。

## 写入失败恢复示例

成功追加返回 `()`，不返回原始 buffer；普通失败通过 `BufferFailure<B>` 返还调用
时的原始 `B`。取消 Future 没有失败值交付通道，因此不能依赖取消取回 buffer。

```rust
use pi_async_fs::{FileIo, TransferProgress};

async fn append_once<F: FileIo>(
    file: &mut F,
    buffer: Vec<u8>,
) -> Result<(), (pi_result::Error, Vec<u8>, TransferProgress)> {
    match file.append(buffer).await {
        Ok(()) => Ok(()),
        Err(failure) => Err(failure.into_parts()),
    }
}
```

`TransferProgress` 的语义如下：

- `Exact { bytes }`：已经确认恰好传输了该字节数；
- `AtLeast { bytes }`：只能确认至少传输了该字节数，实际值可能更大；
- `Unknown`：无法给出可靠下界。

覆盖失败还必须同时检查 `OverwriteTargetEvidence`：

- `Unchanged`：本操作没有改变目标；
- `ExactInputPrefix { bytes }`：目标完整内容可证明为输入的精确前缀；
- `MutationStarted`：目标已经开始变化，但无法给出精确内容；
- `Unknown`：目标状态无法可靠证明。

取回原始 buffer 只解决资产所有权问题，不证明写操作没有产生副作用。除非进度和
目标证据都允许，否则不能从头盲目重试追加或覆盖。

### 覆盖、刷新和缩短示例

覆盖与截断使用不同的资源角色。覆盖成功后内容精确等于输入；截断只缩短长度，
不能用来扩容，也不接收新内容。

```rust
use std::path::PathBuf;

use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace, LocalFileNamespace,
};

async fn replace_then_shorten(path: PathBuf) -> pi_result::Result<()> {
    let namespace = LocalFileNamespace::new();

    let mut overwrite = namespace
        .open(&path, FileAccessMode::Overwrite)
        .await?;
    if let Err(failure) = overwrite.overwrite_all(b"abcdef".to_vec()).await {
        let (error, _buffer, _progress, _target_evidence) = failure.into_parts();
        return Err(error);
    }
    overwrite.flush(FileFlushMode::DataAndMetadata).await?;
    drop(overwrite);

    let mut truncate = namespace
        .open(&path, FileAccessMode::Truncate)
        .await?;
    truncate.truncate(3).await?;
    truncate.flush(FileFlushMode::DataAndMetadata).await?;
    Ok(())
}
```

## 内存映射使用

`MmapRange` 只接受非空半开区间 `[start, end)`。例如 `[0, 4096)` 包含偏移
`0..=4095`；相邻范围 `[0, 4096)` 与 `[4096, 8192)` 不相交。

- 同一文件可以同时存在任意数量的不相交映射；
- 任意相交范围都会被拒绝，不区分只读或读写权限；
- 活动映射与同文件普通读取、覆盖、截断和安全删除互斥；
- 已经建立的只读/可写映射可以与严格尾部追加及普通文件刷新并存；
- 正在执行追加时不能同时建立新映射；
- 后续追加不会扩大既有映射的范围；
- 映射字节只能通过透明句柄访问；
- `ReadWriteMmapHandle::flush` 刷新映射修改，`FileIo::flush` 不会替代它；
- `Drop` 释放句柄拥有的公开资源，但不会隐式刷新修改。

普通资源只能调用两个 `unsafe ..._uncoordinated` 映射入口。它们的 `unsafe` 表示
调用方必须保证其它进程和所有库外访问不会破坏文件长度、身份及映射安全；它不
表示调用方可以绕过本库公开声明的同进程冲突规则。安全的 `map_read_only` 和
`map_read_write` 只接受通过有效跨进程 authority 打开的资源。

### 协调式只读映射示例

```rust
use std::path::PathBuf;

use pi_async_fs::{
    CrossProcessFileAuthority, FileAccessMode, FileIo, FileNamespace,
    LocalFileNamespace, MmapRange, ReadMmapHandle,
};

async fn open_coordinated_mapping(
    path: PathBuf,
) -> pi_result::Result<(CrossProcessFileAuthority, ReadMmapHandle)> {
    let namespace = LocalFileNamespace::new();

    // SAFETY: 应用必须保证所有相关进程及库外访问都遵守同一协调合同，
    // 并且所有合作进程为该目标使用同一物理协调位置。
    let authority = unsafe {
        namespace.establish_cross_process_authority(&path)
    }
    .await?;

    let file = namespace
        .open_with_cross_process_authority(&authority, FileAccessMode::ReadMmap)
        .await?;
    let range = MmapRange::new(0, 4096).expect("固定范围非空且方向正确");
    let mapping = file.map_read_only(range).await?;

    Ok((authority, mapping))
}
```

`ReadMmapHandle::as_bytes()` 返回的切片只在句柄借用期间有效。克隆只读句柄得到的
是同一个逻辑映射的共享读取能力，不是另一次范围申请。最后一个克隆释放后，该
逻辑映射不再可访问。

`ReadWriteMmapHandle` 可以整体移动到另一个线程，但不能共享并发写，也不能克隆。
修改必须使用 `as_bytes_mut()`，需要可观察的刷新结果时必须显式等待 `flush()`。

### 保留映射并追加、刷新

下面的函数保留已有只读映射，追加记录后等待文件刷新，并返回原映射。文件必须
至少有 4096 字节，且调用方提供的授权必须满足跨进程协调合同。追加不会扩大
原映射，也不要求先释放它。

```rust
use pi_async_fs::{
    CrossProcessFileAuthority, FileAccessMode, FileFlushMode, FileIo,
    FileNamespace, LocalFileNamespace, MmapRange, ReadMmapHandle,
};

async fn append_while_mapping(
    authority: &CrossProcessFileAuthority,
) -> pi_result::Result<ReadMmapHandle> {
    let namespace = LocalFileNamespace::new();
    let mapper = namespace
        .open_with_cross_process_authority(authority, FileAccessMode::ReadMmap)
        .await?;
    let mapping = mapper
        .map_read_only(MmapRange::new(0, 4096).expect("范围非空"))
        .await?;
    let mut appender = namespace
        .open_with_cross_process_authority(authority, FileAccessMode::Append)
        .await?;
    if let Err(failure) = appender.append(b"record\n".to_vec()).await {
        return Err(failure.into_parts().0);
    }
    appender.flush(FileFlushMode::DataAndMetadata).await?;
    Ok(mapping)
}
```

可写映射同样允许与追加及文件刷新并存。若还修改了映射内容，应另外等待
`ReadWriteMmapHandle::flush()`；两条写入路径的刷新不构成原子事务。

## 严格复制、改名和删除

### `copy_new`

- 目标必须不存在，存在任意条目都会失败；
- 成功时最终目标具有本次操作承诺的完整内容；
- 不替换现有目标；
- 不复制权限、时间、ACL、扩展属性、备用数据流或后端专有元信息；
- 源文件并发变化时，不提供内容版本快照保证；
- 失败时分别检查 `CopyTargetEvidence` 和 `CopyStagingEvidence`；
- 暂不保证掉电、操作系统崩溃或硬件故障边界上的发布原子性。

```rust
use std::path::PathBuf;

use pi_async_fs::{CopyFailure, FileNamespace, LocalFileNamespace};

async fn copy_without_replacement(
    source: PathBuf,
    destination: PathBuf,
) -> Result<u64, CopyFailure> {
    let namespace = LocalFileNamespace::new();
    namespace
        .copy_new(&source, &destination)
        .await
        .map(|outcome| outcome.content_byte_len())
}
```

### `rename`

`rename` 只提供严格不替换的名称切换。目标存在、跨越不支持的存储作用域或平台
拒绝改名时返回错误；不会自动降级为复制后删除。默认改名不与已经打开的普通内容
资源或活动映射建立额外互斥，平台对占用中的对象有额外限制时，以明确错误返回。

### 删除

`remove_file` 和 `remove_dir` 是进程内安全入口：遇到本库已知的冲突资源或操作时
立即失败，不等待调用方释放资源。`remove_dir` 及其未协调版本都只删除空目录。

`remove_file_uncoordinated` 和 `remove_dir_uncoordinated` 不检查本库的活动资源和
namespace 冲突。它们仍是内存安全的 Rust API，但调用方必须自行承担操作排序；
删除成功只表示名称绑定已解除，不保证底层数据立即物理删除、空间立即回收或所有
既有平台句柄立即失效。

## 跨进程协调使用边界

调用方可以在第一次协调操作前设置进程级根策略：

```rust
use std::path::PathBuf;

use pi_async_fs::{
    set_cross_process_coordination_root, CrossProcessCoordinationRoot,
};

fn configure_coordination_root(path: PathBuf) -> pi_result::Result<()> {
    set_cross_process_coordination_root(
        CrossProcessCoordinationRoot::Custom(path),
    )
}
```

`Custom` 必须是非空绝对路径。成功设置只表示配置被接受，不证明目录当前存在、
可访问或适合目标环境；这些条件会在实际建立 authority 时报告。首次协调操作后
配置被冻结，相同表示可以幂等重复设置，不同表示返回冲突。

两个建立 authority 的函数是 `unsafe`：

- `create_new_coordinated` 为尚不存在的目标建立协调关系并严格创建文件；
- `establish_cross_process_authority` 为已存在文件建立协调关系。

`unsafe` 的原因是 Rust 类型系统无法验证其它进程、库外文件句柄和部署配置是否
遵守合作合同。建立成功后，通过 `open_with_cross_process_authority` 得到的文件
资源可以调用不带 `unsafe` 的协调式映射方法；这不会让普通 `open`、普通删除或
普通资源自动获得跨进程保证。

`CrossProcessFileAuthority` 不实现 `Clone` 或 `Copy`。旧 authority 不能用于同名
删除后重新创建的新文件，也不能用于另一个 namespace 或另一个目标。协调式删除
必须调用 `remove_file_with_cross_process_authority`；正常业务路径不要调用仅供恢复
未完成删除使用的 `resume_coordinated_file_removal`。

## 本地阻塞准入容量

`set_local_blocking_capacity(NonZeroUsize)` 设置本库公开定义的进程级本地阻塞工作
在途容量。容量同时计算已经获准等待执行和正在执行的相关工作；达到上限时，尚未
提交的调用返回 `ErrorKind::ResourceExhausted`，而不是无限等待。

```rust
use std::num::NonZeroUsize;

use pi_async_fs::set_local_blocking_capacity;

fn configure_capacity() -> pi_result::Result<()> {
    set_local_blocking_capacity(NonZeroUsize::new(1000).unwrap())
}
```

首次显式配置或首次相关操作会冻结该进程级值。相同值可幂等重复设置，不同值返回
`Conflict`，运行期间不能重置。未显式设置时当前版本采用 1000；该默认值不是稳定
常量，需要稳定部署预算的应用应主动设置。这个数值不是线程数、文件句柄数或全部
异步任务数，也不改变其它工作来源的并行度。

## 并发、生命周期和取消

### 同一文件的主要并发规则

这些规则按实际文件对象生效，而不是只比较路径文本；不同路径和硬链接可能仍然
指向同一文件。

| 操作组合 | 公开语义 |
| --- | --- |
| 多个普通只读操作 | 可以并发；顺序读取仍要求各自资源的独占借用 |
| 多个追加调用 | 可以从独立资源发起，但同一文件的完整调用不会让字节互相穿插 |
| 已建立映射与之后的追加 | 可以并存；追加不扩大映射范围 |
| 正在执行的追加与新映射建立 | 不能同时进入 |
| 多个不相交映射 | 可以并存 |
| 任意相交映射 | 拒绝，不区分权限 |
| 活动映射与普通读取、覆盖、截断或安全删除 | 拒绝 |
| 只读/可写映射与普通文件刷新 | 可以并存，无需释放映射；映射写后也允许调用文件刷新 |
| 独立资源上的追加、读取、元信息与普通文件刷新 | 不因刷新自动互斥；其它已有冲突规则仍生效 |
| 覆盖或截断与其它内容操作 | 按其排它语义拒绝冲突调用 |
| 默认改名与已打开文件或映射 | 本库不额外互斥；是否成功仍受平台约束 |
| namespace 元信息查询与内容操作 | 可以并存，但查询结果只是瞬时快照 |

不要通过克隆或复制一个已打开文件资源创建并发通道。需要多个角色或线程独立操作
同一文件时，每个通道都应从同一个逻辑 namespace 独立 `open`，并保留各自资源的
所有权边界。

### Future 和 Stream 生命周期

- 返回 Future 的生命周期与其所借用的 namespace、文件资源、定位符或 buffer
  一致，不要求调用方把所有输入都提升为 `'static`；
- Future 为 `Send` 不等于资源可以被克隆，也不等于同一 Future 可以并发轮询；
- 从未轮询的 Future 没有文件副作用；
- 操作开始后取消不保证回滚，也不会产生可供调用方接收的成功值、失败证据或原始
  写入 buffer；
- 目录 Stream 可以整体在线程之间移动，但只能由一个消费者按固定规则轮询；
- 映射切片的生命周期绝不能超过透明映射句柄。

## 错误、幂等性和持久性

普通错误使用 `pi_result::Result<T>`。需要返还调用方资产或副作用证据的方法使用
`pi_result::RawResult<T, E>`，其中 `E` 是本库的专用失败载体。调用方应同时检查：

1. `error()` 或 `into_parts()` 返回的统一错误；
2. 原始 buffer 或其它拥有型资产；
3. 传输进度；
4. 创建、删除、改名、复制或覆盖的状态证据。

不要解析 `Display` 或 `Debug` 文本建立程序逻辑；应使用错误分类、枚举变体和公开
访问器。可报告的输入、能力、角色、冲突和 I/O 失败均通过返回值表达，不应依赖
panic 处理正常错误。

幂等性边界：

- 查询可以重复调用，但外部状态变化时结果可以不同；
- 相同的全局配置值可以幂等重复设置；
- `create_new`、`create_dir`、`append`、`rename` 和 `copy_new` 不是无条件幂等操作；
- `create_dir_all` 可能在失败前已经创建一部分目录；
- 失败证据为 `Unknown` 时必须重新观察，不能假定原请求可安全重试；
- 刷新可以重复请求，但每次只对调用时已经可见的相应修改建立保证。

写入、覆盖或截断成功只表示相应普通文件操作成功完成，不自动等于强持久化。
普通文件使用 `FileIo::flush(FileFlushMode)`；映射修改使用
`ReadWriteMmapHandle::flush()`。两类刷新都不承诺父目录名称变更已经同步，也不保证
掉电、操作系统崩溃、失信存储控制器、介质损坏或硬件故障后的绝对存续。

映射可以持续保留，追加数据后直接等待 `FileIo::flush`，不需要先解除只读
或可写映射。普通文件刷新只与覆盖、截断等全文件破坏性操作互斥。
不同独立资源的追加与刷新可以并行；若需要确认某次追加已达到所选刷新等级，
应先等待该次追加成功，再等待刷新成功。一次刷新不保证其它资源上仍在进行
或之后才开始的追加也已经完成同步。

`ReadWriteMmap` 文件资源也允许调用 `FileIo::flush`，因此映射写后可以请求
文件刷新；该调用不替代可写映射句柄的脏页刷新保证。要取得映射修改的明确
完成证据，仍需等待 `ReadWriteMmapHandle::flush()`。只读文件资源不因此
增加刷新权限，活动只读映射期间应通过独立追加资源刷新普通写入。

## 注意事项与使用禁忌

- 不要用“先 `try_exists`、再创建/改名”模拟原子不替换；直接使用 `create_new`、
  `rename` 或 `copy_new`。
- 不要把 `try_exists` 的查询结果、元信息、长度、可用空间或目录项类型提示当作
  后续操作的锁或稳定前置条件。
- 不要把 `ReadTargetRegion` 当成文件范围；它只描述目标缓冲区中的初始化区域。
- 不要把 `ReadGrowthLimit` 当成最终 buffer 长度；它限制本次调用的新增量。
- 不要把短读当成错误；需要完整窗口时使用 `read_exact` 或 `read_exact_at`。
- 不要把备用容量当作已经初始化的读取目标；固定读取不会写入未初始化容量。
- 不要从取回原始 buffer 推导写操作没有副作用，也不要忽略 `TransferProgress`。
- 不要把 `AtLeast` 当作精确进度，不要把 `Unknown` 当作零进度。
- 不要忽略覆盖、创建、删除、改名和复制失败中的状态证据。
- 不要用顺序读取位置或“定位到尾部”模拟严格追加。
- 不要把 `truncate` 用于扩容；它只允许缩短。需要新完整内容时使用
  `overwrite_all`。
- 不要让同一文件的映射作为该文件自身的追加、覆盖或普通读取 buffer。
- 不要通过映射句柄以外的引用、裸地址或其它资源访问映射范围。
- 不要依赖映射句柄 `Drop` 或文件资源 `Drop` 自动刷新。
- 不要把 `FileIo::flush` 当作映射刷新，也不要把映射刷新当作普通文件刷新。
- 不要从路径字符串相等或不等推导文件身份；路径别名和硬链接可能指向同一对象。
- 不要把普通资源的同进程协调描述成跨进程安全。
- 不要在无法满足所有外部合作前提时调用任何 `unsafe` 协调或未协调映射入口。
- 不要把 authority 当作可序列化的跨进程令牌、系统权限或永久锁。
- 不要把未协调删除解释为立即物理删除或既有句柄立即失效。
- 不要假定 `read_dir` 或 `walk` 产生稳定顺序或目录快照。
- 不要对一个 Stream 并发轮询，也不要让映射切片比句柄存活更久。
- 不要为 `pi_atom` 使用的同一份 `pi_share` 启用 `rc` feature。
- 不要调用当前尚不可用的远端定位符行为或假定本地适配器接受 URL。

## 构建与验证

建议使用与当前支持边界一致的串行单 crate 命令：

```text
cargo test --locked --all-features -- --test-threads=1
cargo test --release --locked --all-features -- --test-threads=1
cargo clippy --locked --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --all-features --no-deps
```

需要查看每个公开枚举变体、字段、泛型约束和完整签名时，可生成并打开本库
Rustdoc：

```text
cargo doc --locked --all-features --no-deps --open
```

## 许可证

MIT
