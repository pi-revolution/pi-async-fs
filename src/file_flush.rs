// 普通文件强刷新所请求的最低完成等级。
//
// [`FileFlushMode`] 只选择一次 [`crate::FileIo::flush`] 至少必须证明哪些
// 状态已经完成刷新。它不执行 I/O，不持有文件资源，也不描述内存映射脏页、
// 父目录项或跨文件事务。可写映射必须继续使用
// [`crate::ReadWriteMmapHandle::flush`]。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

// 普通文件强刷新所要求的最低保证等级。
//
// # 作用与使用位置
//
// 本类型是 [`crate::FileIo::flush`] 的唯一显式参数。调用方用它区分“至少
// 刷新文件内容”与“同时刷新文件自身元信息”两种完成屏障，而不必依赖含糊
// 的布尔值或具体后端方法名。
//
// 它表达的是**最低保证**，不是“系统只能刷新这些状态”。后端可以用更强的
// 原语满足较弱请求：例如某个平台没有独立的数据刷新时，可以用完整文件
// 刷新实现 [`Self::Data`]。反向降级不成立；只能证明数据刷新的后端不能把
// [`Self::DataAndMetadata`] 报告为成功。
//
// # 保证强度与边界
//
// 两个变体形成稳定的强度全序：
//
// ```text
// Data < DataAndMetadata
// ```
//
// [`Self::Data`] 仍包含恢复文件内容所必需的状态。例如追加数据若离不开更新
// 后的文件长度便无法恢复，则相应平台原语必须处理该必要状态；它只是不承诺
// 权限、时间戳等全部文件元信息。后端允许连带刷新这些额外元信息。
//
// [`Self::DataAndMetadata`] 覆盖文件内容和文件自身元信息，但不包括父目录
// 目录项、创建/删除/改名协议、多个文件组成的事务或映射脏页。上述边界不因
// 排序、显示或哈希本值而改变。
//
// # 所有权、演进与线程能力
//
// 本类型刻意不实现 `Clone`、`Copy` 或 `Default`。一次刷新调用按值取得明确
// 模式；不存在能够替调用方安全猜测的默认持久化等级。需要再次刷新时可以
// 重新写出相应无字段变体，不需要复制任何资源。
//
// 枚举保持封闭，不使用 `#[non_exhaustive]`。父目录同步不属于已打开
// `FileIo` 的职责，未来也不能通过向本类型追加“目录”变体悄悄扩大资源
// 边界。本类型不声明稳定 ABI、整数判别值、序列化格式或反向解析协议。
//
// `Send` 与 `Sync` 由无字段枚举自动获得，不使用手写 `unsafe impl`。类型
// 不实现任何内部可变性，不分配、不加锁、不访问全局状态，也不执行 I/O。
/// [`crate::FileIo::flush`] 要求的最低完成等级。
///
/// 更强等级可以满足较弱请求，反向降级不成立。该值只约束普通文件内容路径，
/// 不包括映射脏页、父目录项、命名空间操作或跨文件事务。枚举顺序同时表示
/// `Data < DataAndMetadata` 的保证强度；不存在默认等级。
pub enum FileFlushMode {
    // 至少同步文件内容以及恢复这些内容所必需的状态。
    //
    // 本地 `async-fs` adapter 使用 `async_fs::File::sync_data()` 或更强的
    // 等价原语。成功不保证权限、时间戳等全部文件自身元信息都已刷新；平台
    // 或后端可以连带刷新它们。
    /// 至少刷新文件内容及恢复这些内容所必需的状态。
    ///
    /// 不保证权限、时间戳等全部文件自身元信息都已刷新。
    Data,

    // 同步文件内容以及文件自身元信息。
    //
    // 本地 `async-fs` adapter 使用 `async_fs::File::sync_all()` 的完成等级。
    // 父目录项、命名空间操作和跨文件事务仍不属于该保证。
    /// 刷新文件内容及文件自身元信息。
    ///
    /// 父目录项、命名空间变更和映射脏页仍不属于该保证。
    DataAndMetadata,
}

impl fmt::Debug for FileFlushMode {
    // 以 Rust 变体名显示所选刷新等级，不读取文件或后端状态。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明文件刷新模式 Debug 接口")
    }
}

impl fmt::Display for FileFlushMode {
    // 输出稳定的人类可读标签 `data` 或 `data-and-metadata`。
    //
    // 该文本服务于日志和诊断，不是序列化格式，也不承诺可以用 `FromStr`
    // 解析回来。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明文件刷新模式 Display 接口")
    }
}

impl PartialEq for FileFlushMode {
    // 仅在两个值请求相同最低保证等级时判等。
    fn eq(&self, other: &Self) -> bool {
        todo!("第一批次仅声明文件刷新模式 PartialEq 接口")
    }
}

impl Eq for FileFlushMode {}

impl PartialOrd for FileFlushMode {
    // 返回与 [`Ord`] 一致的完整保证强度顺序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("第一批次仅声明文件刷新模式 PartialOrd 接口")
    }
}

impl Ord for FileFlushMode {
    // 按 `Data < DataAndMetadata` 比较最低保证强度。
    fn cmp(&self, other: &Self) -> Ordering {
        todo!("第一批次仅声明文件刷新模式 Ord 接口")
    }
}

impl Hash for FileFlushMode {
    // 按与判等一致的保证等级向调用方哈希器写入状态。
    //
    // 哈希值只适合当前进程容器，不能充当稳定存储编码或跨版本协议。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        todo!("第一批次仅声明文件刷新模式 Hash 接口")
    }
}
