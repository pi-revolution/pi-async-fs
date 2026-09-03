// 定位符所在存储作用域的当前候选可用空间快照。
//
// [`AvailableSpace`] 只保存后端在一次查询中能够诚实统一的主数值。它不把
// Windows 调用身份配额、Linux 非特权可用块和远端容量接口误写成完全相同的
// 物理磁盘概念，也不把文件系统总空闲量、总容量或浏览器估计混进同一个值。
//
// 本模块只承载无 I/O 的值语义；定位符解析和平台查询由 namespace 实现负责。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

// 一个存储作用域为当前查询上下文报告的候选可用字节数快照。
//
// # 作用与使用位置
//
// 本类型是后续 [`crate::FileNamespace`] 可用空间查询的成功结果。查询输入是
// namespace 自己的 locator，而容量属于该 locator 所在的更大存储作用域：
// 例如 Windows 卷或共享、Linux 已挂载文件系统、WebDAV 配额根，或专有远端
// 后端明确声明的账户/租户配额。
//
// 定位符像一间房的门牌号，存储作用域像整栋楼的公共容量；结果回答的是
// 后端根据这间房定位到的“整栋楼当前还可分配多少”，并不表示这些字节已经
// 为该定位符预留。类比不覆盖 inode、压缩、文件大小上限或设备错误等其它
// 写入条件。
//
// # 跨平台数值语义
//
// `available_bytes` 的统一含义是“底层存储为实际查询上下文报告的当前候选
// 可用字节数”：
//
// - Windows 使用与查询执行身份相关的 `FreeBytesAvailableToCaller`；用户
//   配额可能使它小于卷总空闲量；
// - Linux 使用 `f_frsize * f_bavail`；它表达文件系统为非特权访问者报告的
//   可用块，但不宣称所有文件系统都完整纳入用户、组和项目配额；
// - 远端只有在后端提供语义明确的容量或配额接口时才能产生本类型。
//
// 标准 S3、普通 HTTP、没有有限容量合同的内存后端，以及只能提供模糊估计
// 而首版合同无法表达估计质量的后端，不得用 `0` 或 `u64::MAX` 伪造成功，
// 必须由查询接口返回明确的能力错误。
//
// 零表示后端确实报告当前没有候选可用字节；`u64::MAX` 也保留为合法平台
// 结果，不能兼作“不受支持”的哨兵。平台块计数换算超出 `u64` 时必须由查询
// 接口报告溢出错误，不能在构造本值前饱和、截断或回绕。
//
// # 快照与不保证事项
//
// 本值可能在返回后立即过期。它不是空间预留、写权限、文件系统可写状态、
// 配额令牌或一次写入成功保证。即使数值大于零，inode 耗尽、只读挂载、
// 并发写入、压缩/稀疏策略、文件大小上限、额外配额、远端策略和设备错误仍
// 可以使写入失败。
//
// 首版刻意不保存 `free_bytes`、`total_bytes`、作用域名称、新鲜时间或估计
// 质量。Windows、Linux 和远端对这些字段没有足够一致的合同；以后若需要，
// 必须单独设计而不能给现有字段追加含义。
//
// # 所有权与基础能力
//
// 本类型是无资源的拥有型值，显式实现低成本 [`Clone`]，但按项目 helper
// 原则不实现 `Copy`。它不实现 `Default`，因为零是有业务含义的真实快照，
// 不是通用默认状态。
//
// 判等、全序和哈希只比较 `available_bytes`，便于确定性诊断、排序和容器
// 索引，不表示两个结果来自同一个存储作用域或同一查询时刻。`Debug` 和
// `Display` 都显示明确的字节单位，但输出不承诺可反向解析或稳定序列化。
//
// 类型不实现 `From<u64>`、`Into<u64>`、`AsRef` 或 `Borrow`，避免调用方在
// 文件长度、传输进度、总容量与可用空间之间进行无语义转换。`Send + Sync`
// 由唯一的 `u64` 字段自动获得，不使用手写 `unsafe impl`。本类型不声明稳定
// ABI、内存布局或序列化格式。
/// 存储作用域为当前查询上下文报告的候选可用空间快照。
///
/// 本地位置对应其卷、分区、挂载点或共享，远端位置对应后端明确声明的容量
/// 作用域。数值与当前调用身份和配额有关，可能在返回后立即过期；它不是空间
/// 预留、写权限、总容量或写入成功保证。零和 `u64::MAX` 都是合法数值，不能
/// 用作“不支持”的哨兵。
pub struct AvailableSpace {
    available_bytes: u64,
}

impl AvailableSpace {
    // 从后端已经核验的候选可用字节数构造快照。
    //
    // 本构造器接受所有 `u64` 值，包括零和 `u64::MAX`，不执行 I/O、配额
    // 查询或平台换算。第三方 adapter 必须在调用前完成字段语义核验和受检
    // 算术；不能借本构造器把不支持、估计或溢出状态伪装成成功。
    #[must_use]
    /// 从已经符合本类型语义的候选可用字节数构造快照。
    pub fn new(available_bytes: u64) -> Self {
        Self { available_bytes }
    }

    // 返回本次快照中的候选可用字节数。
    //
    // 返回 `u64` 不消耗快照，也不刷新查询。该数值继承类型文档中的作用域、
    // 平台差异和即时过期边界，不能脱离这些语义当作空间预留或总容量。
    #[must_use]
    /// 返回快照中的候选可用字节数。
    pub fn available_bytes(&self) -> u64 {
        self.available_bytes
    }
}

impl Clone for AvailableSpace {
    // 复制同一个数值快照，不重新查询后端，也不产生新的空间保证。
    fn clone(&self) -> Self {
        Self::new(self.available_bytes)
    }
}

impl fmt::Debug for AvailableSpace {
    // 显示字段名和明确字节单位，不查询后端。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AvailableSpace")
            .field("available_bytes", &self.available_bytes)
            .finish()
    }
}

impl fmt::Display for AvailableSpace {
    // 显示供人阅读的可用字节数，不作为稳定机器格式。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} bytes available", self.available_bytes)
    }
}

impl PartialEq for AvailableSpace {
    // 只比较候选可用字节数，不比较来源作用域或查询时刻。
    fn eq(&self, other: &Self) -> bool {
        self.available_bytes == other.available_bytes
    }
}

impl Eq for AvailableSpace {}

impl PartialOrd for AvailableSpace {
    // 返回与 [`Ord`] 一致的候选可用字节数全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AvailableSpace {
    // 按候选可用字节数从小到大排序，不表达来源质量或新鲜度。
    fn cmp(&self, other: &Self) -> Ordering {
        self.available_bytes.cmp(&other.available_bytes)
    }
}

impl Hash for AvailableSpace {
    // 按与判等一致的候选可用字节数写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.available_bytes.hash(state);
    }
}
