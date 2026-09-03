// 文件内存映射的公开逻辑范围。
//
// [`MmapRange`] 只描述调用方希望映射的文件字节区间。它使用文件开头作为
// 坐标原点，单位为字节，并统一采用半开区间 `[start, end)`。操作系统要求
// 的页或分配粒度对齐属于后端实现细节：adapter 可以向前扩大实际映射，
// 但必须把公开句柄可访问的内容严格裁剪回本类型描述的逻辑范围。
//
// 本模块不描述缓冲区窗口、尾部追加范围或映射建立后的文件长度变化。文件
// 快照边界、平台可表达性、稳定身份协调和范围冲突必须由建立映射的 API 在
// 接触底层映射设施之前继续验证。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::ops::Range;

use pi_result::{ClassifyErrorKind, ErrorKind, RawResult};

// 创建文件内存映射逻辑范围时发现的结构错误。
//
// 这些错误只说明两个端点无法组成一个非空半开区间。它们不会查询文件、
// 获取进程内协调租约或调用操作系统，因此失败没有外部 I/O 副作用。
#[derive(Debug, pi_result::thiserror::Error)]
#[non_exhaustive]
/// 构造内存映射逻辑范围时发现的端点错误。
pub enum MmapRangeError {
    // 起点与排他终点相同，范围不包含任何字节。
    #[error("mmap range is empty at byte offset {at}")]
    /// 起点与排他终点相同，范围不包含任何字节。
    Empty {
        // 相等的起点和排他终点。
        /// 相等的起点和排他终点。
        at: u64,
    },

    // 起点位于排他终点之后。
    #[error("mmap range is reversed: start {start} exceeds end {end_exclusive}")]
    /// 起点位于排他终点之后。
    Reversed {
        // 包含的文件字节起点。
        /// 包含的文件字节起点。
        start: u64,
        // 不包含的文件字节终点。
        /// 不包含的文件字节终点。
        end_exclusive: u64,
    },
}

impl ClassifyErrorKind for MmapRangeError {
    fn classify_error_kind(&self) -> ErrorKind {
        ErrorKind::InvalidInput
    }
}

// 文件内存映射请求所覆盖的非空逻辑字节范围。
//
// # 作用与使用位置
//
// 本类型会出现在建立映射的公开 API 入参、进程内稳定文件身份协调器的范围
// 租约，以及透明映射句柄报告自身逻辑范围的接口中。它不是实际虚拟地址
// 范围，也不暴露操作系统为了对齐而额外映射的前缀或后缀。
//
// # 不变量
//
// 每个可构造值都满足 `start < end_exclusive`。因此长度始终非零，且
// `end_exclusive - start` 不会下溢。构造本类型并不证明范围位于某次文件
// 长度快照内；建立映射时仍必须验证 `end_exclusive <= snapshot_len`，并
// 完成平台长度转换、页对齐和并发冲突检查。
//
// # 所有权与基本能力
//
// 本类型刻意不实现 `Clone`、`Copy` 或 `Default`。需要长期共享同一范围的
// 内部组件应让范围由映射租约统一拥有，而不是复制出可能脱离租约的公开
// 值。它实现判等、全序和哈希，以便诊断、索引和确定性排序；`Send` 与
// `Sync` 由两个 `u64` 字段自动获得。
/// 文件内存映射请求覆盖的非空半开字节范围 `[start, end)`。
///
/// 坐标原点是文件开头，单位为字节。每个值都满足 `start < end`。构造成功
/// 只证明端点结构有效；建立映射时仍会检查范围是否位于当时文件长度内、
/// 当前平台是否可表达以及是否与其它操作冲突。
pub struct MmapRange {
    start: u64,
    end_exclusive: u64,
}

impl MmapRange {
    // 从文件逻辑字节端点创建非空半开范围 `[start, end_exclusive)`。
    //
    // 空范围和反向范围返回 [`MmapRangeError`]。本方法是纯结构验证：不会
    // 查询文件长度、分配内存、接触全局协调状态或执行 I/O，也不会 panic。
    /// 从端点构造非空半开范围 `[start, end_exclusive)`。
    pub fn new(start: u64, end_exclusive: u64) -> RawResult<Self, MmapRangeError> {
        match start.cmp(&end_exclusive) {
            Ordering::Less => Ok(Self {
                start,
                end_exclusive,
            }),
            Ordering::Equal => Err(MmapRangeError::Empty { at: start }),
            Ordering::Greater => Err(MmapRangeError::Reversed {
                start,
                end_exclusive,
            }),
        }
    }

    // 返回范围中包含的第一个文件字节偏移。
    #[must_use]
    /// 返回范围包含的第一个文件字节偏移。
    pub fn start(&self) -> u64 {
        self.start
    }

    // 返回范围中不包含的文件字节终点。
    #[must_use]
    /// 返回范围不包含的文件字节终点。
    pub fn end_exclusive(&self) -> u64 {
        self.end_exclusive
    }

    // 返回范围包含的字节数。
    //
    // 由类型不变量可知结果始终大于零。返回 `u64` 是为了保持文件坐标系，
    // 不表示当前进程必然可以把该长度转换为 `usize` 或一次完成映射。
    #[must_use]
    /// 返回范围包含的非零字节数。
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.end_exclusive - self.start
    }
}

impl fmt::Debug for MmapRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MmapRange")
            .field("start", &self.start)
            .field("end_exclusive", &self.end_exclusive)
            .finish()
    }
}

impl fmt::Display for MmapRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "[{}, {})", self.start, self.end_exclusive)
    }
}

impl PartialEq for MmapRange {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end_exclusive == other.end_exclusive
    }
}

impl Eq for MmapRange {}

impl PartialOrd for MmapRange {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MmapRange {
    fn cmp(&self, other: &Self) -> Ordering {
        self.start
            .cmp(&other.start)
            .then_with(|| self.end_exclusive.cmp(&other.end_exclusive))
    }
}

impl Hash for MmapRange {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.start.hash(state);
        self.end_exclusive.hash(state);
    }
}

impl TryFrom<Range<u64>> for MmapRange {
    type Error = MmapRangeError;

    fn try_from(range: Range<u64>) -> Result<Self, Self::Error> {
        Self::new(range.start, range.end)
    }
}
