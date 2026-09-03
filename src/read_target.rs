//! 固定读取目标内存窗口。
//!
//! [`ReadTargetRegion`] 只回答“文件读取结果可以覆盖调用方目标缓冲区已初始化
//! 可写视图中的哪一段”。它不描述文件偏移、文件范围、MMAP 范围、备用
//! 容量、自动增长、尾部追加或多段向量 I/O。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::ops::{Bound, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo, RangeToInclusive};

use pi_result::{ClassifyErrorKind, ErrorKind, RawResult};

/// 创建或解析 [`ReadTargetRegion`] 时发现的范围错误。
///
/// 结构错误可以在没有目标缓冲区时发现；越界错误只有把范围绑定到本次真实
/// 已初始化可写视图长度后才能判断。所有变体都属于无后端副作用的输入错误。
#[derive(Debug, pi_result::thiserror::Error)]
#[non_exhaustive]
pub enum ReadTargetRegionError {
    /// 排除起点需要执行 `start + 1`，但输入为 `usize::MAX`。
    #[error("excluded read target start overflows usize")]
    ExcludedStartOverflow,

    /// 包含终点需要执行 `end + 1`，但输入为 `usize::MAX`。
    #[error("included read target end overflows usize")]
    IncludedEndOverflow,

    /// 端点规范化后，起点位于固定终点之后。
    #[error("read target region is reversed: start {start} exceeds end {end_exclusive}")]
    Reversed {
        /// 规范化后包含的起点。
        start: usize,
        /// 规范化后不包含的终点。
        end_exclusive: usize,
    },

    /// 规范化范围没有完全落入本次目标已初始化可写视图。
    #[error(
        "read target region [{start}, {end_exclusive}) exceeds initialized length {initialized_len}"
    )]
    OutOfBounds {
        /// 规范化后包含的起点。
        start: usize,
        /// 解析后不包含的终点。
        end_exclusive: usize,
        /// 本次目标权威已初始化可写视图的字节长度。
        initialized_len: usize,
    },
}

impl ClassifyErrorKind for ReadTargetRegionError {
    fn classify_error_kind(&self) -> ErrorKind {
        ErrorKind::InvalidInput
    }
}

/// 固定目标读取获准覆盖的一段连续、已初始化可写内存窗口。
///
/// # 坐标系
///
/// 索引只相对于输入缓冲区为本次固定读取公开的可写字节视图，单位始终是
/// 字节。
///
/// # 范围语义
///
/// 所有开闭端点都按等价半开区间 `[start, end)` 解释；无界终点表示延伸至
/// 本次缓冲区已初始化视图的末尾，并在 [`Self::resolve`] 时确定。等价写法
/// （如 `..5` 与 `..=4`）具有相同可观察语义。
///
/// # 安全边界
///
/// 本类型只验证范围值，不拥有或借用目标缓冲区。范围必须先通过
/// [`Self::resolve`] 绑定真实已初始化长度，成功后才能访问相应窗口。固定
/// 读取不得访问缓冲区的未初始化备用容量或改变其逻辑长度。
pub struct ReadTargetRegion {
    start: usize,
    end_exclusive: Option<usize>,
}

impl ReadTargetRegion {
    /// 从任意一对标准开闭/无界端点创建规范化目标窗口。
    ///
    /// 排除起点和包含终点所需的 `+1` 使用受检算术。规范化后反向范围立即
    /// 失败；空范围合法。该方法不接触缓冲区，也不执行 I/O。
    pub fn new(start: Bound<usize>, end: Bound<usize>) -> RawResult<Self, ReadTargetRegionError> {
        let start = match start {
            Bound::Unbounded => 0,
            Bound::Included(start) => start,
            Bound::Excluded(start) => start
                .checked_add(1)
                .ok_or(ReadTargetRegionError::ExcludedStartOverflow)?,
        };
        let end_exclusive = match end {
            Bound::Unbounded => None,
            Bound::Excluded(end_exclusive) => Some(end_exclusive),
            Bound::Included(end) => Some(
                end.checked_add(1)
                    .ok_or(ReadTargetRegionError::IncludedEndOverflow)?,
            ),
        };

        if let Some(end_exclusive) = end_exclusive {
            if start > end_exclusive {
                return Err(ReadTargetRegionError::Reversed {
                    start,
                    end_exclusive,
                });
            }
        }

        Ok(Self {
            start,
            end_exclusive,
        })
    }

    /// 创建覆盖整个已初始化可写视图的目标意图。
    ///
    /// 该值在解析时成为 `[0, initialized_len)`。当真实长度为零时，它解析
    /// 为合法空范围；该显式构造器不意味着实现 [`Default`]。
    #[must_use]
    pub fn full() -> Self {
        Self {
            start: 0,
            end_exclusive: None,
        }
    }

    /// 返回规范化后包含的起点。
    #[must_use]
    pub fn start(&self) -> usize {
        self.start
    }

    /// 返回规范化后的可选固定排他终点。
    ///
    /// `None` 表示“到本次目标已初始化视图末尾”，不是未知值或追加权限。
    #[must_use]
    pub fn end_exclusive(&self) -> Option<usize> {
        self.end_exclusive
    }

    /// 把目标意图绑定到真实已初始化可写视图长度。
    ///
    /// 成功返回唯一半开区间，并证明 `start <= end <= initialized_len`。失败
    /// 必须发生在切片、指针访问、后端 I/O 和顺序游标推进之前。
    ///
    /// 空范围合法；后续固定读取必须把它作为无需调用后端的零字节成功处理。
    pub fn resolve(
        &self,
        initialized_len: usize,
    ) -> RawResult<Range<usize>, ReadTargetRegionError> {
        let end_exclusive = self.end_exclusive.unwrap_or(initialized_len);
        if self.start > end_exclusive || end_exclusive > initialized_len {
            return Err(ReadTargetRegionError::OutOfBounds {
                start: self.start,
                end_exclusive,
                initialized_len,
            });
        }

        Ok(self.start..end_exclusive)
    }
}

impl Clone for ReadTargetRegion {
    fn clone(&self) -> Self {
        Self {
            start: self.start,
            end_exclusive: self.end_exclusive,
        }
    }
}

impl fmt::Debug for ReadTargetRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadTargetRegion")
            .field("start", &self.start)
            .field("end_exclusive", &self.end_exclusive)
            .finish()
    }
}

impl fmt::Display for ReadTargetRegion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.end_exclusive {
            Some(end_exclusive) => {
                write!(formatter, "[{}, {})", self.start, end_exclusive)
            }
            None => write!(formatter, "[{}, end)", self.start),
        }
    }
}

impl PartialEq for ReadTargetRegion {
    fn eq(&self, other: &Self) -> bool {
        self.start == other.start && self.end_exclusive == other.end_exclusive
    }
}

impl Eq for ReadTargetRegion {}

impl PartialOrd for ReadTargetRegion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadTargetRegion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.start.cmp(&other.start).then_with(|| {
            match (self.end_exclusive, other.end_exclusive) {
                (Some(left), Some(right)) => left.cmp(&right),
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            }
        })
    }
}

impl Hash for ReadTargetRegion {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.start.hash(state);
        self.end_exclusive.hash(state);
    }
}

impl TryFrom<Range<usize>> for ReadTargetRegion {
    type Error = ReadTargetRegionError;

    fn try_from(range: Range<usize>) -> Result<Self, Self::Error> {
        Self::new(Bound::Included(range.start), Bound::Excluded(range.end))
    }
}

impl TryFrom<RangeInclusive<usize>> for ReadTargetRegion {
    type Error = ReadTargetRegionError;

    fn try_from(range: RangeInclusive<usize>) -> Result<Self, Self::Error> {
        let (start, end) = range.into_inner();
        Self::new(Bound::Included(start), Bound::Included(end))
    }
}

impl From<RangeFrom<usize>> for ReadTargetRegion {
    fn from(range: RangeFrom<usize>) -> Self {
        Self {
            start: range.start,
            end_exclusive: None,
        }
    }
}

impl From<RangeTo<usize>> for ReadTargetRegion {
    fn from(range: RangeTo<usize>) -> Self {
        Self {
            start: 0,
            end_exclusive: Some(range.end),
        }
    }
}

impl TryFrom<RangeToInclusive<usize>> for ReadTargetRegion {
    type Error = ReadTargetRegionError;

    fn try_from(range: RangeToInclusive<usize>) -> Result<Self, Self::Error> {
        Self::new(Bound::Unbounded, Bound::Included(range.end))
    }
}

impl From<RangeFull> for ReadTargetRegion {
    fn from(_: RangeFull) -> Self {
        Self::full()
    }
}
