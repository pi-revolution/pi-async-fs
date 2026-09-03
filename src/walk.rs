// 递归目录遍历的轻量结果模型。
//
// 本模块只表达已经从递归流中产生的单个后代项目。它不保存遍历
// 器、namespace、后端分页令牌、已打开目录句柄或元信息缓存。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::num::NonZeroUsize;

use crate::DirectoryEntry;

// 一次递归目录遍历允许产生的最大逻辑深度。
//
// # 深度原点和边界
//
// 遍历根本身不会产生 [`WalkEntry`]；根的直接子项深度为一。有限
// 上限是包含式上限：位于该深度的项目仍会产生，但实现不得再进入
// 其子目录。
//
// `Limited { max_depth: 0 }` 是显式零结果请求，但不允许 `walk`
// 忽略根定位符的语法、存在性和目录语义校验。`Unlimited` 只表示本库
// 不设置额外逻辑深度上限，不取消后端、路径、内存、句柄或其它资源
// 限制。
//
// # 实现要求
//
// adapter 必须在进入下一层之前执行上限，不能先读取完整子树再过滤
// 已越界的输出。远端实现限制的也是逻辑层级，不是分页大小或项目总数。
#[non_exhaustive]
/// 一次递归遍历允许产生的最大逻辑深度。
///
/// 遍历根不产生项目，直接子项深度为一。有限上限是包含式上限；达到上限的
/// 项目仍会返回，但其后代不会被遍历。零表示验证根后不产生任何后代。
pub enum WalkDepthLimit {
    // 不设置本库层面的逻辑深度上限。
    /// 不设置库层面的逻辑深度上限。
    Unlimited,

    // 最深产生到 `max_depth` 层，并且不进入更深子目录。
    /// 最深产生到 `max_depth` 层。
    Limited {
        // 包含式最大逻辑深度；零表示不产生任何后代。
        /// 包含式最大逻辑深度；零表示不产生后代。
        max_depth: usize,
    },
}

impl WalkDepthLimit {
    // 返回包含式最大深度，或在本库不设逻辑上限时返回 `None`。
    //
    // `Some(0)` 是合法且与 `None` 完全不同的结果：前者要求验证根
    // 后不产生任何后代，后者表示本库不为该次遍历设置额外深度限制。
    // 本方法不读取遍历器或后端状态，计划为 O(1)、无分配、无锁、无
    // I/O、无外部副作用且不 panic。
    #[must_use]
    /// 返回包含式最大深度；无限时返回 `None`。
    pub fn maximum_depth(&self) -> Option<usize> {
        match self {
            Self::Unlimited => None,
            Self::Limited { max_depth } => Some(*max_depth),
        }
    }
}

impl Clone for WalkDepthLimit {
    // 显式复制变体和标量上限，不复制遍历状态。
    fn clone(&self) -> Self {
        match self {
            Self::Unlimited => Self::Unlimited,
            Self::Limited { max_depth } => Self::Limited {
                max_depth: *max_depth,
            },
        }
    }
}

impl fmt::Debug for WalkDepthLimit {
    // 以变体名称和字段名称显示开发者格式。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unlimited => formatter.write_str("Unlimited"),
            Self::Limited { max_depth } => formatter
                .debug_struct("Limited")
                .field("max_depth", max_depth)
                .finish(),
        }
    }
}

impl fmt::Display for WalkDepthLimit {
    // 显示明确的无限或包含式最大深度摘要。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unlimited => formatter.write_str("unlimited depth"),
            Self::Limited { max_depth } => {
                write!(formatter, "maximum depth: {max_depth}")
            }
        }
    }
}

impl PartialEq for WalkDepthLimit {
    // 按变体和有限上限值判等。
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Unlimited, Self::Unlimited) => true,
            (
                Self::Limited {
                    max_depth: left,
                },
                Self::Limited {
                    max_depth: right,
                },
            ) => left == right,
            _ => false,
        }
    }
}

impl Eq for WalkDepthLimit {}

impl PartialOrd for WalkDepthLimit {
    // 按语义上限比较；无限大于任何有限上限。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WalkDepthLimit {
    // 按语义上限建立全序；有限值按数值排序，无限位于最后。
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::Limited {
                    max_depth: left,
                },
                Self::Limited {
                    max_depth: right,
                },
            ) => left.cmp(right),
            (Self::Limited { .. }, Self::Unlimited) => Ordering::Less,
            (Self::Unlimited, Self::Limited { .. }) => Ordering::Greater,
            (Self::Unlimited, Self::Unlimited) => Ordering::Equal,
        }
    }
}

impl Hash for WalkDepthLimit {
    // 将有限/无限变体和可选数值共同写入哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::Limited { max_depth } => {
                0_u8.hash(state);
                max_depth.hash(state);
            }
            Self::Unlimited => {
                1_u8.hash(state);
            }
        }
    }
}

// 建立一次递归目录遍历时的通用选项。
//
// # 当前选项与固定合同
//
// 首版只让调用方显式选择 [`WalkDepthLimit`]。符号链接始终不跟随；
// 局部项目错误会作为流项产生，遍历状态仍可安全继续时则继续，根级
// 或状态已无法继续的错误产生后终止。调用方可以在任何错误后主动
// 丢弃流来选择“首错即停”。
//
// # 资源边界
//
// `Unlimited` 可能使本地深度优先实现在极深目录树上保留更多目录
// 流状态。本通用选项不暴露“打开目录句柄数”等本地实现细节；实现
// 必须在自己的资源预算内执行，超出时返回结构化错误，不得通过隐藏的
// 无界预取规避。
//
// 本结构标记为 `non_exhaustive`，使未来可以在不改变 `walk` 方法参数
// 形状的前提下增加具有真实跨后端语义的选项。
#[non_exhaustive]
/// 建立递归目录遍历时的通用选项。
///
/// 当前版本只选择深度限制。遍历不跟随符号链接；项目级错误通过流返回，状态
/// 仍可靠时可以继续，根级或不可恢复的遍历错误会结束流。不存在默认选项，
/// 调用方必须明确选择有限或无限深度。
pub struct WalkOptions {
    // 本次遍历允许产生的包含式最大深度。
    /// 本次遍历的逻辑深度限制。
    pub depth_limit: WalkDepthLimit,
}

impl WalkOptions {
    // 使用调用方显式选择的深度限制建立遍历选项。
    //
    // 本构造器只移动纯选项值，不打开根目录、不建立流、不查询后端，
    // 也不把 `Unlimited` 替换为隐藏的实现上限。调用方必须在该入口
    // 显式选择有限或无限语义，因此本类型不提供无参构造器或
    // `Default`。
    //
    // 真实实现计划为 O(1)、无分配、无锁、无 I/O、无外部副作用且
    // 不 panic。
    #[must_use]
    /// 使用显式深度限制构造遍历选项。
    pub fn new(depth_limit: WalkDepthLimit) -> Self {
        Self { depth_limit }
    }
}

impl Clone for WalkOptions {
    // 显式克隆纯选项值，不克隆任何活动遍历流或后端状态。
    fn clone(&self) -> Self {
        Self::new(self.depth_limit.clone())
    }
}

impl fmt::Debug for WalkOptions {
    // 以字段名称和完整深度限制显示开发者格式。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WalkOptions")
            .field("depth_limit", &self.depth_limit)
            .finish()
    }
}

impl fmt::Display for WalkOptions {
    // 显示面向人类的遍历深度选项摘要。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "depth limit: {}", self.depth_limit)
    }
}

impl PartialEq for WalkOptions {
    // 按全部稳定选项字段判等。
    fn eq(&self, other: &Self) -> bool {
        self.depth_limit == other.depth_limit
    }
}

impl Eq for WalkOptions {}

impl PartialOrd for WalkOptions {
    // 按字段顺序进行部分比较。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WalkOptions {
    // 按字段顺序进行全序比较，只用于集合和确定性输出。
    fn cmp(&self, other: &Self) -> Ordering {
        self.depth_limit.cmp(&other.depth_limit)
    }
}

impl Hash for WalkOptions {
    // 将全部稳定选项字段写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.depth_limit.hash(state);
    }
}

// 一次递归目录遍历产生的单个后代项目。
//
// # 使用位置
//
// 本类型是后续 `FileNamespace::walk` 所关联异步流的成功项目。
// [`Self::entry`] 提供项目名称、完整定位符和无额外 I/O 的可选类型
// 提示；需要完整元信息时，调用方必须把定位符显式交回产生它的
// namespace。
//
// # 深度语义
//
// 遍历根本身不会产生 `WalkEntry`。根的直接子项深度为 `1`，下一层
// 为 `2`，以此类推。[`NonZeroUsize`] 从类型上排除了把后代项误标为
// 根层的可能。深度只是逻辑层级，不授权调用方用字符串分隔符重新
// 解析定位符。
//
// # 快照、顺序与竞态
//
// 单个项目是已产生事实的拥有型值，但整个遍历不因此成为原子快照。
// 后续项目可能受并发创建、删除、改名或远端最终一致性影响。本类型
// 也不编码深度优先/广度优先或同层排序保证；这些由 `walk` 的选项和
// 方法合同表达。
#[non_exhaustive]
/// 递归目录遍历产生的一个拥有型后代项目。
///
/// 根本身不会形成本类型；直接子项深度为一。单个项目可以在遍历流释放后
/// 保存，但不代表整个遍历是快照，也不编码遍历顺序。项目包含远端
/// [`DirectoryEntry`] 时，继承其中 [`crate::EntryName`] 的跨线程安全依赖图
/// 前置条件。
pub struct WalkEntry<L> {
    // 当前后代的轻量目录项。
    /// 当前后代的轻量目录项。
    pub entry: DirectoryEntry<L>,

    // 当前项目相对于遍历根的非零逻辑深度。
    /// 当前项目相对于遍历根的非零逻辑深度。
    pub depth: NonZeroUsize,
}

impl<L> WalkEntry<L> {
    // 组合一个递归遍历后代项目。
    //
    // 参数顺序与字段顺序一致：先移入主要目录项，再提供相对于
    // 遍历根的非零深度。[`NonZeroUsize`] 已排除零深度；本构造器
    // 无法访问遍历根或后端，因此定位符确属该深度的生产者不变量由
    // adapter 承担。
    //
    // 真实实现应只移动字段，计划为 O(1)、无分配、无锁、无 I/O、
    // 无外部副作用且不 panic。
    #[must_use]
    /// 按“目录项、深度”的顺序构造遍历项目。
    pub fn new(entry: DirectoryEntry<L>, depth: NonZeroUsize) -> Self {
        Self { entry, depth }
    }

    // 消费本值并取回轻量目录项与非零深度。
    //
    // 返回顺序与 [`Self::new`] 的参数顺序及公开字段顺序一致。本方法
    // 不要求 `L: Clone`，不会复制定位符或丢弃深度。如果未来因
    // `non_exhaustive` 增加私有组成部分，本接口仍只承诺交付这两个
    // 已冻结的公开组成部分。
    //
    // 真实实现计划为 O(1) 字段移动，无分配、无锁、无 I/O、无外部
    // 副作用且不 panic。
    #[must_use]
    /// 消费项目并按构造顺序返回目录项和深度。
    pub fn into_parts(self) -> (DirectoryEntry<L>, NonZeroUsize) {
        (self.entry, self.depth)
    }
}

impl<L> Clone for WalkEntry<L>
where
    L: Clone,
{
    // 显式克隆已拥有的目录项和标量深度。
    //
    // 这不克隆遍历流、已打开目录或 namespace；定位符的具体
    // 克隆成本由 `L` 决定。
    fn clone(&self) -> Self {
        Self::new(self.entry.clone(), self.depth)
    }
}

impl<L> fmt::Debug for WalkEntry<L>
where
    L: fmt::Debug,
{
    // 以包含字段名称的开发者格式显示项目和深度。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WalkEntry")
            .field("entry", &self.entry)
            .field("depth", &self.depth)
            .finish()
    }
}

impl<L> fmt::Display for WalkEntry<L>
where
    L: fmt::Debug,
{
    // 以适合人类日志的单项定位与深度摘要显示遍历结果。
    //
    // 本合同只要求 `L: Debug`，使不直接实现 `Display` 的非 UTF-8
    // 原生路径仍能获得安全、可诊断的显示形式。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?} at depth {}", self.entry, self.depth)
    }
}

impl<L> PartialEq for WalkEntry<L>
where
    L: PartialEq,
{
    // 按完整目录项与深度联合判等。
    fn eq(&self, other: &Self) -> bool {
        self.entry == other.entry && self.depth == other.depth
    }
}

impl<L> Eq for WalkEntry<L> where L: Eq {}

impl<L> PartialOrd for WalkEntry<L>
where
    L: PartialOrd,
{
    // 按目录项后深度的字段顺序进行部分比较。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.entry.partial_cmp(&other.entry)? {
            Ordering::Equal => self.depth.partial_cmp(&other.depth),
            ordering => Some(ordering),
        }
    }
}

impl<L> Ord for WalkEntry<L>
where
    L: Ord,
{
    // 按目录项后深度的字段顺序进行全序比较。
    //
    // 该顺序只服务集合与确定性输出，不表示遍历产生顺序。
    fn cmp(&self, other: &Self) -> Ordering {
        self.entry
            .cmp(&other.entry)
            .then_with(|| self.depth.cmp(&other.depth))
    }
}

impl<L> Hash for WalkEntry<L>
where
    L: Hash,
{
    // 将完整目录项和深度共同写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.entry.hash(state);
        self.depth.hash(state);
    }
}
