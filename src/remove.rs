// 单个 namespace 目标移除失败时的提交证据。
//
// 本模块区分诊断原因与“本次操作是否跨过移除提交点”这两个独立维度。
// 证据只描述一次操作的历史副作用，不把易受并发和远端完成不确定性影响的
// 失败结果误报成 locator 当前存在或不存在的实时断言。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

use pi_result::Error;

// 单目标移除操作失败时，对 namespace 绑定提交状态的可证明事实。
//
// # 作用与使用位置
//
// 本类型用于严格单文件删除、严格空目录删除以及协调式单文件删除。它只回答
// “本次操作是否移除过目标 namespace 绑定”，不表示底层存储字节已经物理
// 销毁，也不替代对 locator 当前状态的新查询。
//
// 名称采用 `Remove` 而不是 `Delete`，与 Rust 的 `remove_file`、`remove_dir`
// 术语保持一致，并强调公共提交对象首先是父 namespace 中名称到目标的绑定。
// Unix 文件可能仍有其它硬链接或已打开句柄；远端 backend 也可能受版本保留、
// 回收站、延迟回收或合规策略影响。上述事实不会把成功移除的名称绑定改写为
// “没有移除”，但必须阻止调用方把本证据理解为数据不可恢复销毁证明。
//
// # 适用边界
//
// 递归删除可能移除多个文件和目录项，不能用一个三态值完整表达部分进度；
// 本 crate 因此不提供 `remove_dir_all` 或其它递归目录树删除 API。改名、替换、
// 复制、截断与覆盖具有不同提交点和恢复资产，同样必须使用各自单独冻结的
// 结果合同。
//
// 本库管理的活动 MMAP、读取、追加、覆盖、截断或其它冲突操作必须在单目标
// 删除触及底层移除之前由稳定身份协调器处理。本类型只报告移除提交结果，
// 不能补救绕过本库的原生句柄、不合作进程或未经协调的外部映射。
//
// # 并发、重试与实时状态
//
// 任一变体都不是 locator 的实时存在性断言。`NotRemovedByOperation` 不排除
// 另一个参与者已经删除目标；`RemovedByOperation` 不排除随后又在相同位置
// 创建新对象；`Unknown` 则要求调用方按可能已经移除处理。恢复前必须重新
// 查询并在需要时核验稳定身份。
//
// 本类型刻意不提供 `was_removed` 或 `is_retry_safe`。布尔值无法保留未知
// 结果，重试安全性还取决于错误类别、目标身份、调用方期望状态及并发协议。
//
// # 基本能力与演进
//
// 本类型不实现 `Clone`、`Copy` 或 `Default`。它实现调试、显示、判等、全序
// 和哈希，以支持诊断、确定性集合与问题归档；排序只比较证据标签，不表示
// 可靠程度、危险程度、时间顺序或重试优先级。`Send` 与 `Sync` 由无字段枚举
// 自动获得，不使用手写 `unsafe impl`。
//
// 本类型不承诺稳定 ABI、整数判别值、序列化格式或可反向解析文本。
// `#[non_exhaustive]` 要求库外调用方保留保守分支，以便未来增加更精确且不
// 削弱现有语义的证据。
#[non_exhaustive]
/// 删除操作失败时，对目标名称是否由本次操作移除的可证明事实。
///
/// 证据只描述本次调用的提交历史，不是目标当前位置或物理存储的实时状态，
/// 也不单独表示可以安全重试。
pub enum RemoveTargetEvidence {
    // 能证明本次操作没有跨过目标移除提交点。
    //
    // 常见情况包括参数或权限检查失败、稳定身份或操作准入冲突，以及底层在
    // 提交前明确报告目标不存在。本变体不证明目标当前存在；其它参与者可能
    // 已经删除、改名或替换该位置。
    /// 能证明本次操作没有提交目标移除。
    ///
    /// 这不证明目标当前仍存在；其它参与者可能已经改变该位置。
    NotRemovedByOperation,

    // 能证明本次操作已经移除目标绑定或被 backend 不可撤销地接受。
    //
    // 本变体不证明底层对象已经销毁、空间已经回收、其它硬链接已经消失或
    // 远端保留版本已经清除，也不证明读取证据时相同 locator 仍为空。
    /// 能证明本次操作已经提交目标名称移除。
    ///
    /// 这不证明对象立即物理销毁、空间已经回收或其它名称已经消失。
    RemovedByOperation,

    // 无法证明本次操作是否跨过目标移除提交点。
    //
    // 常见来源包括远端提交响应丢失、不可确认的完成竞态，或无法提供可靠
    // 提交证据的第三方 adapter。调用方必须按目标可能已经移除处理。
    /// 无法证明本次操作是否提交了目标移除。
    Unknown,
}

impl fmt::Debug for RemoveTargetEvidence {
    // 以 Rust 变体名称显示证据，不查询 locator 或底层对象。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRemovedByOperation => "NotRemovedByOperation",
            Self::RemovedByOperation => "RemovedByOperation",
            Self::Unknown => "Unknown",
        })
    }
}

impl fmt::Display for RemoveTargetEvidence {
    // 输出面向日志的人类可读证据摘要，不承诺可反向解析。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRemovedByOperation => "target not removed by operation",
            Self::RemovedByOperation => "target removed by operation",
            Self::Unknown => "target removal outcome unknown",
        })
    }
}

impl PartialEq for RemoveTargetEvidence {
    // 仅在两个值表示相同证据标签时判等。
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                Self::NotRemovedByOperation,
                Self::NotRemovedByOperation
            ) | (
                Self::RemovedByOperation,
                Self::RemovedByOperation
            ) | (Self::Unknown, Self::Unknown)
        )
    }
}

impl Eq for RemoveTargetEvidence {}

impl PartialOrd for RemoveTargetEvidence {
    // 返回与 [`Ord`] 一致的证据标签全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RemoveTargetEvidence {
    // 按变体声明顺序比较，不表达证据强度或恢复优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        fn rank(evidence: &RemoveTargetEvidence) -> u8 {
            match evidence {
                RemoveTargetEvidence::NotRemovedByOperation => 0,
                RemoveTargetEvidence::RemovedByOperation => 1,
                RemoveTargetEvidence::Unknown => 2,
            }
        }

        rank(self).cmp(&rank(other))
    }
}

impl Hash for RemoveTargetEvidence {
    // 按与判等一致的证据标签向调用方哈希器写入状态。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        let discriminant = match self {
            Self::NotRemovedByOperation => 0_u8,
            Self::RemovedByOperation => 1_u8,
            Self::Unknown => 2_u8,
        };
        discriminant.hash(state);
    }
}

// 单个 namespace 目标移除普通失败时的诊断与提交证据载体。
//
// # 作用与使用位置
//
// 本类型把统一的 [`pi_result::Error`] 诊断报告和本次操作的
// [`RemoveTargetEvidence`] 绑定在同一个错误分支中。以后单独冻结的严格单
// 文件删除、严格空目录删除及协调式单文件删除方法使用本类型作为
// `pi_result::RawResult` 的错误值；成功时直接返回 `()`，不附带本载体。
//
// 目标证据不是可选日志附件。调用方必须同时处理诊断原因与移除提交状态，
// 决定是否重新查询、恢复上层索引、拒绝盲目重试或报告不确定结果。把两个
// 维度放入一个命名载体，也防止 adapter 只返回底层错误而遗漏已经发生的
// namespace 副作用。
//
// 本类型不用于递归删除、改名、替换、复制、覆盖或截断。本 crate 不提供
// 递归目录树删除 API；其它操作则具有不同的提交点和恢复资产，不能为了复用
// 相同字段形状而牺牲精确语义。
//
// # 与错误体系的关系
//
// 本类型不是新的叶子错误分类，也不实现 `std::error::Error`。`error` 字段
// 已经是完整的 `pi_result` report；本地和远端叶子错误仍须使用 `thiserror`
// 定义、分类并在公共接口处归一化。本载体只负责保证普通错误分支整体携带
// report 与恢复证据，避免再包装出一条丢失 frame 或 attachment 的错误链。
//
// # 所有权、隐私与基本能力
//
// 两个字段对库外私有。类型不保存 locator、文件内容、原生句柄、认证数据、
// 稳定文件身份或协调文件路径；调用方必须使用自己保存的位置重新查询，且
// 不能把 locator 误认为稳定对象身份。
//
// 本类型不实现 `Clone`、`Copy`、`Default`、判等、排序或哈希。诊断 report
// 是一次性资产，不具有适合稳定值集合的比较语义。`Send` 与 `Sync` 只通过
// 字段条件性自动获得，不使用手写 `unsafe impl`。
//
// `Debug` 与 `Display` 组合已有 report 和证据的脱敏摘要，不额外发现或打印
// 目标信息。本类型不实现 `Deref`、`AsRef<Error>` 或
// `AsRef<RemoveTargetEvidence>`；显式访问器会持续提醒调用方失败包含两个
// 必须共同处理的维度。
/// 删除失败时的统一诊断和目标提交证据。
///
/// 本类型不保存定位符或文件资源，也不会在释放时重试、恢复或继续删除。
pub struct RemoveFailure {
    error: Error,
    target_evidence: RemoveTargetEvidence,
}

impl RemoveFailure {
    // 组合单目标移除失败的统一诊断与提交证据。
    //
    // # 参数顺序与所有权
    //
    // 参数顺序固定为“诊断报告、目标移除证据”，与以后单独冻结的整体拆解
    // 返回顺序一致。两个参数都按所有权移入载体，不克隆 report 或证据，也
    // 不接受可能在公共接口边界隐式丢失 `pi_result` frame、attachment 或错误
    // 分类的 `impl Into<Error>`。
    //
    // 本构造器保持公开，使 crate 外部的 [`crate::FileNamespace`] adapter
    // 也能生产统一失败值。adapter 必须在调用前根据真实 backend 移除提交点
    // 选择证据，不能仅根据最终错误名称猜测目标是否已经移除。
    //
    // # 验证与安全边界
    //
    // 本方法不接收 locator、目标句柄、稳定身份或 backend 提交令牌，因而不
    // 查询 namespace，也无法自行验证证据。虚假证据属于严重 adapter 合同
    // 错误，可能误导恢复与重试；构造本值本身不依据证据执行裸内存访问或
    // 建立后续内存安全证明，因此构造器保持安全函数而不是 `unsafe fn`。
    //
    // # 副作用、panic 与性能
    //
    // 本方法只移动两个字段，计划成本为 `O(1)`；不分配、不执行本地或远端
    // I/O、不查询或修改目标、不加锁、不记录日志。满足生产者合同的调用不应
    // panic。模块不提供无语义的元组 `From`，调用方必须显式看见两个参数。
    #[must_use]
    /// 按“错误、目标证据”的顺序构造失败值。
    pub fn new(error: Error, target_evidence: RemoveTargetEvidence) -> Self {
        Self {
            error,
            target_evidence,
        }
    }

    // 共享借用单目标移除失败的统一诊断报告。
    //
    // # 返回值与所有权
    //
    // 返回引用的生命周期严格绑定到 `self` 的本次共享借用。本方法不消费
    // [`RemoveFailure`]，不克隆、移动或重新包装 report；调用方可以在借用
    // 结束后继续访问目标移除证据，或使用以后单独冻结的整体拆解接口同时
    // 取得两个字段的所有权。
    //
    // 本接口不提供可变引用。诊断与 [`RemoveTargetEvidence`] 描述同一次失败，
    // 不能让调用方单独替换报告并留下失配的提交证据。模块也不提供只消费
    // report 的 `into_error`，避免错误传播代码静默丢弃必要的恢复维度。
    //
    // # 接口选择
    //
    // 本类型不实现 `Deref<Target = Error>` 或 `AsRef<Error>`。显式方法提醒
    // 调用方：report 只是结构化移除失败的一部分，不能把整个载体透明伪装成
    // 普通错误。
    //
    // # 副作用、幂等性与性能
    //
    // 本方法计划为 `O(1)` 共享借用，无分配、无锁、无 I/O，不查询或修改
    // namespace。借用规则允许时重复调用返回同一 report 的共享引用，合法
    // 调用不应 panic。
    #[must_use]
    /// 返回失败诊断的共享引用。
    pub fn error(&self) -> &Error {
        &self.error
    }

    // 共享借用本次单目标操作记录的移除提交证据。
    //
    // # 返回值与语义
    //
    // 返回引用的生命周期严格绑定到 `self` 的本次共享借用。本方法不克隆、
    // 移动或重新推断证据，也不查询 locator、目标元信息、底层文件状态或
    // 远端对象。返回值只回答本次操作记录的移除提交事实，不能证明读取引用
    // 时目标当前存在或不存在。
    //
    // 调用方必须把本结果与 [`Self::error`] 以及外部协调状态共同解释。
    // `NotRemovedByOperation` 不排除并发删除，`RemovedByOperation` 不排除
    // 随后重建，`Unknown` 则要求按可能已经移除处理。
    //
    // # 接口选择
    //
    // 本接口不提供可变引用，防止证据与诊断报告失配；也不提供丢失
    // [`RemoveTargetEvidence::Unknown`] 的 `was_removed` 布尔便利方法。类型
    // 不实现 `AsRef<RemoveTargetEvidence>`，避免隐式转换使调用方忽略诊断。
    // 需要字段所有权时必须使用以后单独冻结的整体拆解接口，同时取得 report。
    //
    // # 副作用、幂等性与性能
    //
    // 本方法计划为 `O(1)` 共享借用，无分配、无锁、无 I/O，不访问或修改
    // namespace。借用规则允许时重复调用返回同一证据引用，合法调用不应
    // panic。
    #[must_use]
    /// 返回目标移除证据的共享引用。
    pub fn target_evidence(&self) -> &RemoveTargetEvidence {
        &self.target_evidence
    }

    // 消费失败载体并一次性取回诊断报告与移除提交证据。
    //
    // # 返回顺序与所有权
    //
    // 返回顺序固定为“诊断报告、目标移除证据”，即
    // `(Error, RemoveTargetEvidence)`；它与
    // [`Self::new(error, target_evidence)`](Self::new) 的参数顺序完全对称。
    // 本方法消费 `self` 并按值移出两个字段，不克隆 report 或证据。
    //
    // 拆解不会重新分类或包装错误，不查询目标当前状态，也不会把历史证据
    // 替换成一次新查询结果。调用方取得所有权后仍须共同解释两个字段。
    //
    // # 接口选择
    //
    // 模块不提供只消费其中一个字段的接口，避免调用方静默丢弃另一必要维度；
    // 也不实现与元组之间的 `From`/`Into`，使构造和拆解动作的语义名称、参数
    // 顺序与证据责任在调用点保持可见。
    //
    // # 副作用、幂等性与性能
    //
    // 本方法只移动字段，计划成本为 `O(1)`；无新增分配、无锁、无 I/O，不
    // 访问或修改 namespace，也不记录日志。合法调用不应 panic。因为方法
    // 消费一次性所有权，同一个失败值只能拆解一次，不具有幂等性。
    #[must_use]
    /// 消费失败值并按构造顺序返回全部字段。
    pub fn into_parts(self) -> (Error, RemoveTargetEvidence) {
        (self.error, self.target_evidence)
    }
}

impl fmt::Debug for RemoveFailure {
    // 显示统一诊断与移除证据，不查询或修改 namespace 目标。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RemoveFailure")
            .field("error", &self.error)
            .field("target_evidence", &self.target_evidence)
            .finish()
    }
}

impl fmt::Display for RemoveFailure {
    // 输出面向日志的脱敏失败摘要，不承诺稳定文本协议。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; target: {}",
            self.error, self.target_evidence
        )
    }
}
