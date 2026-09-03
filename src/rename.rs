// 严格不替换改名失败时的原子提交证据。
//
// 本模块只描述一次 `source -> destination` 原子 namespace 改名是否跨过提交
// 点。它不把改名拆成“删除源”和“创建目标”两次可独立成功的操作，也不提供
// 覆盖目标、跨 backend 搬运或复制后删除语义。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

use pi_result::Error;

// 一次严格不替换改名操作的可证明提交状态。
//
// # 作用与使用位置
//
// 本类型只用于 [`crate::FileNamespace::rename`] 的普通失败分支。受支持的
// backend 必须把 source 名称解除和 destination 名称建立作为一个不可分割
// 的 namespace 提交；证据因此描述整个改名，而不是允许两个绑定分别报告。
//
// 它不表示 locator 当前状态，也不证明底层文件内容、目录树或远端对象版本
// 没有随后变化。调用方在未知结果后必须重新查询两个位置，并在需要时核验
// 元信息或稳定身份，不能把本枚举当成实时快照。
//
// # 重试与并发边界
//
// `NotRenamedByOperation` 只证明本次操作没有提交，不排除另一参与者同时完成
// 了相同或不同改名。`RenamedByOperation` 只证明本次提交发生过，不排除目标
// 随后再次改名、删除或被其它合法操作改变。`Unknown` 要求调用方按改名可能
// 已经完成处理，不能盲目重试并假定 source 仍代表原对象。
//
// 本类型刻意不提供 `was_renamed` 或 `is_retry_safe` 布尔便利方法。未知状态、
// 当前身份和严格 destination 不存在前置条件共同决定恢复策略，单个布尔值
// 会丢失必要信息。
//
// # 基本能力
//
// 本类型不实现 `Clone`、`Copy` 或 `Default`。它实现调试、显示、判等、全序
// 和哈希，以支持诊断与确定性集合；排序只比较证据标签，不表示可靠程度、
// 时间先后或恢复优先级。`Send` 与 `Sync` 由无字段枚举自动获得，不使用手写
// `unsafe impl`。
//
// 本类型不承诺稳定 ABI、整数判别值、序列化格式或可反向解析文本。
// `#[non_exhaustive]` 允许未来增加更精确且不削弱现有语义的证据。
#[non_exhaustive]
/// 严格改名失败时，对原子提交是否由本次操作发生的可证明事实。
///
/// 证据只描述本次调用历史，不是源或目标当前位置的实时快照，也不单独表示
/// 可以安全重试或反向改名。
pub enum RenameCommitEvidence {
    // 能证明本次操作没有跨过原子改名提交点。
    //
    // 常见情况包括参数、对象类型、backend 或同域检查失败，destination 已经
    // 存在，进程内资源冲突，权限不足，以及底层原子原语明确在提交前失败。
    // 本变体不证明 source 当前存在或 destination 当前不存在。
    /// 能证明本次操作没有提交改名。
    NotRenamedByOperation,

    // 能证明本次操作已经跨过原子改名提交点。
    //
    // source 绑定与 destination 绑定的原子切换已经发生，或 backend 已经
    // 不可撤销地接受等价事务。本变体不证明读取证据时 destination 仍指向
    // 同一对象，也不证明目录项已经抗掉电持久化。
    /// 能证明本次操作已经原子提交改名。
    ///
    /// 这不证明目标随后未被其它参与者再次改变，也不提供持久化保证。
    RenamedByOperation,

    // 无法证明本次操作是否跨过原子改名提交点。
    //
    // 典型来源是远端提交响应丢失或 adapter 无法从 backend 取得可靠完成
    // 证据。实现不能用本变体掩盖本可执行的本地系统调用结果核验。
    /// 无法证明本次操作是否提交改名。
    Unknown,
}

impl fmt::Debug for RenameCommitEvidence {
    // 以 Rust 变体名称显示证据，不查询 source 或 destination。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRenamedByOperation => "NotRenamedByOperation",
            Self::RenamedByOperation => "RenamedByOperation",
            Self::Unknown => "Unknown",
        })
    }
}

impl fmt::Display for RenameCommitEvidence {
    // 输出面向日志的人类可读摘要，不承诺可反向解析。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotRenamedByOperation => "rename not committed by operation",
            Self::RenamedByOperation => "rename committed by operation",
            Self::Unknown => "rename commit outcome unknown",
        })
    }
}

impl PartialEq for RenameCommitEvidence {
    // 仅在两个值表示相同提交证据标签时判等。
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                Self::NotRenamedByOperation,
                Self::NotRenamedByOperation
            ) | (
                Self::RenamedByOperation,
                Self::RenamedByOperation
            ) | (Self::Unknown, Self::Unknown)
        )
    }
}

impl Eq for RenameCommitEvidence {}

impl PartialOrd for RenameCommitEvidence {
    // 返回与 [`Ord`] 一致的证据标签全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RenameCommitEvidence {
    // 按变体声明顺序比较，不表达证据强度或恢复优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        fn rank(evidence: &RenameCommitEvidence) -> u8 {
            match evidence {
                RenameCommitEvidence::NotRenamedByOperation => 0,
                RenameCommitEvidence::RenamedByOperation => 1,
                RenameCommitEvidence::Unknown => 2,
            }
        }

        rank(self).cmp(&rank(other))
    }
}

impl Hash for RenameCommitEvidence {
    // 按与判等一致的证据标签向调用方哈希器写入状态。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        let discriminant = match self {
            Self::NotRenamedByOperation => 0_u8,
            Self::RenamedByOperation => 1_u8,
            Self::Unknown => 2_u8,
        };
        discriminant.hash(state);
    }
}

// 严格不替换改名失败时的统一诊断与提交证据载体。
//
// # 作用与错误体系
//
// 本类型把 [`pi_result::Error`] 诊断报告和 [`RenameCommitEvidence`] 绑定在
// 同一个失败分支中。叶子错误仍由 adapter 使用 `thiserror` 定义、分类并按
// 项目统一方式转换；本载体不另建错误链，也不实现 `std::error::Error`。
//
// 改名的原子提交可能已经发生，而随后的协调收尾、结果转换或远端响应失败。
// 只返回普通错误会丢失恢复所需事实，因此调用方必须同时处理 report 和提交
// 证据。本类型不用于复制、替换、覆盖、删除或其它具有不同提交模型的操作。
//
// # 隐私、所有权与基本能力
//
// 字段对库外私有。类型不保存 source、destination、文件内容、原生句柄、
// 稳定身份或认证数据；两个 locator 由调用方继续拥有，未知结果后的新查询
// 不能只依赖错误中缓存的位置或元信息。
//
// 本类型不实现 `Clone`、`Copy`、`Default`、判等、排序或哈希。诊断 report
// 是一次性资产，不适合作为稳定集合键。`Send` 与 `Sync` 只通过字段条件性
// 自动获得，不使用手写 `unsafe impl`。
//
// `Debug` 与 `Display` 只组合 report 和证据的脱敏摘要，不查询 namespace。
// 类型不实现 `Deref`、`AsRef` 或元组转换，避免错误传播静默丢弃提交证据。
/// 严格改名失败时的统一诊断和提交证据。
///
/// 本类型不保存源、目标或文件资源，也不会在释放时重试、补偿或反向改名。
pub struct RenameFailure {
    error: Error,
    commit_evidence: RenameCommitEvidence,
}

impl RenameFailure {
    // 组合严格改名失败的统一诊断与提交证据。
    //
    // 参数顺序固定为“诊断报告、提交证据”，与 [`Self::into_parts`] 的返回
    // 顺序一致。两个参数按值进入载体，不克隆或重新包装 report。
    //
    // 本构造器保持公开，使 crate 外部的 [`crate::FileNamespace`] adapter
    // 能生产统一失败值。adapter 必须根据真实 backend 原子提交点选择证据，
    // 不能仅根据最终错误名称猜测改名是否发生。
    //
    // 本方法只移动字段，计划成本为 `O(1)`；不查询 locator、不执行 I/O、
    // 不加锁、不记录日志。虚假证据属于严重 adapter 合同错误，但构造值本身
    // 不建立裸内存安全证明，因此保持安全函数。
    #[must_use]
    /// 按“错误、提交证据”的顺序构造失败值。
    pub fn new(error: Error, commit_evidence: RenameCommitEvidence) -> Self {
        Self {
            error,
            commit_evidence,
        }
    }

    // 共享借用统一诊断报告。
    //
    // 返回引用严格绑定到 `self` 的本次借用，不克隆、移动或重新包装 report。
    // 本接口不提供可变引用，防止诊断与提交证据失配；需要字段所有权时必须
    // 使用 [`Self::into_parts`] 同时取回两者。
    //
    // 本方法为 `O(1)` 共享借用，无分配、无锁、无 I/O；合法调用不应 panic。
    #[must_use]
    /// 返回失败诊断的共享引用。
    pub fn error(&self) -> &Error {
        &self.error
    }

    // 共享借用本次改名记录的提交证据。
    //
    // 返回值只描述本次原子提交历史，不查询 source、destination 或当前对象
    // 身份。调用方必须与 [`Self::error`] 共同解释，不能把 `Unknown` 压缩成
    // 布尔值或据此盲目重试。
    //
    // 本方法为 `O(1)` 共享借用，无分配、无锁、无 I/O；合法调用不应 panic。
    #[must_use]
    /// 返回改名提交证据的共享引用。
    pub fn commit_evidence(&self) -> &RenameCommitEvidence {
        &self.commit_evidence
    }

    // 消费失败载体并取回诊断报告与提交证据。
    //
    // 返回顺序固定为 `(Error, RenameCommitEvidence)`，与 [`Self::new`] 的
    // 参数顺序对称。方法只移动字段，不查询或修改 namespace，也不重新分类
    // 错误。因为消费所有权，同一个值只能拆解一次。
    #[must_use]
    /// 消费失败值并按构造顺序返回全部字段。
    pub fn into_parts(self) -> (Error, RenameCommitEvidence) {
        (self.error, self.commit_evidence)
    }
}

impl fmt::Debug for RenameFailure {
    // 显示统一诊断与提交证据的脱敏调试摘要。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RenameFailure")
            .field("error", &self.error)
            .field("commit_evidence", &self.commit_evidence)
            .finish()
    }
}

impl fmt::Display for RenameFailure {
    // 输出面向日志的失败摘要，不承诺稳定文本协议。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; commit: {}",
            self.error, self.commit_evidence
        )
    }
}
