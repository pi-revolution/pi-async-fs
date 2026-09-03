// 文件内容 I/O 的普通错误进度模型。
//
// 文件读取或写入即使最终返回错误，也可能已经处理一段连续字节。
// [`TransferProgress`] 表达调用方能够可靠确认的字节前缀及其确定性；
// [`BufferFailure`] 则是仅用于写侧的错误恢复信封。读取失败不通过本模块
// 返还目标缓冲区的引用或所有权。

use core::fmt;
use core::hash::{Hash, Hasher};

use pi_result::Error;

// 普通完成错误发生时，一次文件内容传输已经处理的连续前缀。
//
// # 在读取与写入中的含义
//
// - 对读取，字节数表示本次目标窗口起点之后，已经初始化并提交给调用方的
//   连续前缀；它不包含目标窗口以外的旧内容。
// - 对写入或尾部追加，字节数表示权威写入源起点之后，已确认由本次操作
//   处理的连续前缀。具体方法必须进一步说明“处理”是写入操作系统、被远端
//   服务确认，还是其它后端完成点。
//
// 无论哪种操作，本值都不证明字节已经刷新到磁盘、通过 `sync` 达到持久化、
// 在远端达到强一致可见性，或在掉电后仍然存在。持久性必须由独立 API 和
// 成功合同表达。
//
// # 与写侧原始缓冲区返还的关系
//
// 写入普通错误恢复载体会同时保存本值和调用时传入的原始
// 写入缓冲区。返还原值只解决 allocation、引用或所有权复用问题，
// 不代表可以从头重新提交：
// [`Self::AtLeast`] 与 [`Self::Unknown`] 都要求调用方先消除不确定性；即使是
// [`Self::Exact`]，尾部追加也只能从明确未处理的后缀继续，不能重复前缀。
// 读取错误不使用 [`BufferFailure`]，也不返还任何 buffer。
//
// # 不变量与 adapter 责任
//
// `bytes` 必须不超过该方法本次权威逻辑请求的字节长度。该上限取决于实际
// 缓冲区，无法由本枚举单独验证；每个 `FileIo` 实现必须在构造结果前核对。
// 如果后端只能报告离散片段、乱序完成或不连续区域，adapter 不得把总和
// 冒充为连续前缀，应降级为 [`Self::Unknown`] 或使用未来的独立分段接口。
#[non_exhaustive]
/// 写入失败时可证明的连续输入前缀进度。
///
/// 字节数均从本次逻辑请求起点计算，且不得超过请求长度。进度只描述后端
/// 已处理事实，不证明持久化，也不单独表示操作可安全重试。
pub enum TransferProgress {
    // 已经准确确认处理的连续前缀长度。
    /// 已准确确认处理的连续前缀长度。
    Exact {
        // 从本次逻辑请求起点开始计算的精确字节数。
        /// 精确连续前缀字节数。
        bytes: usize,
    },

    // 至少已经处理给定连续前缀，但后续完成量无法确认。
    /// 至少已处理给定连续前缀，但无法确认是否处理了更多字节。
    AtLeast {
        // 能够可靠证明已经处理的最小连续前缀长度。
        /// 可证明的最小连续前缀字节数。
        bytes: usize,
    },

    // 无法给出可信的已处理字节下限。
    //
    // 实际结果可能是零字节、部分字节或整个请求。该状态与能够证明零副作用
    // 的 `Exact { bytes: 0 }` 不同。
    /// 无法给出可信的已处理字节下限。
    ///
    /// 这不同于能够证明零副作用的 `Exact { bytes: 0 }`。
    Unknown,
}

impl TransferProgress {
    // 返回能够可靠证明已经处理的最小连续前缀长度。
    //
    // `Exact` 与 `AtLeast` 返回其字段值；`Unknown` 返回零。对 `Unknown` 返回
    // 零只表示没有可证明的正下限，不表示实际没有处理字节。该方法无分配、
    // 无锁、无 I/O、无外部副作用，并计划为 O(1)。
    #[must_use]
    /// 返回可证明的最小连续前缀长度。
    ///
    /// `Unknown` 返回零，但这不表示实际处理量为零。
    pub fn minimum_bytes(&self) -> usize {
        match self {
            Self::Exact { bytes } | Self::AtLeast { bytes } => *bytes,
            Self::Unknown => 0,
        }
    }

    // 在进度精确时返回处理字节数，否则返回 `None`。
    //
    // `Some(0)` 明确证明没有处理任何字节；`None` 不能按零解释。该方法不
    // 查询后端或文件状态，并计划为 O(1)。
    #[must_use]
    /// 在进度精确时返回字节数，否则返回 `None`。
    pub fn exact_bytes(&self) -> Option<usize> {
        match self {
            Self::Exact { bytes } => Some(*bytes),
            Self::AtLeast { .. } | Self::Unknown => None,
        }
    }

    // 判断进度是否为一个完整、精确的字节计数。
    //
    // 只有 [`Self::Exact`] 返回 `true`。该判断不证明操作整体成功，也不表示
    // 已处理字节已经持久化。
    #[must_use]
    /// 仅当进度为 [`Self::Exact`] 时返回 `true`。
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact { .. })
    }

    // 判断实际处理量是否仍然存在不确定部分。
    //
    // [`Self::AtLeast`] 与 [`Self::Unknown`] 返回 `true`。调用方不得仅凭原始
    // 缓冲区仍可用便盲目重试这些操作，尤其不得从头重试非幂等尾部追加。
    #[must_use]
    /// 当实际处理量仍可能大于可证明下限时返回 `true`。
    pub fn is_uncertain(&self) -> bool {
        !self.is_exact()
    }
}

impl Clone for TransferProgress {
    // 显式复制进度事实，不复制缓冲区、文件内容或后端状态。
    fn clone(&self) -> Self {
        match self {
            Self::Exact { bytes } => Self::Exact { bytes: *bytes },
            Self::AtLeast { bytes } => Self::AtLeast { bytes: *bytes },
            Self::Unknown => Self::Unknown,
        }
    }
}

impl fmt::Debug for TransferProgress {
    // 以包含变体名称和字段名称的开发者格式显示进度。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact { bytes } => formatter
                .debug_struct("Exact")
                .field("bytes", bytes)
                .finish(),
            Self::AtLeast { bytes } => formatter
                .debug_struct("AtLeast")
                .field("bytes", bytes)
                .finish(),
            Self::Unknown => formatter.write_str("Unknown"),
        }
    }
}

impl fmt::Display for TransferProgress {
    // 以面向日志的确定性和字节数量摘要显示进度。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exact { bytes } => write!(formatter, "exactly {bytes} bytes"),
            Self::AtLeast { bytes } => write!(formatter, "at least {bytes} bytes"),
            Self::Unknown => formatter.write_str("unknown progress"),
        }
    }
}

impl PartialEq for TransferProgress {
    // 按确定性变体和字节字段判等。
    //
    // `Exact { bytes: n }` 与 `AtLeast { bytes: n }` 不相等，因为二者对能否
    // 安全恢复或重试提供的知识不同。
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Exact { bytes: left }, Self::Exact { bytes: right })
            | (Self::AtLeast { bytes: left }, Self::AtLeast { bytes: right }) => {
                left == right
            }
            (Self::Unknown, Self::Unknown) => true,
            _ => false,
        }
    }
}

impl Eq for TransferProgress {}

impl Hash for TransferProgress {
    // 将确定性变体和字节字段共同写入哈希器。
    //
    // 哈希只服务进程内集合与诊断，不能作为持久化进度、请求幂等键或远端
    // 提交身份。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::Exact { bytes } => {
                0_u8.hash(state);
                bytes.hash(state);
            }
            Self::AtLeast { bytes } => {
                1_u8.hash(state);
                bytes.hash(state);
            }
            Self::Unknown => 2_u8.hash(state),
        }
    }
}

// 文件内容写入普通错误的诊断、原始缓冲区与传输进度恢复载体。
//
// # 为什么需要独立载体
//
// 公共文件写入方法允许按值接收 owned buffer 或带调用期生命周期的引用。
// 操作成功时消费输入值；Future 被正常轮询到错误完成时，调用方需要取回同
// 一个原始 `B`，以复用 allocation、引用、池租约或其它所有权状态。同时，
// 文件可能已经接收部分字节，因此只返回 `B` 会错误暗示可以从头
// 重试。本类型把写入错误报告、恢复资产和 [`TransferProgress`] 作为一个不可
// 分割的普通完成结果交还。
//
// # 与 `pi_result` 的关系
//
// `error` 固定使用 [`pi_result::Error`]，也就是公共边界的
// `Report<ErrorKind>`。`B` 不会被塞入 report 的 context 或 attachment：
// 借用型、独占型或池化缓冲区可能只满足操作所需的 `Send + 'a`，并不满足
// 普通错误对象经常要求的 `Sync + 'static`，而且附件也不能安全表达一次性
// 恢复所有权。
//
// 本类型本身故意不实现 [`std::error::Error`]。它是包含错误和可恢复资产的
// 结果信封，不是可自由传播、共享或重复格式化后丢弃的普通错误对象。调用方
// 可以通过 [`Self::error`] 读取报告，或用 [`Self::into_parts`] 一次性拆解。
//
// # 成功、普通错误与取消
//
// - 成功路径不产生本值，并按具体方法合同消费 `B`；
// - 写入 Future 正常完成为错误时，adapter 必须先停止对内部载体的访问，再恢复并
//   返还调用时的同一个原始 `B`；
// - 调用方直接丢弃 Future 时不存在同步交付本值的返回通道。取消后的内存
//   存活、后台完成和最终释放由操作 Future 合同承担，不能伪装成普通错误。
//
// “同一个原始 `B`”不仅表示字节相等。后续按需脱离接口必须按具体类型保存
// 并恢复 owned/borrowed/Cow 变体、allocation、引用身份、容量、池租约及该
// 类型合同要求保持的其它状态。已经产生的外部文件副作用不会因恢复 `B` 而
// 自动回滚。
//
// # 重试与部分副作用
//
// 前置校验失败必须携带 `TransferProgress::Exact { bytes: 0 }`。其它错误是否
// 可以重试，必须同时检查操作幂等性和 `progress`：非幂等追加即使返还完整
// 原缓冲区，也不能忽略已提交前缀或不确定完成状态后从头盲目重试。
/// 写入失败时的统一诊断、原始输入值和可证明进度。
///
/// 普通失败返还与调用时同一逻辑值的 `B`，以便复用分配或引用；这不表示
/// 文件未改变或从头重试安全。释放本类型不会执行 I/O、回滚或重试。
pub struct BufferFailure<B> {
    pub(crate) error: Error,
    pub(crate) buffer: B,
    pub(crate) progress: TransferProgress,
}

impl<B> BufferFailure<B> {
    // 组合一个已经完成恢复的普通错误结果。
    //
    // 参数顺序固定为“诊断报告、恢复资产、传输进度”，与
    // [`Self::into_parts`] 的返回顺序一致。构造本身不验证 `progress` 是否
    // 超过 `buffer` 的逻辑请求长度，因为本类型不要求 `B` 具有统一字节视图；
    // 产生写入错误的 `FileIo` 实现必须在调用本接口前完成该检查。
    //
    // 本方法不执行 I/O、不重试操作，也不修改报告或缓冲区，只以 O(1)
    // 移动字段形成失败载体。
    #[must_use]
    /// 按“错误、原始输入、进度”的顺序构造失败值。
    pub fn new(error: Error, buffer: B, progress: TransferProgress) -> Self {
        Self {
            error,
            buffer,
            progress,
        }
    }

    // 借用统一的 `pi_result` 错误报告。
    //
    // 返回引用只在 `self` 借用期内有效；不会克隆 report、修改 frame、附加
    // 上下文或触发日志。调用方仍须保留本载体，才能随后取回原始缓冲区。
    #[must_use]
    /// 返回失败诊断的共享引用。
    pub fn error(&self) -> &Error {
        &self.error
    }

    // 共享借用尚未被取回的原始缓冲区值。
    //
    // 本接口不要求 `B: AsRef<[u8]>`，也不把内容写入日志。它故意不提供
    // `&mut B`：在调用方理解错误和进度前修改缓冲区，可能破坏原始请求范围
    // 与恢复策略的对应关系。需要取得所有权时应消费载体并调用
    // [`Self::into_parts`]。
    #[must_use]
    /// 返回原始输入值的共享引用。
    pub fn buffer(&self) -> &B {
        &self.buffer
    }

    // 借用与本次普通错误对应的传输进度。
    //
    // 该引用不重新查询文件或后端，因此只代表错误完成点已经记录的事实；
    // 外部协作者可能在此后继续改变同一文件。
    #[must_use]
    /// 返回可证明进度的共享引用。
    pub fn progress(&self) -> &TransferProgress {
        &self.progress
    }

    // 一次性拆解错误报告、原始缓冲区和传输进度。
    //
    // 返回顺序与 [`Self::new`] 的参数顺序相同。该方法消费 `self`，从而保证
    // 恢复资产只能被安全移动出一次；它不克隆缓冲区、不重试 I/O，也不因
    // 拆解而回滚已有文件副作用。后续真实实现计划为 O(1) 字段移动。
    #[must_use]
    /// 消费失败值并按构造顺序返回全部字段。
    pub fn into_parts(self) -> (Error, B, TransferProgress) {
        (self.error, self.buffer, self.progress)
    }
}

impl<B> fmt::Debug for BufferFailure<B> {
    // 显示错误报告、传输进度、`B` 的类型名称和脱敏占位符。
    //
    // 该实现不要求 `B: Debug`，不会调用缓冲区自己的格式化逻辑，也不得输出
    // 原始文件内容、备用容量、指针、引用计数、池槽位或原生句柄。格式化不
    // 执行文件/网络 I/O，不消费恢复资产；错误报告自身的调试成本仍由其已有
    // frame 与 attachment 数量决定。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BufferFailure")
            .field("error", &self.error)
            .field("buffer_type", &core::any::type_name::<B>())
            .field("buffer", &"<redacted>")
            .field("progress", &self.progress)
            .finish()
    }
}

impl<B> fmt::Display for BufferFailure<B> {
    // 显示错误报告与传输进度的面向人类摘要。
    //
    // 输出故意省略 `B` 的类型和全部内容，不能用来恢复、解析、序列化或判断
    // 缓冲区身份。格式化失败只通过 `fmt::Error` 返回，不改变任何所有权。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; progress: {}",
            self.error, self.progress
        )
    }
}
