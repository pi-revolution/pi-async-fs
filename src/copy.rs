// 严格新建复制失败时的发布与暂存证据。
//
// [`CopyTargetEvidence`] 只描述一次 `copy_new` 操作能够证明自己对目标条目和
// 完整内容发布产生了什么影响。复制使用同一目标原子域内的暂存对象，真实
// 目标只能从“不由本次操作发布”一次跃迁为“完整发布”；部分内容只可能存在
// 于内部暂存对象，不能进入本类型。
//
// 复制采用分阶段取消语义：发布尚未提交时，取消禁止再发布并只对已经稳定
// 识别的暂存物作尽力清理；原子发布一旦提交便不能靠丢弃 Future 假装撤销，
// 内部拥有型完成者必须继续到安全停止点。Future 的 `Drop` 始终非阻塞，且
// 普通失败、取消或进程终止后均可能留下内部暂存资源。
//
// # 源文件协调
//
// `copy_new` 跟随 source 的最终符号链接并打开普通文件或受支持的远端文件
// 对象，再按打开句柄的稳定文件身份或远端稳定版本登记共享复制读取租约；
// locator 只参与定位，绝不能充当文件协调键。adapter 在取得稳定身份以前
// 必须使用短期候选获取许可，避免受管安全删除在“打开但尚未登记”的窗口
// 穿透；登记完成后释放目录项候选许可，长期只保留稳定身份租约。
//
// 复制只捕获一次源逻辑长度 `L`，随后读取同一稳定源对象的 `[0, L)`。共享
// 复制读取可以与其它普通读取、长度或元信息查询、普通强刷新及严格尾部追加
// 并发；追加只允许扩展捕获时的文件尾，复制不追随以后新增的字节。若捕获
// `L` 时一个应用层追加记录只完成了可见前缀，本操作可以包含该前缀；接口
// 不承诺记录级事务快照或整个源来自同一时点。
//
// 同一稳定源对象上的全文件覆盖、截断、任意待建立或活动 MMAP，以及安全
// `remove_file` 都与复制读取租约立即冲突，不等待另一方结束。默认 `rename`
// 不主动取得文件内容租约；平台允许时，已经打开的源句柄在改名后继续指向
// 原对象，平台拒绝时直接返回准确错误。非协调删除、库外句柄、其它进程和
// 未共享注册表的库副本不受本租约控制，但 adapter 仍必须借助拥有型句柄或
// 稳定远端版本维持 Rust 内存安全，不能在路径被复用后偷偷改读新对象。
//
// 进程级协调表按稳定身份分片，租约准入期望为 `O(1)`，不得扫描全部文件或
// 映射。同步临界区只允许身份比较、状态检查、计数和拥有型许可转移，其中
// 禁止 I/O、`.await`、数据复制、用户回调、可能阻塞的日志和无界分配；复制
// 循环复用已经取得的协调核心，不在每个数据块上重新查询全局表。
//
// # 最终目标条目排它预留
//
// 完成 locator、backend 和原子发布能力的无副作用检查后，`copy_new` 必须
// 在创建暂存物或读取 source 之前锚定 destination 的物理父 namespace，并
// 取得最终目标条目的进程内排它预留。预留键由 backend 身份、父目录稳定
// 身份及 backend 的原生最终名称等价键组成，不能直接使用未经解释的 locator
// 文本；Windows 必须服从目标目录真实的大小写敏感设置，远端则服从 backend
// 的对象键规则。
//
// 排它预留不是零长度占位文件，也不使 destination 提前存在。metadata、
// `try_exists`、目录枚举和其它只读 namespace 观察仍读取 backend 的真实状态；
// 但同一进程协调域中涉及同一条目的 `create_new`、`create_dir`、另一项
// `copy_new`、安全 rename、安全文件或空目录删除，以及新文件资源的候选 open
// 都必须立即返回冲突，不等待当前复制完成。显式非协调操作、库外调用、其它
// 进程和未共享注册表的库副本不受该预留控制。
//
// 预留从正式复制开始保持到原子发布明确完成、普通失败的安全清理完成，或
// 取消后的拥有型完成者到达安全停止点。跨异步阶段保存的是拥有型许可，绝不
// 保存同步锁 guard。因预留冲突而失败时，目标证据必须是
// [`CopyTargetEvidence::NotPublishedByOperation`]，且尚未创建暂存物时暂存
// 证据必须是 [`CopyStagingEvidence::NotCreatedByOperation`]。
//
// 同进程的两个同目标复制只有一个能够取得预留，另一项立即失败而不重复搬运
// 全部数据。预留不能代替底层原子 `no-replace`：其它进程仍可抢先创建任何
// 类型的 destination，最终发布必须据真实条件失败且不得触碰该对象。目标
// 条目协调按父身份和名称分片，准入期望 `O(1)`；不同目标可以并发，复制热
// 循环不重复访问该索引。
//
// # 内部暂存物
//
// 本地 adapter 必须在 destination 的同一物理父目录中原子排他创建暂存
// 文件；远端 adapter 必须使用同一原子发布域中的暂存对象、multipart upload
// 或语义等价资源。实现名称应组合进程级高熵随机量和单调操作序号以降低碰撞
// 与猜测概率，但名称格式不是稳定 API，随机性也不是正确性或安全边界。每次
// 创建仍必须使用原子 `create_new` 等价原语；碰撞时只能有界重选名称，绝不
// 覆盖、打开、复用或清理碰撞对象。
//
// 暂存物创建后必须立即取得稳定身份并登记内部 staging 排它租约。同一身份
// 上的公开读取、追加、覆盖、截断、普通强刷新、任意 MMAP、安全删除、改名
// 和候选 open 都立即冲突；内部复制写入只由该排它租约授权，不把暂存物伪装
// 成公开 [`crate::FileIo`] 资源。同时持有父目录的直接子项变更许可，使受管
// 空目录删除和父目录搬迁不能穿透；不同名称的普通子项操作仍可并发。所有
// 长期状态都由拥有型许可承载，同步锁内禁止 I/O 和 `.await`。
//
// 发布前必须证明暂存物从空内容开始、已经完整形成 source 的 `[0, L)`、
// 逻辑长度精确为 `L`，且普通写入管道已经排空并取得明确完成结果。该门禁
// 不包含 `fsync`、父目录同步、远端持久化屏障或抗掉电保证。只有满足全部
// 条件后，adapter 才能提交一次原子 `no-replace` 发布。
//
// destination 在发布前始终不承载部分内容，但本地同目录暂存文件本身可能被
// 目录枚举、原生工具或文件监听观察到；首版不伪造跨平台“完全不可见”保证，
// 也不从 `read_dir` 或 `walk` 中静默过滤。公开复制结果不主动交付暂存 locator，
// 调用方不得按实现名称前缀推断所有权或盲目删除。进程崩溃残留的专项回收
// 必须以后基于稳定身份和可验证标记独立设计。
//
// 库外句柄、其它进程、未共享注册表的库副本和恶意同权限参与者不受进程内
// 租约控制；高熵名称不是路径沙箱。实现仍应使用最小必要权限、平台允许的
// 独占打开和发布前稳定身份复核。原子本地改名成功会消费旧暂存名称；远端
// 发布若保留独立暂存资源，只按已冻结合同尽力清理，清理失败不能否定已经
// 明确成功的最终发布。
//
// # 跨进程协调边界
//
// 普通 `copy_new` 只加入当前进程内的受管协调域，不接收
// [`crate::FileNamespace::CrossProcessAuthority`]，也不创建 sidecar、协议记录
// 或 `fs4` 文件锁。source 的稳定身份已经进入跨进程 authority 域，或者
// destination 条目已经存在 `Published`、`Removing`、`Removed`、
// `IdentityConflict` 等协调协议记录时，本方法必须在可证明无发布副作用的
// 阶段返回冲突；不能自动升级、降级或绕过协议。仅仅配置了进程全局协调根
// 不会让所有普通文件自动进入该协议。
//
// 当前进程中的 authority、派生资源、映射和取消后完成者同样形成冲突。若
// 在创建暂存物前发现，失败证据为
// [`CopyTargetEvidence::NotPublishedByOperation`] 与
// [`CopyStagingEvidence::NotCreatedByOperation`]。其它进程中未加入协议的
// 普通访问仍不受本方法协调；底层原子 `no-replace` 继续防止 destination
// 被覆盖，但 source 不因此取得跨进程内容快照。
//
// 首批 API 不提供协调式复制重载或布尔选项。未来能力必须独立设计 source
// 授权、destination 新目标实例发布、双端锁序、崩溃恢复和协议状态机，不能
// 从本方法的安全 Rust 签名推导跨进程合作保证。
//
// # Backend 路由与跨域传输
//
// source 与 destination 可以属于不同 backend；只有暂存物与 destination
// 必须位于同一原子发布域。组合 namespace 在操作开始时分别路由两端一次，
// 后续不能按数据块或操作阶段反复选择 backend，也不能在多个 I/O 库之间转换
// 同一个公开文件资源。复制由 namespace 私有地拥有专用 source reader、
// destination staging writer 和发布能力，不接收或拼装已有公开 [`crate::FileIo`]
// 资源。
//
// source 必须提供稳定对象或版本、一次捕获的逻辑长度 `L` 以及对 `[0, L)`
// 的可靠读取；destination 必须提供完整暂存形成和原子 `no-replace` 发布。任一
// 能力不足时必须在可行的最早阶段返回 [`pi_result::ErrorKind::Unsupported`]，
// 不能降级为客户端 `exists` 后覆盖复制。
//
// 默认跨 backend 路径使用拥有型有界缓冲流，通常具有 `O(L)` 时间和 `O(C)`
// 额外工作内存，`C` 是 adapter 选择的有界窗口；接口不承诺固定窗口、零
// 用户态复制或零网络中转。同 backend 的 reflink、`copy_file_range`、服务端
// Copy 或写时复制可以作为内部优化，但只有在精确长度、完整暂存、原子不
// 覆盖、完整错误响应解析和当前元信息策略全部等价时才能采用。
//
// 标准 [`async_fs::copy`](https://docs.rs/async-fs/latest/async_fs/fn.copy.html)
// 和 Fusio 0.6.1 的通用 `Fs::copy` 都不满足该能力合同，不能成为严格复制的
// 直接实现或兜底路径。本地 adapter 使用 `async-fs` 拥有专用读写句柄并调用
// 私有的定长传输管道；远端 adapter 使用经过能力核验的 Fusio 具体实现或
// backend 专有条件 API。实际不支持的 backend 组合可以明确拒绝，不需要
// 改变公共签名。
//
// 本地正确性基线是：以 `async-fs` 分别打开稳定 source 和原子排他创建的
// staging，捕获一次 `L`，再通过 `futures-lite` 的 `AsyncRead`/`AsyncWrite`
// 或语义等价私有循环搬运恰好 `L` 字节。实现必须处理短读和短写；实际完成
// 少于 `L` 是意外文件尾并禁止发布，后续追加的数据则因定长限制不会进入
// staging。写入完成后先排空普通异步写管道并验证 staging 逻辑长度，再进入
// 平台专用原子发布；普通 flush 不等于 `sync_all` 或持久化提交。
//
// 即使 destination 参数改成内部 staging 路径，也不能调用 `async_fs::copy`：
// 它会按路径重新打开和截断目标、采用自己的元信息复制策略、读取到实际 EOF，
// 且不能提供本模块的取消阶段和身份证据。复制也不能由公开
// [`crate::FileIo`] 的读取与严格 append 方法拼装：append 表达正式文件的
// `O_APPEND` 语义，逐块调用还会重复 buffer 脱离、操作准入与错误包装；整个
// source 范围则需要一份跨全操作存活的共享复制读取租约。adapter 可以复用
// `FileIo` 下方的私有低层组件，但 namespace 层不能反向调用公共 trait 方法。
//
// Linux reflink、`copy_file_range`、Windows 专用复制设施及远端服务端 Copy
// 只能在后续专项测试证明其满足全部合同后作为优化快路径；第一项实现应先以
// 有界传输建立正确性基线，性能优化不得改变结果、证据或取消语义。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

use pi_result::Error;

// 严格新建复制普通错误完成时，对最终目标发布状态的可证明事实。
//
// # 作用与使用位置
//
// 本类型只用于后续 [`crate::CopyFailure`] 的目标证据字段，并最终由
// `FileNamespace::copy_new` 的普通失败分支返回。该方法先在 destination 的
// 同一原子发布域内形成完整暂存内容，再以严格不替换的单次原子操作发布最终
// 名称。成功分支使用 [`crate::CopyOutcome`]，不返回本枚举。
//
// 本类型刻意不表达暂存文件的空、部分或完整状态。暂存对象是否曾经建立、
// 是否已经清理，以及普通错误完成后是否可能遗留，由独立暂存证据说明；
// 调用方不能把暂存进度解释成真实 destination 的部分发布。
//
// # 时间边界与实时状态
//
// 每个变体只陈述普通错误完成点可以证明的本次操作历史。进程外参与者、库外
// 原生句柄或后续合法调用可能立即创建、修改、删除或改名 destination；因此
// 本值不能代替 metadata、存在性查询、稳定身份核验或恢复时的排它许可。
//
// [`Self::NotPublishedByOperation`] 不等于 destination 当前不存在：竞争者可以
// 已经创建该名称。[`Self::PublicationUnknown`] 也不允许调用方盲目重试或删除
// 当前目标，因为本次发布可能已经成功，或者同一位置可能已被后续参与者使用。
//
// # 持久性与重试边界
//
// 任一变体都不证明目标内容、文件元信息、父目录项、远端索引或对象版本已经
// 强制到达持久介质，也不提供掉电、操作系统崩溃、失信硬件缓存或硬件损坏
// 保证。这里的原子发布只约束操作系统或后端正常运行时的名称可见性；需要
// 崩溃持久性必须使用以后独立冻结的平台能力。
//
// 本类型刻意不提供 `is_retry_safe`。严格新建复制的可重试性同时取决于错误
// 分类、目标当前状态、源是否变化、暂存清理证据、外部参与者以及调用方是否
// 拥有恢复协议；单个布尔方法会隐藏这些必要条件。
//
// # 基本能力与演进
//
// 本类型不实现 `Clone`、`Copy` 或 `Default`。它实现 `Debug`、`Display`、
// 判等、全序和哈希，以支持诊断与确定性集合；排序只比较证据标签及字节字段，
// 不表示完成程度、证据强度、时间先后或恢复优先级。`Send` 与 `Sync` 由字段
// 自动获得，不使用手写 `unsafe impl`。
//
// `PublicationUnknown` 中的字节计数使用 `u64`，使证据可以描述大于当前
// 进程 `usize` 地址空间的本地文件或远端对象。本枚举不承诺稳定 ABI、整数
// 判别值、序列化格式或可反向解析的显示文本。`#[non_exhaustive]` 允许未来
// 增加更精确且不削弱现有原子发布语义的可移植证据。
#[non_exhaustive]
/// 严格新建复制失败时，对最终目标发布结果的可证明事实。
///
/// 证据只描述本次操作的历史，不是目标当前位置的实时快照，也不单独表示
/// 可以安全重试或删除当前同名对象。它不提供持久化保证。
pub enum CopyTargetEvidence {
    // 能证明本次复制没有把暂存内容发布为 destination。
    //
    // 常见情况包括参数、backend、原子发布域、权限或协调许可失败，source
    // 不存在，暂存复制失败，以及严格不替换发布发现 destination 已被竞争者
    // 占用。本变体不证明 destination 当前不存在，也不证明暂存对象已经清理。
    /// 能证明本次操作没有发布最终目标。
    ///
    /// 这不证明该位置当前不存在；其它参与者可能已经创建目标。
    NotPublishedByOperation,

    // 完整暂存内容已经形成并发起发布，但无法证明发布是否提交。
    //
    // 若本次发布成功，目标在发布线性化点的全部主逻辑内容必定完整，且长度
    // 精确为 `content_byte_len`；若发布未提交，则本次操作没有产生最终目标。
    // 该变体绝不允许“本次操作发布了部分目标”这一解释。
    //
    // 常见来源是远端条件提交响应丢失，或者底层原子发布已经进入不能安全
    // 重试的完成边界却无法取得最终结果。调用方必须重新查询并核验身份或
    // 版本，不能把当前同名目标直接归因于本次操作。
    /// 完整内容已经具备发布条件，但无法证明最终发布是否提交。
    ///
    /// 若发布成功，目标主内容长度精确为 `content_byte_len`；否则本次操作没有
    /// 发布最终目标。该变体绝不表示发布了部分目标。
    PublicationUnknown {
        // 若发布成功，最终目标应具有的完整主逻辑内容字节长度。
        /// 若发布成功，最终目标主内容的完整逻辑字节长度。
        content_byte_len: u64,
    },
}

impl fmt::Debug for CopyTargetEvidence {
    // 以包含变体名称和公开字节字段的开发者格式显示证据。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublishedByOperation => {
                formatter.write_str("NotPublishedByOperation")
            }
            Self::PublicationUnknown { content_byte_len } => formatter
                .debug_struct("PublicationUnknown")
                .field("content_byte_len", content_byte_len)
                .finish(),
        }
    }
}

impl fmt::Display for CopyTargetEvidence {
    // 输出面向日志的人类可读摘要，不承诺可反向解析。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublishedByOperation => {
                formatter.write_str("target not published by operation")
            }
            Self::PublicationUnknown { content_byte_len } => write!(
                formatter,
                "target publication outcome unknown; complete content length: {content_byte_len} bytes"
            ),
        }
    }
}

impl PartialEq for CopyTargetEvidence {
    // 按证据变体及其字节字段判等。
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::NotPublishedByOperation,
                Self::NotPublishedByOperation,
            ) => true,
            (
                Self::PublicationUnknown {
                    content_byte_len: left,
                },
                Self::PublicationUnknown {
                    content_byte_len: right,
                },
            ) => left == right,
            _ => false,
        }
    }
}

impl Eq for CopyTargetEvidence {}

impl PartialOrd for CopyTargetEvidence {
    // 返回与 [`Ord`] 一致的证据标签和字段全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CopyTargetEvidence {
    // 按变体声明顺序和字节字段比较，不表达完成度或恢复优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (
                Self::NotPublishedByOperation,
                Self::NotPublishedByOperation,
            ) => Ordering::Equal,
            (Self::NotPublishedByOperation, _) => Ordering::Less,
            (_, Self::NotPublishedByOperation) => Ordering::Greater,
            (
                Self::PublicationUnknown {
                    content_byte_len: left,
                },
                Self::PublicationUnknown {
                    content_byte_len: right,
                },
            ) => left.cmp(right),
        }
    }
}

impl Hash for CopyTargetEvidence {
    // 按与判等一致的证据变体和字节字段写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::NotPublishedByOperation => 0_u8.hash(state),
            Self::PublicationUnknown { content_byte_len } => {
                1_u8.hash(state);
                content_byte_len.hash(state);
            }
        }
    }
}

// 严格新建复制普通错误完成时，对内部暂存物生命周期的可证明事实。
//
// # 作用与使用位置
//
// 本类型是后续 [`crate::CopyFailure`] 的独立暂存证据字段。它只描述本次
// `copy_new` 为形成完整发布内容而创建、且 adapter 能够独立识别和管理的
// 暂存物；最终 destination 是否发布由 [`CopyTargetEvidence`] 说明，两个
// 证据不能互相替代。
//
// “暂存物”可以是本地同目录临时文件、远端临时对象、multipart upload，或
// backend 向 adapter 暴露的等价资源。服务端完全内部、adapter 既不能命名
// 也不能清理的实现细节不进入本类型；使用单次原子条件复制且没有可管理暂存
// 资源的实现可以报告 [`Self::NotCreatedByOperation`]。
//
// # 内容与恢复边界
//
// 本枚举不描述暂存内容的字节长度、连续前缀、完整性或可续传位置。暂存物
// 不是公开目标文件，调用方也没有取得其句柄、locator、认证信息或稳定身份
// 租约，因此不能根据本值继续复制、读取内容或自行删除内部路径。
//
// 特别地，[`Self::KnownPresentAtCompletion`] 不携带暂存 locator。仅返回一个
// 可能已经过期的名称，会诱使调用方删除同路径后来创建的其它对象；安全清理
// 必须由仍持有本次稳定身份、对象版本或 multipart upload identity 的 adapter
// 完成，或者进入以后独立设计的恢复工具。
//
// # 时间边界与异常退出
//
// 每个变体只陈述返回 [`crate::CopyFailure`] 的普通错误完成点所能证明的历史
// 事实。返回后，外部参与者可以删除、替换或重新创建相同临时名称；本值不是
// 实时状态查询，也不是长期清理授权。
//
// 调用方 Future 被丢弃后，`Drop` 不执行同步 I/O、网络请求或阻塞等待。发布
// 尚未提交时，adapter 必须停止提交新的复制工作且不得再发布 destination；
// 已经提交且不能抢占的底层工作由完全拥有所需定位符、句柄和协调状态的内部
// 完成者继续到安全停止点，不能借用已经结束的 Future、namespace、source 或
// destination 参数。进程内许可也必须保持到真实底层工作停止，不能随公开
// Future 提前释放。
//
// adapter 应对仍可按稳定身份识别的暂存物作一次安全的尽力清理，但不进行
// 无界重试。调用方已经离开，没有结果通道取得本值；发布已经提交时，调用方
// 必须按 destination 可能未发布或已完整发布处理。进程被强制终止时同样不会
// 产生错误返回；遗留暂存物属于后续专项清理或人工诊断范围，不能从一次不
// 存在的 `CopyFailure` 反向推导。
//
// # 持久性与非目标
//
// 本类型不证明临时名称删除、远端 abort、配额回收或对象生命周期变化已经
// 强制到达持久介质，也不提供掉电、操作系统崩溃、失信硬件缓存或硬件损坏
// 保证。它不把暂存清理描述成整个复制事务回滚：日志、指标、网络请求、远端
// 版本、缓存和计费记录仍可能存在。
//
// # 基本能力与演进
//
// 本类型不实现 `Clone`、`Copy` 或 `Default`。它实现 `Debug`、`Display`、
// 判等、全序和哈希，便于诊断与确定性集合；排序只比较变体声明顺序，不表示
// 清理质量、证据强度、时间先后或恢复优先级。`Send` 与 `Sync` 由无字段枚举
// 自动获得，不使用手写 `unsafe impl`。
//
// 本枚举不承诺稳定 ABI、整数判别值、序列化格式或可反向解析的显示文本。
// `#[non_exhaustive]` 允许未来增加后端能够可靠证明的新暂存终态，而不要求
// 调用方把当前四个变体误解为所有平台内部状态的封闭全集。
#[non_exhaustive]
/// 严格新建复制失败时，对中间产物生命周期的可证明事实。
///
/// 本值与最终目标证据彼此独立，只描述本次操作管理的中间产物。它不公开
/// 位置、句柄或清理权限，也不能用于盲目删除任何同名对象。
pub enum CopyStagingEvidence {
    // 能证明本次操作没有创建需要 adapter 管理的暂存物。
    //
    // 常见情况包括参数或 source 检查失败、目标协调准入失败，以及 backend
    // 使用单次原子条件复制而没有暴露独立暂存资源。本变体不证明 backend
    // 内部从未使用缓存、服务端任务或不可管理的中间状态。
    /// 能证明本次操作没有创建需要管理的中间产物。
    NotCreatedByOperation,

    // 能证明本次操作创建过暂存物，随后又移除了同一暂存身份。
    //
    // “移除”只覆盖 adapter 能管理的临时名称、对象版本、multipart upload
    // 或等价资源；它不证明日志、指标、配额历史、缓存和远端审计记录已经
    // 回滚，也不保证相同临时名称没有在证据产生后被重新使用。
    /// 能证明本次操作创建过中间产物，随后移除了同一产物。
    CreatedThenRemovedByOperation,

    // 普通错误完成点能证明本次操作创建的暂存物仍然存在。
    //
    // adapter 必须仍能把该暂存物与本次操作的稳定身份或版本关联，不能只因
    // 某个临时路径当前存在便选择本变体。返回后该对象可以被外部参与者修改
    // 或删除；本变体也不授予调用方按路径清理的权利。
    /// 错误完成点能证明本次操作的中间产物仍存在。
    ///
    /// 返回后状态仍可能变化；本变体不授予调用方清理权限。
    KnownPresentAtCompletion,

    // 无法证明暂存物是否创建、是否清理或是否仍然存在。
    //
    // 典型来源是远端清理响应丢失、条件发布与 abort 的完成竞态，或者 backend
    // 没有提供足以稳定识别暂存资源的状态。调用方必须按可能遗留需要运维
    // 处理的暂存物解释，不能降级为“没有临时副作用”。
    /// 无法证明中间产物是否创建、清理或仍然存在。
    Unknown,
}

impl fmt::Debug for CopyStagingEvidence {
    // 以包含暂存证据变体名称的开发者格式显示。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotCreatedByOperation => "NotCreatedByOperation",
            Self::CreatedThenRemovedByOperation => {
                "CreatedThenRemovedByOperation"
            }
            Self::KnownPresentAtCompletion => "KnownPresentAtCompletion",
            Self::Unknown => "Unknown",
        })
    }
}

impl fmt::Display for CopyStagingEvidence {
    // 输出面向日志的人类可读暂存摘要，不承诺可反向解析。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::NotCreatedByOperation => {
                "staging object not created by operation"
            }
            Self::CreatedThenRemovedByOperation => {
                "staging object created then removed by operation"
            }
            Self::KnownPresentAtCompletion => {
                "staging object known present at completion"
            }
            Self::Unknown => "staging object outcome unknown",
        })
    }
}

impl PartialEq for CopyStagingEvidence {
    // 仅在两个值表示相同暂存证据变体时判等。
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl Eq for CopyStagingEvidence {}

impl PartialOrd for CopyStagingEvidence {
    // 返回与 [`Ord`] 一致的暂存证据变体全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CopyStagingEvidence {
    // 按变体声明顺序比较，不表达清理质量或恢复优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        fn tag(value: &CopyStagingEvidence) -> u8 {
            match value {
                CopyStagingEvidence::NotCreatedByOperation => 0,
                CopyStagingEvidence::CreatedThenRemovedByOperation => 1,
                CopyStagingEvidence::KnownPresentAtCompletion => 2,
                CopyStagingEvidence::Unknown => 3,
            }
        }

        tag(self).cmp(&tag(other))
    }
}

impl Hash for CopyStagingEvidence {
    // 按与判等一致的暂存证据变体写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        core::mem::discriminant(self).hash(state)
    }
}

// 严格新建复制成功后目标主内容的逻辑结果。
//
// # 作用与使用位置
//
// 本类型是后续 `FileNamespace::copy_new` 的成功值。它证明严格不替换的目标
// 已经成功形成完整权威复制数据流，并给出目标主内容在复制提交点的逻辑字节
// 长度。失败分支使用 [`CopyFailure`]；本类型不携带错误、部分进度或恢复资产。
//
// 使用命名结果而不是裸 `u64`，是为了避免把“逻辑内容长度”误读成实际经过
// 用户态、网络、系统调用或存储设备传输了多少字节。文件系统克隆、写时复制、
// 稀疏复制或远端服务端复制可以只搬运少量甚至不搬运用户态数据，但仍返回
// 完整目标内容长度。
//
// # 字节范围与非目标
//
// `content_byte_len` 只统计目标的主逻辑内容。它不统计稀疏区物理分配、文件
// 系统块、扩展属性、Windows 备用数据流、对象 metadata、ACL、协议头、分片
// 请求、临时对象或缓存开销。具体复制是否保留任何可移植元信息或链接语义，
// 由复制方法合同独立决定，不能从本计数推导。
//
// 本结果也不证明目标内容或父目录已经强制到达持久介质，不提供掉电、操作
// 系统崩溃、失信硬件缓存或硬件损坏保证。返回后其它参与者仍可立即修改、
// 改名或删除目标，因此它是成功提交事实，不是持续锁定的实时元信息快照。
//
// 已确认的原子发布成功始终返回本类型，不能因为后续日志、结果包装或独立
// 暂存物清理失败而伪装成复制失败。某些 backend 的发布会直接消费暂存物，
// 另一些 backend 只能在发布后尽力删除独立临时对象；本结果不携带暂存清理
// 证据，普通成功仍可能伴随安全隔离、不可经 destination 访问的内部残留。
//
// # 基本能力与演进
//
// 本类型不实现 `Clone`、`Copy` 或 `Default`。它实现 `Debug`、`Display`、
// 判等、全序和哈希，以支持诊断与确定性集合；排序只比较逻辑长度，不表示
// 复制先后、性能、持久性或结果质量。`Send` 与 `Sync` 由 `u64` 字段自动
// 获得，不使用手写 `unsafe impl`。
//
// 类型不实现 `From<u64>`、`Into<u64>`、`Deref` 或 `AsRef`，避免转换隐藏
// 字节单位和成功语义。它不承诺稳定 ABI、序列化格式或可反向解析的显示文本。
// `#[non_exhaustive]` 允许未来增加版本或 backend 证据，而不迫使当前调用方
// 依赖字段布局。
#[non_exhaustive]
/// 严格新建复制成功时的逻辑结果。
///
/// 该值记录成功发布目标主内容的逻辑长度，不表示物理传输量、存储占用、
/// 元信息保留程度或持久化等级。返回后目标仍可能被其它参与者修改。
pub struct CopyOutcome {
    content_byte_len: u64,
}

impl CopyOutcome {
    // 构造严格新建复制的成功结果。
    //
    // `content_byte_len` 必须是完整权威复制数据流以及成功提交点目标主内容的
    // 精确逻辑长度。构造器不能自行查询目标或验证 adapter 是否已经完整复制；
    // 生产本值的 [`crate::FileNamespace`] 实现负责在调用前建立该事实。
    //
    // 本构造器保持公开，使 crate 外部 adapter 可以生产统一结果。它只移动
    // 一个 `u64` 字段，计划为无分配、无锁、无 I/O、无日志和无外部副作用的
    // `O(1)` 操作；合法输入不应 panic。
    #[must_use]
    /// 以目标主内容的精确逻辑字节长度构造成功结果。
    pub fn new(content_byte_len: u64) -> Self {
        Self { content_byte_len }
    }

    // 返回复制完成点目标主内容的精确逻辑字节长度。
    //
    // 该值不重新查询 destination，不随目标后续变化而更新，也不表示物理
    // 传输量、存储占用或持久化程度。重复调用返回相同标量；方法计划为无
    // 分配、无锁、无 I/O 和无外部副作用的 `O(1)` 操作，合法调用不应 panic。
    #[must_use]
    /// 返回成功提交点的目标主内容逻辑字节长度。
    pub fn content_byte_len(&self) -> u64 {
        self.content_byte_len
    }
}

impl fmt::Debug for CopyOutcome {
    // 以包含字段名和字节单位的开发者格式显示成功结果。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CopyOutcome")
            .field("content_byte_len", &self.content_byte_len)
            .finish()
    }
}

impl fmt::Display for CopyOutcome {
    // 输出面向日志的逻辑内容长度摘要，不承诺可反向解析。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "copied content length: {} bytes",
            self.content_byte_len
        )
    }
}

impl PartialEq for CopyOutcome {
    // 仅按目标主内容逻辑长度判等。
    fn eq(&self, other: &Self) -> bool {
        self.content_byte_len == other.content_byte_len
    }
}

impl Eq for CopyOutcome {}

impl PartialOrd for CopyOutcome {
    // 返回与 [`Ord`] 一致的逻辑长度全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CopyOutcome {
    // 按目标主内容逻辑长度比较，不表达复制质量或恢复优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        self.content_byte_len.cmp(&other.content_byte_len)
    }
}

impl Hash for CopyOutcome {
    // 将目标主内容逻辑长度写入调用方哈希器。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.content_byte_len.hash(state)
    }
}

// 严格新建复制普通失败时的统一诊断、目标证据与暂存证据载体。
//
// # 作用与错误体系
//
// 本类型把 [`pi_result::Error`]、[`CopyTargetEvidence`] 和
// [`CopyStagingEvidence`] 作为一个不可分割的普通错误结果交还。三者依次
// 解释失败原因与错误链、本次操作能证明的最终 destination 发布状态，以及
// adapter 管理的内部暂存资源状态。缺少任一维度都会丢失安全恢复或运维诊断
// 所需的信息。
//
// 叶子 adapter 继续使用 `thiserror` 定义具体错误并按项目规则转换为统一
// report；本载体不另建错误链，也不实现 [`std::error::Error`]。它是携带
// 恢复证据的一次性结果信封，不应被当作可随意擦除证据的普通错误对象。
//
// # 所有权、定位符与隐私
//
// 三个字段均为拥有型且对库外私有。类型不重复保存 source 或 destination
// locator：复制方法只借用两个定位符，调用方在普通错误后仍拥有原值；缓存
// locator 也不能成为实时目标状态或稳定身份证明。需要恢复时必须重新查询，
// 并把查询结果与本次历史证据共同解释。
//
// 本类型不保存文件句柄、源内容、目标内容、暂存定位符、认证信息或 backend
// 客户端。它不会在 Drop 时重试、删除暂存物或执行补偿；真实资源和取消后的
// 完成责任必须由复制 Future 的合同管理。刻意不公开暂存定位符，避免调用方
// 在名称已被其它操作复用后盲目删除不属于本次复制的资源。
//
// 普通失败返回前，adapter 应对仍能按稳定身份安全识别的暂存物作一次尽力
// 清理，但不得为制造整洁外观执行无界重试、删除 destination 或按可复用名称
// 盲删。清理成功、已知残留或无法判定分别由 `staging_evidence` 如实表达；
// 清理失败不能被压缩为“本次从未创建暂存物”。
//
// 如果复制主流程与暂存清理同时失败，`error` 必须保留使 `copy_new` 无法正常
// 成功的主错误及其当前 [`pi_result::ErrorKind`]。完整清理 report 作为第二个
// [`pi_result::Error`] attachment 附加，并用
// [`pi_result::attachment::Stage`] 标记 `copy_staging_cleanup` 阶段；它不能
// 替换主分类。调用方需要清理诊断时可以从 report attachment 取得该错误，
// 但暂存物是否清理仍只由 `staging_evidence` 结构化表达。
//
// 未执行清理时不伪造清理错误，只按真实可证明事实选择暂存证据。最终发布
// 已明确成功后发生的独立暂存清理失败不会产生本载体：成功仍返回
// [`CopyOutcome`]，清理问题只能进入 adapter 的日志、指标或后续回收记录。
//
// # 基本能力与线程语义
//
// 本类型不实现 `Clone`、`Copy`、`Default`、判等、排序或哈希。诊断 report
// 不是稳定值键，两类证据也不应脱离错误原因被复制传播。`Send` 与 `Sync`
// 只由字段条件性自动获得，不使用手写 `unsafe impl`。
//
// `Debug` 与 `Display` 只组合统一诊断和两类证据的脱敏摘要，不查询文件系统、
// 不显示源或目标路径，也不承诺稳定文本协议。类型不实现 `Deref`、`AsRef`
// 或元组转换，避免通用错误传播静默丢弃目标证据。
/// 严格新建复制失败时的统一诊断和副作用证据。
///
/// `error` 描述失败原因，目标证据和中间产物证据分别描述两个独立状态维度。
/// 本类型不保存源或目标定位符，也不会在释放时重试、清理或补偿。
pub struct CopyFailure {
    error: Error,
    target_evidence: CopyTargetEvidence,
    staging_evidence: CopyStagingEvidence,
}

impl CopyFailure {
    // 组合严格新建复制的统一诊断、目标证据与暂存证据。
    //
    // 参数顺序固定为“诊断报告、最终目标证据、内部暂存证据”，与
    // [`Self::into_parts`] 的返回顺序一致。三个参数按值移入载体，不克隆、
    // 重新分类或查询后端。
    //
    // 本构造器保持公开，使 crate 外部的 [`crate::FileNamespace`] adapter
    // 可以生产统一失败值。adapter 必须根据真实内容形成、原子发布和暂存清理
    // 提交点分别选择两类证据，不能仅根据最终错误名称或定位符当前状态猜测。
    // 构造器不验证 backend 特有的证据组合；第三方 adapter 必须保证两个证据
    // 分别真实，即使某些组合在内置实现中通常不会出现。
    //
    // 本方法只移动字段，计划成本为 `O(1)`；无分配、无锁、无 I/O、无日志
    // 或外部副作用，合法调用不应 panic。虚假证据属于严重 adapter 合同错误，
    // 但构造值本身不建立裸内存安全证明，因此保持安全函数。
    #[must_use]
    /// 按“错误、目标证据、中间产物证据”的顺序构造失败值。
    pub fn new(
        error: Error,
        target_evidence: CopyTargetEvidence,
        staging_evidence: CopyStagingEvidence,
    ) -> Self {
        Self {
            error,
            target_evidence,
            staging_evidence,
        }
    }

    // 共享借用本次复制失败的统一诊断报告。
    //
    // 返回引用严格绑定到 `self` 的本次借用，不克隆、移动、修改或重新包装
    // report，也不触发日志。调用方查看诊断后仍可读取两类证据，或者在借用
    // 结束后使用 [`Self::into_parts`] 一次性取得三个字段。
    //
    // 本方法为无分配、无锁、无 I/O 的 `O(1)` 共享借用；合法调用不应 panic。
    #[must_use]
    /// 返回失败诊断的共享引用。
    pub fn error(&self) -> &Error {
        &self.error
    }

    // 共享借用本次复制对目标产生的可证明副作用。
    //
    // 返回值只描述普通错误完成点记录的本次操作历史，不重新查询 destination，
    // 也不证明目标当前仍存在、仍完整或仍属于本次创建。调用方必须与
    // [`Self::error`] 共同解释，不能把任一证据变体压缩成无条件重试判断。
    //
    // 本方法为无分配、无锁、无 I/O 的 `O(1)` 共享借用；合法调用不应 panic。
    #[must_use]
    /// 返回最终目标发布证据的共享引用。
    pub fn target_evidence(&self) -> &CopyTargetEvidence {
        &self.target_evidence
    }

    // 共享借用本次复制对内部暂存资源产生的可证明副作用。
    //
    // 返回值只描述 adapter 管理的暂存文件、暂存对象或等价上传会话在普通
    // 错误完成点的历史证据；它不公开暂存定位符，不触发清理，也不证明该资源
    // 在调用方读取时仍然存在。调用方必须把它与 [`Self::error`] 及
    // [`Self::target_evidence`] 一起解释。
    //
    // 本方法为无分配、无锁、无 I/O 的 `O(1)` 共享借用；合法调用不应 panic。
    #[must_use]
    /// 返回中间产物生命周期证据的共享引用。
    pub fn staging_evidence(&self) -> &CopyStagingEvidence {
        &self.staging_evidence
    }

    // 消费失败载体并取回统一诊断、目标证据与暂存证据。
    //
    // 返回顺序固定为
    // `(Error, CopyTargetEvidence, CopyStagingEvidence)`，与 [`Self::new`] 的
    // 参数顺序对称。方法只移动字段，不查询或修改 source/destination，不
    // 重试、删除或补偿复制结果。消费所有权保证同一载体只能拆解一次。
    //
    // 本方法计划为无分配、无锁、无 I/O 的 `O(1)` 字段移动；合法调用不应
    // panic。它没有载体之外的外部副作用。
    #[must_use]
    /// 消费失败值并按构造顺序返回全部字段。
    pub fn into_parts(
        self,
    ) -> (Error, CopyTargetEvidence, CopyStagingEvidence) {
        (self.error, self.target_evidence, self.staging_evidence)
    }
}

impl fmt::Debug for CopyFailure {
    // 显示统一诊断、目标证据与暂存证据的脱敏开发者摘要。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CopyFailure")
            .field("error", &self.error)
            .field("target_evidence", &self.target_evidence)
            .field("staging_evidence", &self.staging_evidence)
            .finish()
    }
}

impl fmt::Display for CopyFailure {
    // 输出面向日志的失败摘要，不承诺可反向解析或稳定文本协议。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; target: {}; staging: {}",
            self.error, self.target_evidence, self.staging_evidence
        )
    }
}
