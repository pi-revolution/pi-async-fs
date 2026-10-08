// 已打开文件资源的单一访问角色。
//
// [`FileAccessMode`] 描述 namespace 创建或打开的一个公开文件资源在其整个
// 生命周期内可以申请哪一类操作。它不是底层操作系统标志的直接镜像，也不
// 表示协调等级：相同访问角色既可以用于普通进程内资源，也可以用于已经绑定
// [`crate::CrossProcessFileAuthority`] 的跨进程协调资源。
//
// 每个资源只有一个角色。需要普通读取、严格追加、全文件覆盖和内存
// 映射等多种能力时，调用方必须通过 namespace 分别打开多个不可克隆
// 资源，再由稳定文件身份协调器统一执行冲突矩阵。这避免一个公开
// 资源在内部偷偷拥有读取、追加、覆盖和读写映射等多个权限不同的
// 原生文件句柄。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};

// 一个公开文件资源在其生命周期内唯一获准的访问角色。
//
// # 作用与使用位置
//
// 本类型会按值进入普通或协调式文件创建/打开方法。成功返回的
// [`crate::FileIo`] 资源必须保存所选角色，并在任何底层 I/O、映射、锁或
// 文件内容副作用之前拒绝角色不允许的方法。adapter 还必须在打开阶段验证
// 后端和操作系统能够支持该角色，不能返回一个已知所有核心操作都会失败的
// 伪资源。
//
// 本类型只表达调用方请求的资源角色，不等同于以后用于查询后端支持范围的
// capability 集合，也不改变同文件操作协调规则。资源空闲存活本身不取得
// 读取、追加、覆盖、映射或截断的操作租约；只有实际方法调用才参加对应
// 互斥关系。
//
// # 单一角色原则
//
// 首版不支持任意 `READ | APPEND | OVERWRITE | MMAP_WRITE` 位组合。多个角色必须由多次
// 独立打开表达：例如一个 [`Self::ReadMmap`] 资源建立只读映射，另一个
// [`Self::Append`] 资源使用独立 `O_APPEND` 或平台等价句柄追加。二者可以
// 共享同一稳定身份协调核心，但不能共享或克隆同一个底层打开状态。
//
// 这种分离使每个资源只申请最小权限，并避免顺序读取游标受到追加句柄文件
// 位置变化影响。远端 adapter 也可以在打开时明确拒绝不具备可靠语义的角色，
// 而不必假装支持本地原生权限组合。
//
// # `Truncate`、覆盖写与内容写入边界
//
// [`Self::Truncate`] 只授予以后显式调用缩短型 `FileIo::truncate` 的能力；
// 使用该角色创建或打开资源时不得自动把已有文件清空。它刻意不同于
// [`std::fs::OpenOptions::truncate`](https://doc.rust-lang.org/nightly/std/fs/struct.OpenOptions.html#method.truncate)
// 的“成功打开已有文件时立即把长度设为零”语义。
//
// 截断方法不会接受 buffer，也不会写入调用方指定字节。新长度大于调用时
// 在排他协调租约内观察到的当前长度时必须失败，不能像
// [`std::fs::File::set_len`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.set_len)
// 那样扩展文件。
//
// [`Self::Overwrite`] 是另一个独立角色：它向以后单独冻结的全文件覆盖方法
// 提供能力，成功时使文件内容和长度与输入 buffer 完全一致。它不提供
// 任意偏移覆盖，也不等于“打开时立即清空”。因此，当前普通内容写入面
// 只包含严格尾部追加、全文件原地覆盖，以及可写映射句柄内的范围修改。
//
// # 基本能力与演进
//
// 本类型刻意不实现 `Clone`、`Copy` 或 `Default`。打开方法按值取得一个
// 明确角色，不存在能够替调用方安全猜测的默认访问能力。无字段变体可在需要
// 时重新写出，不需要复制任何资源。
//
// 判等、全序和哈希只比较角色标签，便于诊断、索引和确定性排序；声明顺序
// 不表达权限包含或安全强度关系，哈希值也不是跨版本编码。`Send` 与 `Sync`
// 由无字段枚举自动获得，不使用手写 `unsafe impl`。
//
// 枚举保持封闭，不使用 `#[non_exhaustive]`，不声明稳定 ABI、整数判别值、
// 序列化格式或 `FromStr` 协议。新增普通内容写入角色会改变当前写入面，必须
// 重新进入正式设计冻结流程，不能作为兼容性扩展悄悄加入。
/// 一个公开文件资源在其整个生命周期内唯一获准的访问角色。
///
/// 角色按值传入创建或打开方法，成功资源只能调用相应能力。一个资源不组合
/// 多个角色；需要不同能力时必须分别打开独立资源。角色不表示跨进程协调
/// 等级，也不会仅因资源存活就执行内容操作。无默认角色，枚举顺序不表达
/// 权限包含或安全强度。
pub enum FileAccessMode {
    // 普通文件内容读取角色。
    //
    // 允许查询长度，以及随机、精确、增长式和顺序读取。不允许严格追加、
    // 全文件覆盖、建立任何内存映射、截断或普通文件强刷新。只读映射必须使用
    // 独立的 [`Self::ReadMmap`] 或 [`Self::ReadWriteMmap`] 资源。
    /// 普通内容读取。
    ///
    /// 允许查询长度以及随机、精确、增长式和顺序读取；不允许写入、截断、
    /// 映射创建或普通文件强刷新。
    Read,

    // 严格文件尾部追加角色。
    //
    // 允许查询长度、严格追加和普通文件强刷新。本地 adapter 必须通过一次
    // 独立的 append 模式打开获得底层句柄，不能使用共享游标、定位到尾部、
    // `dup`、`try_clone` 或等价克隆模拟。其它普通读取、全文件覆盖、映射和
    // 截断均拒绝。
    /// 严格文件尾部追加。
    ///
    /// 允许查询长度、严格追加和普通文件强刷新；不允许普通读取、覆盖、截断
    /// 或映射创建。追加不是“先定位到尾部再写入”。
    Append,

    // 全文件原地覆盖角色。
    //
    // 允许查询长度、调用以后单独冻结的全文件覆盖方法，以及对已完成的
    // 覆盖建立普通文件强刷新屏障。创建或打开本角色资源本身不修改文件；
    // 只有实际覆盖调用才取得排他操作租约并产生内容副作用。
    //
    // 成功返回时，目标文件的全部逻辑字节必须与输入 buffer 完全一致，文件
    // 长度也必须等于 buffer 长度；空 buffer 因而会将文件变为空文件，不得留下
    // 原文件的尾部。本语义不承诺故障原子性：失败或取消后，目标可能未变、
    // 已清空、只含新前缀、已全部覆盖但完成步骤失败，或处于后端无法证明的
    // 状态。调用方必须使用覆盖失败结果中的目标状态证据决定恢复策略，
    // 不得因为取回原 buffer 就盲目重试。
    //
    // 一次覆盖调用与普通读取、追加、截断、其它覆盖、任何已有或新建映射，
    // 以及安全删除和以后可能加入的替换自动互斥。默认 `rename` 不取得内容
    // 租约；平台因占用状态拒绝改名时直接返回明确错误。普通读取、严格追加、
    // 缩短型截断和映射创建均不属于本角色。
    /// 全文件原地覆盖。
    ///
    /// 允许查询长度、使完整内容与输入一致，以及普通文件强刷新。打开资源
    /// 本身不修改内容。失败或取消后目标可能部分改变，调用方必须依据
    /// [`crate::OverwriteFailure`] 的证据恢复。
    Overwrite,

    // 只读内存映射创建角色。
    //
    // 允许查询长度和创建 [`crate::ReadMmapHandle`]。映射成功后的内容访问只
    // 能经过透明句柄；普通读取、追加、全文件覆盖、可写映射、截断和普通文件
    // 强刷新均拒绝。
    /// 只读内存映射创建。
    ///
    /// 允许查询长度并创建只读映射；内容只能通过返回的透明句柄访问。
    ReadMmap,

    // 只读或读写内存映射创建角色。
    //
    // 允许查询长度，并可分别创建 [`crate::ReadMmapHandle`] 或
    // [`crate::ReadWriteMmapHandle`]，并允许文件资源强刷新。不允许普通读取、
    // 严格追加、全文件覆盖或截断；映射脏页的完成证据仍由持有 guard 的
    // 可写映射句柄刷新提供。
    /// 只读或读写内存映射创建。
    ///
    /// 允许查询长度、创建只读或读写映射，以及普通文件强刷新。普通内容
    /// 读写和截断均不允许；文件刷新不替代可写映射句柄的脏页刷新保证。
    ReadWriteMmap,

    // 显式缩短文件逻辑长度的管理角色。
    //
    // 允许查询长度、调用以后单独冻结的缩短型 `FileIo::truncate`，以及为已
    // 完成的长度变更建立普通文件强刷新屏障。创建或打开本角色资源本身没有
    // 截断副作用；普通读取、追加、全文件覆盖和任何映射创建均拒绝。截断不得
    // 扩展文件，
    // 也不等价于接收新内容的覆盖写。
    /// 显式缩短文件逻辑长度。
    ///
    /// 允许查询长度、缩短文件和普通文件强刷新。打开资源本身不改变长度；
    /// 截断不能扩展文件，也不接收新内容。
    Truncate,
}

impl FileAccessMode {
    fn rank(&self) -> u8 {
        match self {
            Self::Read => 0,
            Self::Append => 1,
            Self::Overwrite => 2,
            Self::ReadMmap => 3,
            Self::ReadWriteMmap => 4,
            Self::Truncate => 5,
        }
    }

    fn debug_name(&self) -> &'static str {
        match self {
            Self::Read => "Read",
            Self::Append => "Append",
            Self::Overwrite => "Overwrite",
            Self::ReadMmap => "ReadMmap",
            Self::ReadWriteMmap => "ReadWriteMmap",
            Self::Truncate => "Truncate",
        }
    }

    fn display_name(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::ReadMmap => "read-mmap",
            Self::ReadWriteMmap => "read-write-mmap",
            Self::Truncate => "truncate",
        }
    }
}

impl fmt::Debug for FileAccessMode {
    // 以 Rust 变体名显示资源角色，不查询文件或后端状态。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.debug_name())
    }
}

impl fmt::Display for FileAccessMode {
    // 输出稳定的人类可读角色标签，不承诺可反向解析或跨版本序列化。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.display_name())
    }
}

impl PartialEq for FileAccessMode {
    // 仅在两个值表示相同资源角色时判等。
    fn eq(&self, other: &Self) -> bool {
        self.rank() == other.rank()
    }
}

impl Eq for FileAccessMode {}

impl PartialOrd for FileAccessMode {
    // 返回与 [`Ord`] 一致的角色标签全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileAccessMode {
    // 按声明顺序比较角色标签，不表达权限包含或安全强度关系。
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl Hash for FileAccessMode {
    // 按与判等一致的角色标签向调用方哈希器写入状态。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.rank().hash(state);
    }
}
