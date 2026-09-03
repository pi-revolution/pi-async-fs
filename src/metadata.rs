//! 可移植文件元信息模型。
//!
//! 本模块保存一次查询得到的纯数据快照，不保存 namespace、打开句柄、缓存
//! 或异步加载器。重复查询可能观察到不同状态；任何值都不是锁、事务或
//! 后续不变保证。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use chrono::{DateTime, SecondsFormat, Utc};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// 文件系统对象的可移植种类。
///
/// 该分类由目录枚举或元信息查询产生。它不表示调用方权限、可读写性、
/// MMAP 能力或稳定文件身份。新增平台或后端种类时可能扩展变体，外部匹配
/// 必须保留兜底分支。
#[non_exhaustive]
pub enum FileType {
    /// 操作系统命名空间中的常规文件。
    RegularFile,
    /// 真实目录，或远端后端明确提供目录语义的对象。
    Directory,
    /// 未跟随查询所观察到的符号链接本身。
    SymbolicLink,
    /// 不具有目录语义的远端对象。
    RemoteObject,
    /// 平台可靠识别的块设备。
    BlockDevice,
    /// 平台可靠识别的字符设备。
    CharacterDevice,
    /// 命名管道或 FIFO。
    Fifo,
    /// 文件系统命名空间中的套接字对象。
    Socket,
    /// 对象存在且已经分类，但无法映射到现有公开变体。
    Other,
    /// 查询成功，但后端无法可靠确定对象种类。
    Unknown,
}

impl FileType {
    fn rank(&self) -> u8 {
        match self {
            Self::RegularFile => 0,
            Self::Directory => 1,
            Self::SymbolicLink => 2,
            Self::RemoteObject => 3,
            Self::BlockDevice => 4,
            Self::CharacterDevice => 5,
            Self::Fifo => 6,
            Self::Socket => 7,
            Self::Other => 8,
            Self::Unknown => 9,
        }
    }

    fn debug_name(&self) -> &'static str {
        match self {
            Self::RegularFile => "RegularFile",
            Self::Directory => "Directory",
            Self::SymbolicLink => "SymbolicLink",
            Self::RemoteObject => "RemoteObject",
            Self::BlockDevice => "BlockDevice",
            Self::CharacterDevice => "CharacterDevice",
            Self::Fifo => "Fifo",
            Self::Socket => "Socket",
            Self::Other => "Other",
            Self::Unknown => "Unknown",
        }
    }

    fn display_name(&self) -> &'static str {
        match self {
            Self::RegularFile => "regular file",
            Self::Directory => "directory",
            Self::SymbolicLink => "symbolic link",
            Self::RemoteObject => "remote object",
            Self::BlockDevice => "block device",
            Self::CharacterDevice => "character device",
            Self::Fifo => "fifo",
            Self::Socket => "socket",
            Self::Other => "other",
            Self::Unknown => "unknown",
        }
    }
}

impl Clone for FileType {
    fn clone(&self) -> Self {
        match self {
            Self::RegularFile => Self::RegularFile,
            Self::Directory => Self::Directory,
            Self::SymbolicLink => Self::SymbolicLink,
            Self::RemoteObject => Self::RemoteObject,
            Self::BlockDevice => Self::BlockDevice,
            Self::CharacterDevice => Self::CharacterDevice,
            Self::Fifo => Self::Fifo,
            Self::Socket => Self::Socket,
            Self::Other => Self::Other,
            Self::Unknown => Self::Unknown,
        }
    }
}

impl fmt::Debug for FileType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.debug_name())
    }
}

impl fmt::Display for FileType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.display_name())
    }
}

impl PartialEq for FileType {
    fn eq(&self, other: &Self) -> bool {
        self.rank() == other.rank()
    }
}

impl Eq for FileType {}

impl PartialOrd for FileType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.rank().cmp(&other.rank())
    }
}

impl Hash for FileType {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.rank().hash(state);
    }
}

/// 可跨平台表达的粗粒度权限快照。
///
/// 本类型不是访问控制能力。`read_only = Some(false)` 不能证明当前调用方
/// 拥有写权限，任何字段也不表达 Unix 所有者/组、完整 mode、Windows ACL、
/// 远端 IAM、共享打开模式或 MMAP 权限。
pub struct PortablePermissions {
    /// 后端报告的只读标记；`None` 表示无法可靠提供。
    pub read_only: Option<bool>,
    /// 后端报告的可执行标记；`None` 表示无法可靠提供。
    ///
    /// `Some(true)` 表示来源明确报告对象可执行；无法可靠映射该语义时返回
    /// `None`，不能以 `false` 代替未知。
    pub executable: Option<bool>,
}

fn permission_state(value: Option<bool>) -> &'static str {
    match value {
        None => "unknown",
        Some(false) => "no",
        Some(true) => "yes",
    }
}

impl Clone for PortablePermissions {
    fn clone(&self) -> Self {
        Self {
            read_only: self.read_only,
            executable: self.executable,
        }
    }
}

impl fmt::Debug for PortablePermissions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PortablePermissions")
            .field("read_only", &self.read_only)
            .field("executable", &self.executable)
            .finish()
    }
}

impl fmt::Display for PortablePermissions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "read-only: {}, executable: {}",
            permission_state(self.read_only),
            permission_state(self.executable)
        )
    }
}

impl PartialEq for PortablePermissions {
    fn eq(&self, other: &Self) -> bool {
        self.read_only == other.read_only
            && self.executable == other.executable
    }
}

impl Eq for PortablePermissions {}

impl PartialOrd for PortablePermissions {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PortablePermissions {
    fn cmp(&self, other: &Self) -> Ordering {
        self.read_only
            .cmp(&other.read_only)
            .then_with(|| self.executable.cmp(&other.executable))
    }
}

impl Hash for PortablePermissions {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.read_only.hash(state);
        self.executable.hash(state);
    }
}

/// 文件元信息查询观察到的墙上时钟时间快照。
///
/// 这些时间不适合测量耗时或证明因果顺序。远端值可能来自服务器时钟，
/// 并受时钟回拨、偏差、延迟更新和文件系统精度影响。缺失信息使用 `None`，
/// 不得伪造为 Unix 纪元、零值或当前时间。
///
/// `Debug` 与 `Display` 以清晰日期时间格式显示可表达的时间；超出常用日期
/// 范围时使用带符号的 Unix 纪元偏移表示，且不得 panic。公共字段使用标准库
/// 类型。
pub struct FileTimes {
    /// 后端明确提供的创建时间或出生时间。
    ///
    /// Unix `ctime` 是元信息变化时间，不能填入本字段。
    pub created: Option<SystemTime>,
    /// 文件内容最后修改时间。
    pub modified: Option<SystemTime>,
    /// 最后访问时间。
    pub accessed: Option<SystemTime>,
    /// inode 或其它元信息状态最后变化时间。
    pub metadata_changed: Option<SystemTime>,
}

struct FormattedSystemTime<'a>(Option<&'a SystemTime>);

impl fmt::Debug for FormattedSystemTime<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for FormattedSystemTime<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(time) = self.0 else {
            return formatter.write_str("unknown");
        };

        match time.duration_since(UNIX_EPOCH) {
            Ok(duration) => {
                if let Ok(seconds) = i64::try_from(duration.as_secs()) {
                    if let Some(datetime) =
                        DateTime::<Utc>::from_timestamp(seconds, duration.subsec_nanos())
                    {
                        return formatter.write_str(
                            &datetime.to_rfc3339_opts(SecondsFormat::Nanos, true),
                        );
                    }
                }

                write!(
                    formatter,
                    "unix+{}.{:09}s",
                    duration.as_secs(),
                    duration.subsec_nanos()
                )
            }
            Err(error) => {
                let magnitude = error.duration();
                let nanoseconds = magnitude.subsec_nanos();
                let signed_seconds = if nanoseconds == 0 {
                    -i128::from(magnitude.as_secs())
                } else {
                    -i128::from(magnitude.as_secs()) - 1
                };
                let normalized_nanoseconds = if nanoseconds == 0 {
                    0
                } else {
                    1_000_000_000 - nanoseconds
                };

                if let Ok(seconds) = i64::try_from(signed_seconds) {
                    if let Some(datetime) =
                        DateTime::<Utc>::from_timestamp(seconds, normalized_nanoseconds)
                    {
                        return formatter.write_str(
                            &datetime.to_rfc3339_opts(SecondsFormat::Nanos, true),
                        );
                    }
                }

                write!(
                    formatter,
                    "unix-{}.{:09}s",
                    magnitude.as_secs(),
                    magnitude.subsec_nanos()
                )
            }
        }
    }
}

impl Clone for FileTimes {
    fn clone(&self) -> Self {
        Self {
            created: self.created,
            modified: self.modified,
            accessed: self.accessed,
            metadata_changed: self.metadata_changed,
        }
    }
}

impl fmt::Debug for FileTimes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileTimes")
            .field("created", &FormattedSystemTime(self.created.as_ref()))
            .field("modified", &FormattedSystemTime(self.modified.as_ref()))
            .field("accessed", &FormattedSystemTime(self.accessed.as_ref()))
            .field(
                "metadata_changed",
                &FormattedSystemTime(self.metadata_changed.as_ref()),
            )
            .finish()
    }
}

impl fmt::Display for FileTimes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "created: {}, modified: {}, accessed: {}, metadata changed: {}",
            FormattedSystemTime(self.created.as_ref()),
            FormattedSystemTime(self.modified.as_ref()),
            FormattedSystemTime(self.accessed.as_ref()),
            FormattedSystemTime(self.metadata_changed.as_ref())
        )
    }
}

impl PartialEq for FileTimes {
    fn eq(&self, other: &Self) -> bool {
        self.created == other.created
            && self.modified == other.modified
            && self.accessed == other.accessed
            && self.metadata_changed == other.metadata_changed
    }
}

impl Eq for FileTimes {}

impl PartialOrd for FileTimes {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FileTimes {
    fn cmp(&self, other: &Self) -> Ordering {
        self.created
            .cmp(&other.created)
            .then_with(|| self.modified.cmp(&other.modified))
            .then_with(|| self.accessed.cmp(&other.accessed))
            .then_with(|| self.metadata_changed.cmp(&other.metadata_changed))
    }
}

impl Hash for FileTimes {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.created.hash(state);
        self.modified.hash(state);
        self.accessed.hash(state);
        self.metadata_changed.hash(state);
    }
}

/// 某个 namespace 在特定资源位置观察到的版本。
///
/// `L` 通常是产生该值的 `FileNamespace::Locator`。版本令牌是后端定义的
/// 不透明字节：它可以承载 ETag、generation、UUID、内容摘要或其它精确
/// 表示，但本库不把它解释为字符串、数值或时间顺序。
///
/// # 适用边界
///
/// 相同 locator 与相同 token 只有在同一逻辑 namespace 实例及配置作用域内
/// 才能解释为同一资源版本。本类型不是稳定文件身份；跨 namespace 复用是
/// 非法调用，但不构成 Rust 未定义行为。
///
/// `Debug` 和 `Display` 只显示定位符与令牌字节数，不输出令牌内容；两种格式
/// 都不是稳定序列化格式。排序先比较定位符，再按令牌原始字节排序，仅供集合
/// 与确定性输出使用，不表示版本的新旧、因果或后端顺序。
pub struct ResourceVersion<L> {
    locator: L,
    token: Arc<[u8]>,
}

impl<L> ResourceVersion<L> {
    /// 组合资源位置和后端提供的原始版本令牌。
    ///
    /// 空 token 在类型层允许；只有后端确认其具有可靠条件语义时，才应把
    /// `ResourceVersion` 放入元信息。没有可靠版本时应返回 `None`。
    #[must_use]
    pub fn new(locator: L, token: Arc<[u8]>) -> Self {
        Self { locator, token }
    }

    /// 借用版本所属的资源位置。
    #[must_use]
    pub fn locator(&self) -> &L {
        &self.locator
    }

    /// 借用原始不透明版本令牌。
    ///
    /// 返回值可能不是 UTF-8；调用方不得假定其可读、可排序或可安全记录。
    #[must_use]
    pub fn token(&self) -> &[u8] {
        &self.token
    }

    /// 消费版本值并取回定位符和共享令牌所有权。
    #[must_use]
    pub fn into_parts(self) -> (L, Arc<[u8]>) {
        (self.locator, self.token)
    }
}

impl<L> Clone for ResourceVersion<L>
where
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            locator: self.locator.clone(),
            token: Arc::clone(&self.token),
        }
    }
}

impl<L> fmt::Debug for ResourceVersion<L>
where
    L: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceVersion")
            .field("locator", &self.locator)
            .field("token_len", &self.token.len())
            .finish()
    }
}

impl<L> fmt::Display for ResourceVersion<L>
where
    L: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} (opaque version token: {} bytes)",
            self.locator,
            self.token.len()
        )
    }
}

impl<L> PartialEq for ResourceVersion<L>
where
    L: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.locator == other.locator && self.token.as_ref() == other.token.as_ref()
    }
}

impl<L> Eq for ResourceVersion<L> where L: Eq {}

impl<L> PartialOrd for ResourceVersion<L>
where
    L: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.locator.partial_cmp(&other.locator) {
            Some(Ordering::Equal) => self.token.as_ref().partial_cmp(other.token.as_ref()),
            ordering => ordering,
        }
    }
}

impl<L> Ord for ResourceVersion<L>
where
    L: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.locator
            .cmp(&other.locator)
            .then_with(|| self.token.as_ref().cmp(other.token.as_ref()))
    }
}

impl<L> Hash for ResourceVersion<L>
where
    L: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.locator.hash(state);
        self.token.as_ref().hash(state);
    }
}

/// 一次文件或远端对象元信息查询得到的可移植快照。
///
/// `L` 与产生该结果的 `FileNamespace::Locator` 相同。结构体不重复保存查询
/// locator；只有版本令牌需要防止跨资源误用时，[`ResourceVersion`] 才拥有
/// locator。重复查询可能观察到删除、替换或其它并发变化。
///
/// `Debug` 与 `Display` 保留未知字段的未知状态，并继承版本令牌的脱敏规则；
/// 两种输出都不是稳定序列化格式。排序按公开字段声明顺序进行，仅供集合与
/// 确定性输出使用，不表示对象种类、时间或版本之间的业务优先级。
#[non_exhaustive]
pub struct FileMetadata<L> {
    /// 查询实际观察到的对象种类。
    pub file_type: FileType,
    /// 可可靠提供的内容字节长度。
    ///
    /// `Some(0)` 表示已确认长度为零；目录、链接本身、特殊对象或未知长度
    /// 使用 `None`。该快照不保证后续读取时长度仍相同。
    pub byte_len: Option<u64>,
    /// 可移植的粗粒度权限快照。
    pub permissions: PortablePermissions,
    /// 后端能够可靠提供的时间快照。
    pub times: FileTimes,
    /// 与本次查询资源绑定的可选版本令牌。
    pub version: Option<ResourceVersion<L>>,
}

impl<L> FileMetadata<L> {
    /// 构造元信息结果。
    ///
    /// 参数顺序与公开字段顺序一致。构造者必须保证 `version` 来自本次查询
    /// 的同一 locator；构造器不执行外部查询来验证这一条件。
    #[must_use]
    pub fn new(
        file_type: FileType,
        byte_len: Option<u64>,
        permissions: PortablePermissions,
        times: FileTimes,
        version: Option<ResourceVersion<L>>,
    ) -> Self {
        Self {
            file_type,
            byte_len,
            permissions,
            times,
            version,
        }
    }
}

impl<L> Clone for FileMetadata<L>
where
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            file_type: self.file_type.clone(),
            byte_len: self.byte_len,
            permissions: self.permissions.clone(),
            times: self.times.clone(),
            version: self.version.clone(),
        }
    }
}

impl<L> fmt::Debug for FileMetadata<L>
where
    L: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FileMetadata")
            .field("file_type", &self.file_type)
            .field("byte_len", &self.byte_len)
            .field("permissions", &self.permissions)
            .field("times", &self.times)
            .field("version", &self.version)
            .finish()
    }
}

impl<L> fmt::Display for FileMetadata<L>
where
    L: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "type: {}, byte length: ",
            self.file_type
        )?;
        match self.byte_len {
            Some(byte_len) => write!(formatter, "{byte_len} bytes")?,
            None => formatter.write_str("unknown")?,
        }
        write!(
            formatter,
            ", permissions: {}, times: {}, version: ",
            self.permissions,
            self.times
        )?;
        match &self.version {
            Some(version) => fmt::Display::fmt(version, formatter),
            None => formatter.write_str("none"),
        }
    }
}

impl<L> PartialEq for FileMetadata<L>
where
    L: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.file_type == other.file_type
            && self.byte_len == other.byte_len
            && self.permissions == other.permissions
            && self.times == other.times
            && self.version == other.version
    }
}

impl<L> Eq for FileMetadata<L> where L: Eq {}

impl<L> PartialOrd for FileMetadata<L>
where
    L: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let prefix_ordering = self
            .file_type
            .cmp(&other.file_type)
            .then_with(|| self.byte_len.cmp(&other.byte_len))
            .then_with(|| self.permissions.cmp(&other.permissions))
            .then_with(|| self.times.cmp(&other.times));
        match prefix_ordering {
            Ordering::Equal => self.version.partial_cmp(&other.version),
            ordering => Some(ordering),
        }
    }
}

impl<L> Ord for FileMetadata<L>
where
    L: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.file_type
            .cmp(&other.file_type)
            .then_with(|| self.byte_len.cmp(&other.byte_len))
            .then_with(|| self.permissions.cmp(&other.permissions))
            .then_with(|| self.times.cmp(&other.times))
            .then_with(|| self.version.cmp(&other.version))
    }
}

impl<L> Hash for FileMetadata<L>
where
    L: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.file_type.hash(state);
        self.byte_len.hash(state);
        self.permissions.hash(state);
        self.times.hash(state);
        self.version.hash(state);
    }
}
