//! 目录枚举结果模型。
//!
//! 本模块中的值是被动、拥有型结果。它们不保存 namespace、打开目录句柄、
//! future、缓存或异步加载器；完整元信息必须显式交回产生定位符的
//! `FileNamespace` 查询。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use std::ffi::{OsStr, OsString};

use pi_atom::Atom;

use crate::FileType;

/// 浅层目录枚举结果中的单个直接子项名称。
///
/// 名称不是完整路径、对象 key、URL 或定位符，不能单独用于打开、删除、
/// 改名、文件内容 I/O 或 MMAP。只有由合法 namespace 返回的值才具有“单个
/// 直接子项”的生产者保证；生产者必须排除 `.`、`..`、路径分隔符和虚拟
/// 目录逃逸。
///
/// # 跨线程安全前置条件
///
/// 包含 [`Self::Remote`] 的值只有在最终依赖图未为 `pi_atom` 使用的同一份
/// `pi_share` 启用 `rc` feature 时，才位于本 crate 支持的跨线程安全域。完整
/// 审计义务见 crate 级文档的“跨线程安全的依赖图前置条件”章节。
#[non_exhaustive]
pub enum EntryName {
    /// 操作系统原生目录项名称。
    ///
    /// `OsString` 无损保留当前平台表示，包括 Unix 非 UTF-8 名称。
    Native(OsString),

    /// 远端后端提供的 UTF-8 逻辑子项名称。
    ///
    /// 该值不是远端对象的完整 key；其跨线程使用继承 [`EntryName`] 的最终依赖
    /// 图前置条件。
    Remote(Atom),
}

impl Clone for EntryName {
    fn clone(&self) -> Self {
        match self {
            Self::Native(name) => Self::Native(name.clone()),
            Self::Remote(name) => Self::Remote(name.clone()),
        }
    }
}

impl fmt::Debug for EntryName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(name) => formatter
                .debug_tuple("Native")
                .field(name)
                .finish(),
            Self::Remote(name) => formatter
                .debug_tuple("Remote")
                .field(&name.as_str())
                .finish(),
        }
    }
}

impl fmt::Display for EntryName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(name) => formatter.write_str(&name.to_string_lossy()),
            Self::Remote(name) => formatter.write_str(name.as_str()),
        }
    }
}

impl PartialEq for EntryName {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Native(left), Self::Native(right)) => left == right,
            (Self::Remote(left), Self::Remote(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for EntryName {}

impl PartialOrd for EntryName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryName {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Native(left), Self::Native(right)) => left.cmp(right),
            (Self::Native(_), Self::Remote(_)) => Ordering::Less,
            (Self::Remote(_), Self::Native(_)) => Ordering::Greater,
            (Self::Remote(left), Self::Remote(right)) => left.cmp(right),
        }
    }
}

impl Hash for EntryName {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::Native(name) => {
                0_u8.hash(state);
                name.hash(state);
            }
            Self::Remote(name) => {
                1_u8.hash(state);
                name.hash(state);
            }
        }
    }
}

impl From<OsString> for EntryName {
    fn from(name: OsString) -> Self {
        Self::Native(name)
    }
}

impl From<&OsStr> for EntryName {
    fn from(name: &OsStr) -> Self {
        Self::Native(name.to_os_string())
    }
}

impl From<Atom> for EntryName {
    fn from(name: Atom) -> Self {
        Self::Remote(name)
    }
}

/// 一次浅层目录枚举产生的轻量、拥有型子项结果。
///
/// `L` 通常是产生该结果的 `FileNamespace::Locator`。调用方可以把 `locator`
/// 显式交回同一个逻辑 namespace 执行后续操作，但不能把本值当作已打开
/// 文件、稳定文件身份或能够自行访问后端的活动对象。包含远端 [`EntryName`]
/// 时，本值继承其跨线程安全依赖图前置条件。
#[non_exhaustive]
pub struct DirectoryEntry<L> {
    /// 相对于被枚举目录的单个直接子项名称。
    pub name: EntryName,
    /// 可交回同一 namespace 的完整资源位置。
    pub locator: L,
    /// 枚举过程中无需额外查询即可取得的可选类型提示。
    ///
    /// `None` 表示没有免费取得；`Some(FileType::Unknown)` 表示后端确实给出
    /// 了分类结果，但仍无法确定具体类型。提示可能在后续查询前已经过期。
    pub file_type_hint: Option<FileType>,
}

impl<L> DirectoryEntry<L> {
    /// 构造目录枚举结果。
    ///
    /// 参数顺序与公开字段一致。生产者必须保证 `name` 与 `locator` 指向本次
    /// 枚举的同一个直接子项；`file_type_hint` 只能表示枚举时已经获知的提示。
    #[must_use]
    pub fn new(name: EntryName, locator: L, file_type_hint: Option<FileType>) -> Self {
        Self {
            name,
            locator,
            file_type_hint,
        }
    }
}

impl<L> Clone for DirectoryEntry<L>
where
    L: Clone,
{
    fn clone(&self) -> Self {
        Self::new(
            self.name.clone(),
            self.locator.clone(),
            self.file_type_hint.clone(),
        )
    }
}

impl<L> fmt::Debug for DirectoryEntry<L>
where
    L: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DirectoryEntry")
            .field("name", &self.name)
            .field("locator", &self.locator)
            .field("file_type_hint", &self.file_type_hint)
            .finish()
    }
}

impl<L> fmt::Display for DirectoryEntry<L>
where
    L: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} at {}", self.name, self.locator)?;
        match &self.file_type_hint {
            Some(file_type) => write!(formatter, ", type hint: {file_type}"),
            None => formatter.write_str(", type hint: unavailable"),
        }
    }
}

impl<L> PartialEq for DirectoryEntry<L>
where
    L: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.locator == other.locator
            && self.file_type_hint == other.file_type_hint
    }
}

impl<L> Eq for DirectoryEntry<L> where L: Eq {}

impl<L> PartialOrd for DirectoryEntry<L>
where
    L: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.name.cmp(&other.name) {
            Ordering::Equal => {}
            ordering => return Some(ordering),
        }
        match self.locator.partial_cmp(&other.locator)? {
            Ordering::Equal => self.file_type_hint.partial_cmp(&other.file_type_hint),
            ordering => Some(ordering),
        }
    }
}

impl<L> Ord for DirectoryEntry<L>
where
    L: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.name
            .cmp(&other.name)
            .then_with(|| self.locator.cmp(&other.locator))
            .then_with(|| self.file_type_hint.cmp(&other.file_type_hint))
    }
}

impl<L> Hash for DirectoryEntry<L>
where
    L: Hash,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.name.hash(state);
        self.locator.hash(state);
        self.file_type_hint.hash(state);
    }
}
