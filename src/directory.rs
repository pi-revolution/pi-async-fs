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
#[non_exhaustive]
pub enum EntryName {
    /// 操作系统原生目录项名称。
    ///
    /// `OsString` 无损保留当前平台表示，包括 Unix 非 UTF-8 名称。
    Native(OsString),

    /// 远端后端提供的 UTF-8 逻辑子项名称。
    ///
    /// 该值不是远端对象的完整 key。
    Remote(Atom),
}

impl Clone for EntryName {
    fn clone(&self) -> Self {
        todo!("第一批次仅声明 Clone 接口")
    }
}

impl fmt::Debug for EntryName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明 Debug 接口")
    }
}

impl fmt::Display for EntryName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明 Display 接口")
    }
}

impl PartialEq for EntryName {
    fn eq(&self, other: &Self) -> bool {
        todo!("第一批次仅声明 PartialEq 接口")
    }
}

impl Eq for EntryName {}

impl PartialOrd for EntryName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("第一批次仅声明 PartialOrd 接口")
    }
}

impl Ord for EntryName {
    fn cmp(&self, other: &Self) -> Ordering {
        todo!("第一批次仅声明 Ord 接口")
    }
}

impl Hash for EntryName {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        todo!("第一批次仅声明 Hash 接口")
    }
}

impl From<OsString> for EntryName {
    fn from(name: OsString) -> Self {
        todo!("第一批次仅声明 From<OsString> 接口")
    }
}

impl From<&OsStr> for EntryName {
    fn from(name: &OsStr) -> Self {
        todo!("第一批次仅声明 From<&OsStr> 接口")
    }
}

impl From<Atom> for EntryName {
    fn from(name: Atom) -> Self {
        todo!("第一批次仅声明 From<Atom> 接口")
    }
}

/// 一次浅层目录枚举产生的轻量、拥有型子项结果。
///
/// `L` 通常是产生该结果的 `FileNamespace::Locator`。调用方可以把 `locator`
/// 显式交回同一个逻辑 namespace 执行后续操作，但不能把本值当作已打开
/// 文件、稳定文件身份或能够自行访问后端的活动对象。
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
        todo!("第一批次仅声明目录项构造接口")
    }
}

impl<L> Clone for DirectoryEntry<L>
where
    L: Clone,
{
    fn clone(&self) -> Self {
        todo!("第一批次仅声明 Clone 接口")
    }
}

impl<L> fmt::Debug for DirectoryEntry<L>
where
    L: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明 Debug 接口")
    }
}

impl<L> fmt::Display for DirectoryEntry<L>
where
    L: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!("第一批次仅声明 Display 接口")
    }
}

impl<L> PartialEq for DirectoryEntry<L>
where
    L: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        todo!("第一批次仅声明 PartialEq 接口")
    }
}

impl<L> Eq for DirectoryEntry<L> where L: Eq {}

impl<L> PartialOrd for DirectoryEntry<L>
where
    L: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        todo!("第一批次仅声明 PartialOrd 接口")
    }
}

impl<L> Ord for DirectoryEntry<L>
where
    L: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        todo!("第一批次仅声明 Ord 接口")
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
        todo!("第一批次仅声明 Hash 接口")
    }
}
