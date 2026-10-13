//! 不支持文件独占能力时使用的公开占位类型。
//!
//! 本类型没有可构造的值，不持有资源，也不表示已经取得独占资格。

/// 不支持独占能力的命名空间所使用的、不可构造的所有者类型。
///
/// 这是[`crate::FileNamespace::ExclusiveOwner`]的默认关联类型。保留默认值的
/// 实现可以让独占方法返回[`pi_result::ErrorKind::Unsupported`]，不需要伪造
/// 一个可用所有者。该类型没有变体、公开构造、`Default`或资源副作用。
/// 自动满足`Send + Sync + 'static`；`Debug`与`Display`不提供构造或解析能力。
///
/// ```compile_fail
/// use pi_async_fs::UnsupportedExclusiveFileOwner;
/// let _ = UnsupportedExclusiveFileOwner {};
/// ```
///
/// ```compile_fail,E0277
/// use pi_async_fs::UnsupportedExclusiveFileOwner;
/// fn requires_default<T: Default>() {}
/// requires_default::<UnsupportedExclusiveFileOwner>();
/// ```
#[derive(Debug)]
pub enum UnsupportedExclusiveFileOwner {}

impl core::fmt::Display for UnsupportedExclusiveFileOwner {
    fn fmt(&self, _formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match *self {}
    }
}
