//! `AvailableSpace` 公开值类型的契约测试。
//!
//! 本文件只验证无 I/O 的构造、完整 `u64` 边界、格式化以及值语义；真实路径、
//! 分区／挂载点查询和错误分类由 `tests/local_available_space.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pi_async_fs::AvailableSpace;

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 用标准哈希器验证相等公开值必须产生一致哈希。
fn hash_of(value: &AvailableSpace) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

// 验证 `AvailableSpace::new` 与 `available_bytes` 对 0、普通值和 `u64::MAX`
// 均无截断，并同时保护 `Send + Sync + 'static` 及不混淆单位的诊断格式。
#[test]
fn test_available_space_preserves_all_u64_values_and_formats_units() {
    assert_send_sync_static::<AvailableSpace>();

    let zero = AvailableSpace::new(0);
    let ordinary = AvailableSpace::new(42);
    let maximum = AvailableSpace::new(u64::MAX);

    assert_eq!(zero.available_bytes(), 0);
    assert_eq!(ordinary.available_bytes(), 42);
    assert_eq!(maximum.available_bytes(), u64::MAX);
    assert_eq!(format!("{ordinary:?}"), "AvailableSpace { available_bytes: 42 }");
    assert_eq!(format!("{ordinary}"), "42 bytes available");
}

// 验证 Clone、判等、全序和 Hash 只由可用字节数字决定；该纯值测试不声称
// 覆盖任何真实文件系统空间变化、缓存时效或平台查询行为。
#[test]
fn test_available_space_clone_order_equality_and_hash_use_only_bytes() {
    let lower = AvailableSpace::new(41);
    let value = AvailableSpace::new(42);
    let cloned = value.clone();
    let higher = AvailableSpace::new(43);

    assert_eq!(value, cloned);
    assert_ne!(value, lower);
    assert!(lower < value);
    assert!(value < higher);
    assert_eq!(value.partial_cmp(&cloned), Some(std::cmp::Ordering::Equal));
    assert_eq!(hash_of(&value), hash_of(&cloned));
}
