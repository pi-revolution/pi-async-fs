//! 递归目录遍历配置与条目值类型的纯契约测试。
//!
//! 覆盖深度上限的零／有限／最大／无限边界和后代条目所有权；真实流、遍历
//! 顺序、符号链接和懒执行由 `tests/local_walk.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::ffi::OsString;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

use pi_async_fs::{
    DirectoryEntry, EntryName, FileType, WalkDepthLimit, WalkEntry, WalkOptions,
};

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

fn hash_of<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

// 验证深度上限可区分 0、1、`usize::MAX` 与 Unlimited，并保护 getter、
// Clone、排序、Hash 和诊断一致；0 表示不产生后代而非参数错误。
#[test]
fn test_walk_depth_limit_covers_zero_finite_maximum_and_unlimited() {
    assert_send_sync_static::<WalkDepthLimit>();

    let zero = WalkDepthLimit::Limited { max_depth: 0 };
    let one = WalkDepthLimit::Limited { max_depth: 1 };
    let maximum = WalkDepthLimit::Limited {
        max_depth: usize::MAX,
    };
    let unlimited = WalkDepthLimit::Unlimited;

    assert_eq!(zero.maximum_depth(), Some(0));
    assert_eq!(one.maximum_depth(), Some(1));
    assert_eq!(maximum.maximum_depth(), Some(usize::MAX));
    assert_eq!(unlimited.maximum_depth(), None);
    assert!(zero < one);
    assert!(one < maximum);
    assert!(maximum < unlimited);
    assert_eq!(zero, zero.clone());
    assert_eq!(unlimited, unlimited.clone());
    assert_eq!(hash_of(&zero), hash_of(&zero.clone()));
    assert_ne!(hash_of(&maximum), hash_of(&unlimited));
    assert_eq!(format!("{zero:?}"), "Limited { max_depth: 0 }");
    assert_eq!(format!("{unlimited:?}"), "Unlimited");
    assert_eq!(zero.to_string(), "maximum depth: 0");
    assert_eq!(unlimited.to_string(), "unlimited depth");
}

// 验证 `WalkOptions::new` 原样保留显式有限／无限选择，不偷偷套用默认深度，
// 同时保持 Clone、排序、Hash 与格式化一致。
#[test]
fn test_walk_options_preserve_explicit_depth_without_defaulting() {
    assert_send_sync_static::<WalkOptions>();

    let limited = WalkOptions::new(WalkDepthLimit::Limited { max_depth: 3 });
    let unlimited = WalkOptions::new(WalkDepthLimit::Unlimited);
    let cloned = limited.clone();

    assert_eq!(limited, cloned);
    assert!(limited < unlimited);
    assert_eq!(hash_of(&limited), hash_of(&cloned));
    assert_eq!(
        format!("{limited:?}"),
        "WalkOptions { depth_limit: Limited { max_depth: 3 } }",
    );
    assert_eq!(limited.to_string(), "depth limit: maximum depth: 3");
    assert_eq!(unlimited.to_string(), "depth limit: unlimited depth");
}

// 验证 `WalkEntry` 拥有完整目录条目且深度必须为非零后代深度，Clone、排序、
// Hash、格式化和 `into_parts` 均保留两项资产。
#[test]
fn test_walk_entry_owns_directory_entry_and_nonzero_depth() {
    assert_send_sync_static::<WalkEntry<String>>();

    let entry = DirectoryEntry::new(
        EntryName::from(OsString::from("child")),
        String::from("root/child"),
        Some(FileType::Directory),
    );
    let depth = NonZeroUsize::new(1).expect("一是合法后代深度");
    let item = WalkEntry::new(entry, depth);
    let cloned = item.clone();

    assert_eq!(item, cloned);
    assert_eq!(hash_of(&item), hash_of(&cloned));
    assert_eq!(
        format!("{item:?}"),
        "WalkEntry { entry: DirectoryEntry { name: Native(\"child\"), \
         locator: \"root/child\", file_type_hint: Some(Directory) }, depth: 1 }",
    );
    assert_eq!(
        item.to_string(),
        "DirectoryEntry { name: Native(\"child\"), locator: \"root/child\", \
         file_type_hint: Some(Directory) } at depth 1",
    );

    let deeper = WalkEntry::new(
        cloned.entry.clone(),
        NonZeroUsize::new(2).expect("二是合法后代深度"),
    );
    assert!(cloned < deeper);

    let (returned_entry, returned_depth) = item.into_parts();
    assert_eq!(returned_entry.name.to_string(), "child");
    assert_eq!(returned_entry.locator, "root/child");
    assert_eq!(returned_entry.file_type_hint, Some(FileType::Directory));
    assert_eq!(returned_depth, depth);
}
