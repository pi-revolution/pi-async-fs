//! 目录条目名称与目录条目快照的公开值语义测试。
//!
//! 本文件覆盖原生／远端名称、Unix 非 UTF-8 字节、字段排序和诊断；真实目录
//! 异步流及延迟元信息边界由 `tests/local_read_dir.rs` 覆盖。

use std::collections::hash_map::DefaultHasher;
use std::ffi::{OsStr, OsString};
use std::hash::{Hash, Hasher};

use pi_async_fs::{DirectoryEntry, EntryName, FileType};
use pi_atom::Atom;

fn assert_send_sync_static<T: Send + Sync + 'static>() {}

fn hash_of<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

// 验证相同文本的原生名称与远端名称仍保留来源变体，并保护构造、Clone、
// Display、Debug、排序和 Hash 的确定语义。
#[test]
fn test_entry_name_preserves_native_and_remote_names() {
    assert_send_sync_static::<EntryName>();

    let native_from_owned = EntryName::from(OsString::from("alpha"));
    let native_from_borrowed = EntryName::from(OsStr::new("alpha"));
    let remote = EntryName::from(Atom::from("alpha"));

    assert_eq!(native_from_owned, native_from_borrowed);
    assert_ne!(native_from_owned, remote);
    assert_eq!(native_from_owned.to_string(), "alpha");
    assert_eq!(remote.to_string(), "alpha");
    assert_eq!(format!("{native_from_owned:?}"), "Native(\"alpha\")");
    assert_eq!(format!("{remote:?}"), "Remote(\"alpha\")");
    assert_eq!(native_from_owned, native_from_owned.clone());
    assert_eq!(remote, remote.clone());
    assert_eq!(hash_of(&native_from_owned), hash_of(&native_from_borrowed));
    assert!(native_from_owned < remote, "变体顺序必须确定且可复现");
}

// 验证 Unix 原生名称逐字节保留非 UTF-8 内容，Clone 不转码，Debug 使用可
// 辨认转义而 Display 只作有损人类显示；该平台分支不适用于 Windows。
#[cfg(unix)]
#[test]
fn test_entry_name_preserves_non_utf8_native_bytes() {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let bytes = b"name-\xff".to_vec();
    let name = EntryName::from(OsString::from_vec(bytes.clone()));
    let cloned = name.clone();

    match &cloned {
        EntryName::Native(value) => assert_eq!(value.as_os_str().as_bytes(), bytes),
        EntryName::Remote(_) => panic!("非 UTF-8 原生名称不得转成远端名称"),
        _ => panic!("测试必须识别新增的名称表示"),
    }
    assert_eq!(name, cloned);
    assert_eq!(format!("{name:?}"), "Native(\"name-\\xFF\")");
    assert_eq!(name.to_string(), "name-�");
}

// 验证 `DirectoryEntry` 的名称、定位符和可选类型提示全部参与值语义、排序、
// Hash 与格式化，并明确 `None` 只表示提示不可用而不是条目类型错误。
#[test]
fn test_directory_entry_value_semantics_include_every_public_field() {
    assert_send_sync_static::<DirectoryEntry<String>>();

    let base = DirectoryEntry::new(
        EntryName::from(OsString::from("alpha")),
        String::from("root/alpha"),
        Some(FileType::RegularFile),
    );
    let cloned = base.clone();

    assert_eq!(base, cloned);
    assert_eq!(base.name.to_string(), "alpha");
    assert_eq!(base.locator, "root/alpha");
    assert_eq!(base.file_type_hint, Some(FileType::RegularFile));
    assert_eq!(hash_of(&base), hash_of(&cloned));
    assert_eq!(
        format!("{base:?}"),
        "DirectoryEntry { name: Native(\"alpha\"), locator: \"root/alpha\", \
         file_type_hint: Some(RegularFile) }",
    );
    assert_eq!(
        base.to_string(),
        "alpha at root/alpha, type hint: regular file",
    );

    let different_name = DirectoryEntry::new(
        EntryName::from(OsString::from("beta")),
        String::from("root/alpha"),
        Some(FileType::RegularFile),
    );
    let different_locator = DirectoryEntry::new(
        EntryName::from(OsString::from("alpha")),
        String::from("root/beta"),
        Some(FileType::RegularFile),
    );
    let no_hint = DirectoryEntry::new(
        EntryName::from(OsString::from("alpha")),
        String::from("root/alpha"),
        None,
    );

    assert!(base < different_name);
    assert!(base < different_locator);
    assert!(no_hint < base);
    assert_ne!(hash_of(&base), hash_of(&different_name));
    assert_ne!(hash_of(&base), hash_of(&different_locator));
    assert_ne!(hash_of(&base), hash_of(&no_hint));
    assert_eq!(
        no_hint.to_string(),
        "alpha at root/alpha, type hint: unavailable",
    );
}
