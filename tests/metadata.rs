//! 文件元信息相关公开值类型的完整纯契约测试。
//!
//! 覆盖文件类型、可移植权限、时间、资源版本、元信息快照、极值和诊断脱敏；
//! 真实本地查询与符号链接语义由 `tests/local_namespace.rs` 覆盖。

use pi_async_fs::{
    FileMetadata, FileTimes, FileType, PortablePermissions, ResourceVersion,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

// 验证全部公开 `FileType` 变体的稳定格式、声明顺序、Clone、判等和 Hash，
// 不把 Other 与 Unknown 或本地特殊文件与远端对象混同。
#[test]
fn test_file_type_value_semantics_cover_every_public_variant() {
    let kinds = [
        FileType::RegularFile,
        FileType::Directory,
        FileType::SymbolicLink,
        FileType::RemoteObject,
        FileType::BlockDevice,
        FileType::CharacterDevice,
        FileType::Fifo,
        FileType::Socket,
        FileType::Other,
        FileType::Unknown,
    ];
    let displays = [
        "regular file",
        "directory",
        "symbolic link",
        "remote object",
        "block device",
        "character device",
        "fifo",
        "socket",
        "other",
        "unknown",
    ];
    let debugs = [
        "RegularFile",
        "Directory",
        "SymbolicLink",
        "RemoteObject",
        "BlockDevice",
        "CharacterDevice",
        "Fifo",
        "Socket",
        "Other",
        "Unknown",
    ];

    for ((kind, display), debug) in kinds.iter().zip(displays).zip(debugs) {
        assert_eq!(kind.to_string(), display);
        assert_eq!(format!("{kind:?}"), debug);
        assert_eq!(kind, &kind.clone());

        let mut original_hasher = DefaultHasher::new();
        kind.hash(&mut original_hasher);
        let mut clone_hasher = DefaultHasher::new();
        kind.clone().hash(&mut clone_hasher);
        assert_eq!(original_hasher.finish(), clone_hasher.finish());
    }

    for adjacent in kinds.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }
}

// 验证只读／可执行两个三态字段的九种组合，保护 unknown 与 false 的区别，
// 并使 Display、Debug、排序、Clone 和 Hash 遵循字段值。
#[test]
fn test_portable_permissions_cover_all_known_and_unknown_combinations() {
    let permissions = [
        PortablePermissions {
            read_only: None,
            executable: None,
        },
        PortablePermissions {
            read_only: None,
            executable: Some(false),
        },
        PortablePermissions {
            read_only: None,
            executable: Some(true),
        },
        PortablePermissions {
            read_only: Some(false),
            executable: None,
        },
        PortablePermissions {
            read_only: Some(false),
            executable: Some(false),
        },
        PortablePermissions {
            read_only: Some(false),
            executable: Some(true),
        },
        PortablePermissions {
            read_only: Some(true),
            executable: None,
        },
        PortablePermissions {
            read_only: Some(true),
            executable: Some(false),
        },
        PortablePermissions {
            read_only: Some(true),
            executable: Some(true),
        },
    ];
    let displays = [
        "read-only: unknown, executable: unknown",
        "read-only: unknown, executable: no",
        "read-only: unknown, executable: yes",
        "read-only: no, executable: unknown",
        "read-only: no, executable: no",
        "read-only: no, executable: yes",
        "read-only: yes, executable: unknown",
        "read-only: yes, executable: no",
        "read-only: yes, executable: yes",
    ];

    for (permission, display) in permissions.iter().zip(displays) {
        assert_eq!(permission.to_string(), display);
        assert_eq!(permission, &permission.clone());

        let mut original_hasher = DefaultHasher::new();
        permission.hash(&mut original_hasher);
        let mut clone_hasher = DefaultHasher::new();
        permission.clone().hash(&mut clone_hasher);
        assert_eq!(original_hasher.finish(), clone_hasher.finish());
    }

    assert_eq!(
        format!("{:?}", permissions[0]),
        "PortablePermissions { read_only: None, executable: None }"
    );
    for adjacent in permissions.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }
}

// 验证已知、缺失、纪元后和纪元前时间均以纳秒日期时间显示，四个字段名称
// 与顺序稳定，缺失值明确显示 unknown。
#[test]
fn test_file_times_format_known_missing_and_pre_epoch_values_as_datetimes() {
    // Windows `SystemTime` 使用 100 纳秒刻度；100 纳秒是本测试在 Unix 与
    // Windows 上都能精确构造、同时仍覆盖负时间借位分支的最小公共输入。
    let times = FileTimes {
        created: Some(UNIX_EPOCH),
        modified: Some(UNIX_EPOCH + Duration::new(1, 250_000_000)),
        accessed: Some(UNIX_EPOCH - Duration::from_nanos(100)),
        metadata_changed: None,
    };

    let display = times.to_string();
    let debug = format!("{times:?}");

    assert_eq!(
        display,
        "created: 1970-01-01T00:00:00.000000000Z, \
         modified: 1970-01-01T00:00:01.250000000Z, \
         accessed: 1969-12-31T23:59:59.999999900Z, \
         metadata changed: unknown"
    );
    for expected in [
        "1970-01-01T00:00:00.000000000Z",
        "1970-01-01T00:00:01.250000000Z",
        "1969-12-31T23:59:59.999999900Z",
        "unknown",
    ] {
        assert!(debug.contains(expected));
    }
}

// 验证 `FileTimes` 的 Clone、判等、排序和 Hash 按 created、modified、
// accessed、metadata_changed 声明顺序及实际字段值工作。
#[test]
fn test_file_times_value_semantics_follow_field_values_and_declaration_order() {
    let times = [
        FileTimes {
            created: None,
            modified: None,
            accessed: None,
            metadata_changed: None,
        },
        FileTimes {
            created: Some(UNIX_EPOCH),
            modified: None,
            accessed: None,
            metadata_changed: None,
        },
        FileTimes {
            created: Some(UNIX_EPOCH),
            modified: Some(UNIX_EPOCH),
            accessed: None,
            metadata_changed: None,
        },
        FileTimes {
            created: Some(UNIX_EPOCH),
            modified: Some(UNIX_EPOCH),
            accessed: Some(UNIX_EPOCH),
            metadata_changed: None,
        },
        FileTimes {
            created: Some(UNIX_EPOCH),
            modified: Some(UNIX_EPOCH),
            accessed: Some(UNIX_EPOCH),
            metadata_changed: Some(UNIX_EPOCH),
        },
        FileTimes {
            created: Some(UNIX_EPOCH + Duration::from_secs(1)),
            modified: None,
            accessed: None,
            metadata_changed: None,
        },
    ];

    for time in &times {
        let cloned = time.clone();
        assert_eq!(time, &cloned);

        let mut original_hasher = DefaultHasher::new();
        time.hash(&mut original_hasher);
        let mut clone_hasher = DefaultHasher::new();
        cloned.hash(&mut clone_hasher);
        assert_eq!(original_hasher.finish(), clone_hasher.finish());
    }

    for adjacent in times.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }
}

// 验证超出常规日期格式范围但仍可由 `SystemTime` 表达的极端正负时间不会
// panic，并回退为精确、带符号的 Unix 秒／纳秒表示。
#[test]
fn test_file_times_format_extreme_representable_system_times_without_panicking() {
    let magnitude = Duration::new(9_000_000_000_000, 7);
    let future = UNIX_EPOCH.checked_add(magnitude);
    let past = UNIX_EPOCH.checked_sub(magnitude);
    let times = FileTimes {
        created: future,
        modified: None,
        accessed: past,
        metadata_changed: None,
    };

    let display = times.to_string();
    let debug = format!("{times:?}");

    if future.is_some() {
        assert!(display.contains("unix+9000000000000.000000007s"));
        assert!(debug.contains("unix+9000000000000.000000007s"));
    }
    if past.is_some() {
        assert!(display.contains("unix-9000000000000.000000007s"));
        assert!(debug.contains("unix-9000000000000.000000007s"));
    }
}

// 验证资源版本保留定位符和任意长度／任意字节的不透明 token，并在
// `into_parts` 中原样归还同一 `Arc` 分配，包括空 token。
#[test]
fn test_resource_version_preserves_locator_and_opaque_token_allocation() {
    for token in [
        Arc::<[u8]>::from([]),
        Arc::<[u8]>::from([0x00, 0xff, 0x80, 0x2f]),
    ] {
        let locator = String::from("objects/example");
        let version = ResourceVersion::new(locator.clone(), Arc::clone(&token));

        assert_eq!(version.locator(), &locator);
        assert_eq!(version.token(), token.as_ref());

        let (returned_locator, returned_token) = version.into_parts();
        assert_eq!(returned_locator, locator);
        assert!(Arc::ptr_eq(&returned_token, &token));
    }
}

// 验证 `ResourceVersion::clone` 复制定位符值但零成本共享 token 字节分配，
// 以指针判等证明没有为不透明 token 重复分配。
#[test]
fn test_resource_version_clone_shares_token_bytes() {
    let token = Arc::<[u8]>::from([1, 2, 3, 4]);
    let original = ResourceVersion::new(String::from("objects/example"), token);
    let cloned = original.clone();

    let (original_locator, original_token) = original.into_parts();
    let (cloned_locator, cloned_token) = cloned.into_parts();
    assert_eq!(original_locator, cloned_locator);
    assert!(Arc::ptr_eq(&original_token, &cloned_token));
}

// 验证资源版本 Display／Debug 只报告定位符与 token 长度，绝不输出不透明
// token 内容，避免诊断日志泄漏版本凭据或摘要原文。
#[test]
fn test_resource_version_diagnostic_formats_redact_token_content() {
    let version = ResourceVersion::new(
        String::from("objects/example"),
        Arc::<[u8]>::from(*b"secret"),
    );

    let display = version.to_string();
    let debug = format!("{version:?}");
    assert_eq!(
        display,
        "objects/example (opaque version token: 6 bytes)"
    );
    assert_eq!(
        debug,
        "ResourceVersion { locator: \"objects/example\", token_len: 6 }"
    );
    assert!(!display.contains("secret"));
    assert!(!debug.contains("secret"));
}

// 验证资源版本值语义先比较定位符再比较 token 字节；不同分配但相同字节必须
// 判等且 Hash 相同，长度或字节差异必须参与排序。
#[test]
fn test_resource_version_value_semantics_use_locator_then_token_bytes() {
    let versions = [
        ResourceVersion::new(String::from("a"), Arc::<[u8]>::from([1])),
        ResourceVersion::new(String::from("a"), Arc::<[u8]>::from([1, 0])),
        ResourceVersion::new(String::from("a"), Arc::<[u8]>::from([2])),
        ResourceVersion::new(String::from("b"), Arc::<[u8]>::from([])),
    ];
    let equal_from_distinct_allocation =
        ResourceVersion::new(String::from("a"), Arc::<[u8]>::from([1]));

    assert_eq!(versions[0], equal_from_distinct_allocation);
    assert_ne!(versions[0], versions[1]);
    assert_ne!(versions[0], versions[3]);
    for adjacent in versions.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }

    let mut left_hasher = DefaultHasher::new();
    versions[0].hash(&mut left_hasher);
    let mut right_hasher = DefaultHasher::new();
    equal_from_distinct_allocation.hash(&mut right_hasher);
    assert_eq!(left_hasher.finish(), right_hasher.finish());
}

// 验证 `FileMetadata::new` 对 byte_len 的 None、0、`u64::MAX` 三种状态及
// 类型、权限、时间、可选版本全部原样保存。
#[test]
fn test_file_metadata_constructor_preserves_all_length_states_and_fields() {
    for byte_len in [None, Some(0), Some(u64::MAX)] {
        let metadata = FileMetadata::new(
            FileType::RegularFile,
            byte_len,
            PortablePermissions {
                read_only: Some(false),
                executable: None,
            },
            FileTimes {
                created: Some(UNIX_EPOCH),
                modified: None,
                accessed: None,
                metadata_changed: None,
            },
            Some(ResourceVersion::new(
                String::from("objects/example"),
                Arc::<[u8]>::from([0xff]),
            )),
        );

        assert_eq!(metadata.file_type, FileType::RegularFile);
        assert_eq!(metadata.byte_len, byte_len);
        assert_eq!(metadata.permissions.read_only, Some(false));
        assert_eq!(metadata.permissions.executable, None);
        assert_eq!(metadata.times.created, Some(UNIX_EPOCH));
        let version = metadata.version.expect("version must be preserved");
        assert_eq!(version.locator(), "objects/example");
        assert_eq!(version.token(), &[0xff]);
    }
}

// 验证元信息 Clone 保留完整快照，同时其可选资源版本继续共享同一 token
// 分配；该共享不把快照变成实时查询。
#[test]
fn test_file_metadata_clone_preserves_snapshot_and_shares_version_token() {
    let metadata = FileMetadata::new(
        FileType::RegularFile,
        Some(42),
        PortablePermissions {
            read_only: Some(true),
            executable: Some(false),
        },
        FileTimes {
            created: None,
            modified: Some(UNIX_EPOCH),
            accessed: None,
            metadata_changed: None,
        },
        Some(ResourceVersion::new(
            String::from("objects/example"),
            Arc::<[u8]>::from([1, 2, 3]),
        )),
    );
    let cloned = metadata.clone();

    assert_eq!(metadata.file_type, cloned.file_type);
    assert_eq!(metadata.byte_len, cloned.byte_len);
    assert_eq!(metadata.permissions, cloned.permissions);
    assert_eq!(metadata.times, cloned.times);
    let (_, original_token) = metadata.version.expect("original version").into_parts();
    let (_, cloned_token) = cloned.version.expect("cloned version").into_parts();
    assert!(Arc::ptr_eq(&original_token, &cloned_token));
}

// 验证元信息 Debug／Display 明确保留 unknown 字段并递归脱敏版本 token，
// 同时覆盖有版本文件和无版本目录两种诊断边界。
#[test]
fn test_file_metadata_diagnostic_formats_preserve_unknowns_and_redact_versions() {
    let metadata = FileMetadata::new(
        FileType::RegularFile,
        Some(42),
        PortablePermissions {
            read_only: Some(true),
            executable: Some(false),
        },
        FileTimes {
            created: None,
            modified: Some(UNIX_EPOCH),
            accessed: None,
            metadata_changed: None,
        },
        Some(ResourceVersion::new(
            String::from("objects/example"),
            Arc::<[u8]>::from(*b"secret"),
        )),
    );

    let display = metadata.to_string();
    let debug = format!("{metadata:?}");
    assert_eq!(
        display,
        "type: regular file, byte length: 42 bytes, \
         permissions: read-only: yes, executable: no, \
         times: created: unknown, modified: 1970-01-01T00:00:00.000000000Z, \
         accessed: unknown, metadata changed: unknown, \
         version: objects/example (opaque version token: 6 bytes)"
    );
    assert_eq!(
        debug,
        "FileMetadata { file_type: RegularFile, byte_len: Some(42), \
         permissions: PortablePermissions { read_only: Some(true), executable: Some(false) }, \
         times: FileTimes { created: unknown, modified: 1970-01-01T00:00:00.000000000Z, \
         accessed: unknown, metadata_changed: unknown }, \
         version: Some(ResourceVersion { locator: \"objects/example\", token_len: 6 }) }"
    );
    assert!(!display.contains("secret"));
    assert!(!debug.contains("secret"));

    let unknown = FileMetadata::<String>::new(
        FileType::Directory,
        None,
        PortablePermissions {
            read_only: None,
            executable: None,
        },
        FileTimes {
            created: None,
            modified: None,
            accessed: None,
            metadata_changed: None,
        },
        None,
    )
    .to_string();
    assert!(unknown.contains("byte length: unknown"));
    assert!(unknown.ends_with("version: none"));
}

// 验证元信息判等、排序和 Hash 按公开字段声明顺序工作，且不同 token 分配的
// 等值版本不会破坏整个快照的值语义。
#[test]
fn test_file_metadata_value_semantics_follow_public_field_order() {
    let make_metadata = |
        file_type,
        byte_len,
        read_only,
        created,
        version: Option<ResourceVersion<String>>,
    | {
        FileMetadata::new(
            file_type,
            byte_len,
            PortablePermissions {
                read_only,
                executable: None,
            },
            FileTimes {
                created,
                modified: None,
                accessed: None,
                metadata_changed: None,
            },
            version,
        )
    };
    let metadata = [
        make_metadata(FileType::RegularFile, None, None, None, None),
        make_metadata(FileType::RegularFile, Some(0), None, None, None),
        make_metadata(
            FileType::RegularFile,
            Some(0),
            Some(false),
            None,
            None,
        ),
        make_metadata(
            FileType::RegularFile,
            Some(0),
            Some(false),
            Some(UNIX_EPOCH),
            None,
        ),
        make_metadata(
            FileType::RegularFile,
            Some(0),
            Some(false),
            Some(UNIX_EPOCH),
            Some(ResourceVersion::new(
                String::from("objects/example"),
                Arc::<[u8]>::from([1]),
            )),
        ),
        make_metadata(FileType::Directory, None, None, None, None),
    ];
    let equal_from_distinct_token_allocation = make_metadata(
        FileType::RegularFile,
        Some(0),
        Some(false),
        Some(UNIX_EPOCH),
        Some(ResourceVersion::new(
            String::from("objects/example"),
            Arc::<[u8]>::from([1]),
        )),
    );

    assert_eq!(metadata[4], equal_from_distinct_token_allocation);
    for adjacent in metadata.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }

    let mut left_hasher = DefaultHasher::new();
    metadata[4].hash(&mut left_hasher);
    let mut right_hasher = DefaultHasher::new();
    equal_from_distinct_token_allocation.hash(&mut right_hasher);
    assert_eq!(left_hasher.finish(), right_hasher.finish());
}
