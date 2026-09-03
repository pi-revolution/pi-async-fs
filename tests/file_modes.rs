//! 文件访问角色与刷新强度枚举的公开值语义测试。
//!
//! 本文件验证所有冻结变体的格式、顺序和 Hash；具体角色能调用哪些 I/O 方法
//! 以及刷新副作用由相应 `tests/local_file_*.rs` 集成测试覆盖。

use pi_async_fs::{FileAccessMode, FileFlushMode};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// 验证六种访问角色全部具有稳定且互异的 Display／Debug、声明顺序和 Hash；
// 该顺序仅是值语义，不被测试解释为权限强弱或可相互升级。
#[test]
fn test_file_access_mode_value_semantics_cover_every_role() {
    let modes = [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::ReadMmap,
        FileAccessMode::ReadWriteMmap,
        FileAccessMode::Truncate,
    ];
    let displays = [
        "read",
        "append",
        "overwrite",
        "read-mmap",
        "read-write-mmap",
        "truncate",
    ];
    let debugs = [
        "Read",
        "Append",
        "Overwrite",
        "ReadMmap",
        "ReadWriteMmap",
        "Truncate",
    ];

    for ((mode, display), debug) in modes.iter().zip(displays).zip(debugs) {
        assert_eq!(mode.to_string(), display);
        assert_eq!(format!("{mode:?}"), debug);
        assert_eq!(mode, mode);

        let mut first_hasher = DefaultHasher::new();
        mode.hash(&mut first_hasher);
        let mut second_hasher = DefaultHasher::new();
        mode.hash(&mut second_hasher);
        assert_eq!(first_hasher.finish(), second_hasher.finish());
    }

    for adjacent in modes.windows(2) {
        assert!(adjacent[0] < adjacent[1]);
    }
}

// 验证两种刷新模式按保证强度保持稳定格式与顺序，且相同值 Hash 一致；本
// 纯值测试不声称磁盘、掉电或元信息持久化已经发生。
#[test]
fn test_file_flush_mode_value_semantics_follow_guarantee_strength() {
    let modes = [FileFlushMode::Data, FileFlushMode::DataAndMetadata];
    let displays = ["data", "data-and-metadata"];
    let debugs = ["Data", "DataAndMetadata"];

    for ((mode, display), debug) in modes.iter().zip(displays).zip(debugs) {
        assert_eq!(mode.to_string(), display);
        assert_eq!(format!("{mode:?}"), debug);
        assert_eq!(mode, mode);

        let mut first_hasher = DefaultHasher::new();
        mode.hash(&mut first_hasher);
        let mut second_hasher = DefaultHasher::new();
        mode.hash(&mut second_hasher);
        assert_eq!(first_hasher.finish(), second_hasher.finish());
    }

    assert!(modes[0] < modes[1]);
}
