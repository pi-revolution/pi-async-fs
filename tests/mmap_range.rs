//! 内存映射逻辑范围 `MmapRange` 的纯值契约测试。
//!
//! 覆盖半开区间、不允许空区间、端点极值、错误分类和值语义；真实页对齐、映射
//! 生命周期和冲突矩阵由 `tests/local_file_mmap.rs` 覆盖。

use pi_async_fs::{MmapRange, MmapRangeError};
use pi_result::{ClassifyErrorKind, ErrorKind};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// 验证合法非空 `[start, end)` 的端点与长度，并同时拒绝空区间和反向区间，
// 防止在进入任何映射系统调用前产生零长或下溢语义。
#[test]
fn test_mmap_range_enforces_nonempty_half_open_interval() {
    let range = MmapRange::new(3, 11).expect("非空且正向的半开区间应可构造");

    assert_eq!(range.start(), 3);
    assert_eq!(range.end_exclusive(), 11);
    assert_eq!(range.len(), 8);

    assert!(matches!(
        MmapRange::new(7, 7),
        Err(MmapRangeError::Empty { at: 7 })
    ));
    assert!(matches!(
        MmapRange::new(9, 4),
        Err(MmapRangeError::Reversed {
            start: 9,
            end_exclusive: 4,
        })
    ));
}

// 验证从标准库 `Range<u64>` 转换时保持相同半开端点，不进行隐式闭区间转换、
// 对齐或文件长度裁剪。
#[test]
fn test_mmap_range_converts_from_standard_range() {
    let range = MmapRange::try_from(13..21).expect("标准半开区间应保留相同端点");

    assert_eq!(range.start(), 13);
    assert_eq!(range.end_exclusive(), 21);
}

// 验证用户格式和开发者格式都明确表达排他终点，避免把 `[2, 5)` 误读为
// 闭区间或物理页范围。
#[test]
fn test_mmap_range_formats_for_users_and_diagnostics() {
    let range = MmapRange::new(2, 5).expect("测试范围有效");

    assert_eq!(range.to_string(), "[2, 5)");
    assert_eq!(
        format!("{range:?}"),
        "MmapRange { start: 2, end_exclusive: 5 }"
    );
}

// 验证 Clone、判等、排序和 Hash 完全依据 `(start, end_exclusive)`，其中先
// 比较起点再比较终点，不混入映射权限或文件身份。
#[test]
fn test_mmap_range_uses_endpoint_value_semantics() {
    let first = MmapRange::new(0, 5).expect("测试范围有效");
    let same = MmapRange::new(0, 5).expect("测试范围有效");
    let wider = MmapRange::new(0, 7).expect("测试范围有效");
    let later = MmapRange::new(1, 2).expect("测试范围有效");

    assert_eq!(first, same);
    assert!(first < wider);
    assert!(wider < later);

    let mut first_hasher = DefaultHasher::new();
    first.hash(&mut first_hasher);
    let mut same_hasher = DefaultHasher::new();
    same.hash(&mut same_hasher);
    assert_eq!(first_hasher.finish(), same_hasher.finish());
}

// 验证空区间和反向区间两类构造错误都精确分类为 `InvalidInput`，而非后端
// I/O、冲突或资源错误。
#[test]
fn test_mmap_range_errors_are_invalid_input() {
    let empty = MmapRange::new(4, 4).expect_err("空范围必须失败");
    let reversed = MmapRange::new(8, 3).expect_err("反向范围必须失败");

    assert_eq!(empty.classify_error_kind(), ErrorKind::InvalidInput);
    assert_eq!(reversed.classify_error_kind(), ErrorKind::InvalidInput);
}

// 验证逻辑坐标可使用完整 `u64` 排他终点，包括全坐标窗口和最后一字节窗口，
// 且长度计算不执行会溢出的终点加一。
#[test]
fn test_mmap_range_accepts_u64_coordinate_boundaries_without_overflow() {
    let whole_coordinate_space = MmapRange::new(0, u64::MAX).unwrap();
    let final_byte = MmapRange::new(u64::MAX - 1, u64::MAX).unwrap();

    assert_eq!(whole_coordinate_space.len(), u64::MAX);
    assert_eq!(final_byte.start(), u64::MAX - 1);
    assert_eq!(final_byte.len(), 1);
}
