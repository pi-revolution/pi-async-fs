//! 固定读取目标窗口 `ReadTargetRegion` 的纯值与边界测试。
//!
//! 覆盖各种标准范围表达、半开规范化、空窗口、溢出和绑定时越界；真实 buffer
//! 写入隔离及文件 EOF 行为由 `tests/local_file_read.rs` 覆盖。

use pi_async_fs::{ReadTargetRegion, ReadTargetRegionError};
use pi_result::{ClassifyErrorKind, ErrorKind};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Bound;

// 验证排除起点／包含终点被受检地规范化为 `[2, 5)`，并在给定已初始化长度
// 后解析为完全相同的 Rust 切片范围。
#[test]
fn test_read_target_region_normalizes_and_resolves_explicit_bounds() {
    let region = ReadTargetRegion::new(Bound::Excluded(1), Bound::Included(4))
        .expect("排除起点并包含终点应规范化为半开区间");

    assert_eq!(region.start(), 2);
    assert_eq!(region.end_exclusive(), Some(5));
    assert_eq!(region.resolve(8).expect("范围位于目标视图内"), 2..5);
}

// 验证 `full()` 的无界终点在每次绑定时跟随当前已初始化视图长度，同时让
// 长度为零的目标合法解析为空窗口。
#[test]
fn test_read_target_region_full_tracks_initialized_view_length() {
    let region = ReadTargetRegion::full();

    assert_eq!(region.start(), 0);
    assert_eq!(region.end_exclusive(), None);
    assert_eq!(region.resolve(0).expect("空目标视图允许空范围"), 0..0);
    assert_eq!(region.resolve(6).expect("完整范围延伸到当前末尾"), 0..6);
}

// 验证端点加一溢出、规范化后反向及绑定时越界三类失败在切片前被拒绝，
// 保留精确错误字段并统一分类为 `InvalidInput`。
#[test]
fn test_read_target_region_rejects_invalid_or_out_of_bounds_ranges() {
    let excluded_start = ReadTargetRegion::new(
        Bound::Excluded(usize::MAX),
        Bound::Unbounded,
    )
    .expect_err("排除最大起点会发生受检加法溢出");
    let included_end = ReadTargetRegion::new(
        Bound::Unbounded,
        Bound::Included(usize::MAX),
    )
    .expect_err("包含最大终点会发生受检加法溢出");
    let reversed = ReadTargetRegion::new(Bound::Included(5), Bound::Excluded(4))
        .expect_err("规范化后的反向范围必须失败");
    let trailing = ReadTargetRegion::new(Bound::Included(5), Bound::Unbounded)
        .expect("到末尾的范围在绑定缓冲区前结构有效");
    let out_of_bounds = trailing
        .resolve(4)
        .expect_err("起点超过已初始化长度必须在切片前失败");

    assert!(matches!(
        &excluded_start,
        ReadTargetRegionError::ExcludedStartOverflow
    ));
    assert!(matches!(
        &included_end,
        ReadTargetRegionError::IncludedEndOverflow
    ));
    assert!(matches!(
        &reversed,
        ReadTargetRegionError::Reversed {
            start: 5,
            end_exclusive: 4,
        }
    ));
    assert!(matches!(
        &out_of_bounds,
        ReadTargetRegionError::OutOfBounds {
            start: 5,
            end_exclusive: 4,
            initialized_len: 4,
        }
    ));

    for error in [
        &excluded_start,
        &included_end,
        &reversed,
        &out_of_bounds,
    ] {
        assert_eq!(error.classify_error_kind(), ErrorKind::InvalidInput);
    }
}

// 验证标准库半开、闭、单侧无界和全范围形式都转换为预期逻辑窗口，尤其保护
// 包含终点只进行一次受检加一。
#[test]
fn test_read_target_region_converts_standard_range_forms() {
    let half_open = ReadTargetRegion::try_from(1..3).expect("半开范围有效");
    let inclusive = ReadTargetRegion::try_from(1..=3).expect("闭区间终点可转换");
    let from = ReadTargetRegion::from(2..);
    let to = ReadTargetRegion::from(..3);
    let to_inclusive = ReadTargetRegion::try_from(..=3).expect("包含终点可转换");
    let full = ReadTargetRegion::from(..);

    assert_eq!(half_open.resolve(7).unwrap(), 1..3);
    assert_eq!(inclusive.resolve(7).unwrap(), 1..4);
    assert_eq!(from.resolve(7).unwrap(), 2..7);
    assert_eq!(to.resolve(7).unwrap(), 0..3);
    assert_eq!(to_inclusive.resolve(7).unwrap(), 0..4);
    assert_eq!(full.resolve(7).unwrap(), 0..7);
}

// 验证范围 Clone、判等、排序和 Hash 按规范化后的逻辑端点工作，其中同起点
// 的无界终点按正无穷排序。
#[test]
fn test_read_target_region_clone_order_and_hash_follow_logical_bounds() {
    let fixed = ReadTargetRegion::try_from(2..5).expect("测试范围有效");
    let same = fixed.clone();
    let unbounded = ReadTargetRegion::from(2..);
    let later = ReadTargetRegion::try_from(3..4).expect("测试范围有效");

    assert_eq!(fixed, same);
    assert!(fixed < unbounded, "同起点的无界终点按正无穷排序");
    assert!(unbounded < later, "排序首先比较起点");

    let mut fixed_hasher = DefaultHasher::new();
    fixed.hash(&mut fixed_hasher);
    let mut same_hasher = DefaultHasher::new();
    same.hash(&mut same_hasher);
    assert_eq!(fixed_hasher.finish(), same_hasher.finish());
}

// 验证固定与无界终点的 Display／Debug 明确使用半开表达和 `end` 语义，
// 不把无界值伪装成某个捕获时长度。
#[test]
fn test_read_target_region_formats_fixed_and_unbounded_endpoints() {
    let fixed = ReadTargetRegion::try_from(2..5).expect("测试范围有效");
    let unbounded = ReadTargetRegion::from(2..);

    assert_eq!(fixed.to_string(), "[2, 5)");
    assert_eq!(unbounded.to_string(), "[2, end)");
    assert_eq!(
        format!("{fixed:?}"),
        "ReadTargetRegion { start: 2, end_exclusive: Some(5) }"
    );
}

// 验证固定空窗口、`usize::MAX` 排他终点和最大位置空尾窗口均不溢出，同时
// 以相邻越界固定窗口作为负面控制。
#[test]
fn test_read_target_region_covers_empty_fixed_and_usize_boundary_windows() {
    let empty = ReadTargetRegion::try_from(4..4).expect("固定读取允许空目标窗口");
    let maximum = ReadTargetRegion::try_from(0..usize::MAX)
        .expect("排他最大终点不需要加一");
    let empty_at_end = ReadTargetRegion::from(usize::MAX..);

    assert_eq!(empty.resolve(9).unwrap(), 4..4);
    assert_eq!(maximum.resolve(usize::MAX).unwrap(), 0..usize::MAX);
    assert_eq!(
        empty_at_end.resolve(usize::MAX).unwrap(),
        usize::MAX..usize::MAX
    );

    let fixed_out_of_bounds = ReadTargetRegion::try_from(0..5)
        .unwrap()
        .resolve(4)
        .expect_err("固定终点超过已初始化视图必须失败");
    assert!(matches!(
        fixed_out_of_bounds,
        ReadTargetRegionError::OutOfBounds {
            start: 0,
            end_exclusive: 5,
            initialized_len: 4,
        }
    ));
}
