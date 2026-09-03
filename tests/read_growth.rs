//! 增长式读取上限与结果类型的纯值契约测试。
//!
//! 覆盖“只限制新增字节”、`usize` 算术边界、停止原因和诊断；真实 Vec／
//! `BytesMut` 扩容、读取和取消由 `tests/local_file_read.rs` 覆盖。

use pi_async_fs::{ReadGrowthLimit, ReadGrowthLimitError, ReadGrowthOutcome};
use pi_result::{ClassifyErrorKind, ErrorKind};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

// 验证增长上限只约束相对当前长度新增的字节数，0 表示不允许增长而不是清空
// 既有内容，普通上限按受检加法得到最终长度。
#[test]
fn test_read_growth_limit_bounds_only_newly_appended_bytes() {
    let zero = ReadGrowthLimit::new(0);
    let seven = ReadGrowthLimit::new(7);

    assert_eq!(zero.max_additional_bytes(), 0);
    assert_eq!(zero.checked_final_len(5).unwrap(), 5);
    assert_eq!(seven.max_additional_bytes(), 7);
    assert_eq!(seven.checked_final_len(5).unwrap(), 12);
}

// 验证当前长度加允许增长量发生 `usize` 溢出时，在分配或 I/O 前返回结构化
// `InvalidInput`，并完整保留两个参与计算的边界值。
#[test]
fn test_read_growth_limit_reports_final_length_overflow_as_invalid_input() {
    let error = ReadGrowthLimit::new(1)
        .checked_final_len(usize::MAX)
        .expect_err("最终长度溢出必须在任何分配或读取前失败");

    assert!(matches!(
        &error,
        ReadGrowthLimitError::FinalLengthOverflow {
            current_len: usize::MAX,
            max_additional_bytes: 1,
        }
    ));
    assert_eq!(error.classify_error_kind(), ErrorKind::InvalidInput);
}

// 验证上限的 Clone、判等、全序和 Hash 只依据最大新增字节数，覆盖普通值与
// `usize::MAX`，不引入当前 buffer 长度等运行状态。
#[test]
fn test_read_growth_limit_clone_order_and_hash_follow_numeric_limit() {
    let small = ReadGrowthLimit::new(3);
    let same = small.clone();
    let large = ReadGrowthLimit::new(usize::MAX);

    assert_eq!(small, same);
    assert!(small < large);

    let mut small_hasher = DefaultHasher::new();
    small.hash(&mut small_hasher);
    let mut same_hasher = DefaultHasher::new();
    same.hash(&mut same_hasher);
    assert_eq!(small_hasher.finish(), same_hasher.finish());
}

// 验证 Display 和 Debug 均明确其数字表示“最大新增字节数”，避免被误读为
// 最终 buffer 大小或文件读取终点。
#[test]
fn test_read_growth_limit_formats_numeric_permission() {
    let limit = ReadGrowthLimit::new(7);

    assert_eq!(limit.to_string(), "maximum additional bytes: 7");
    assert_eq!(
        format!("{limit:?}"),
        "ReadGrowthLimit { max_additional_bytes: 7 }"
    );
}

// 验证 `0 + usize::MAX` 和 `usize::MAX + 0` 两个不溢出的算术边界均被接受，
// 与相邻的真实溢出错误测试形成正负控制。
#[test]
fn test_read_growth_limit_accepts_usize_arithmetic_boundaries() {
    assert_eq!(ReadGrowthLimit::new(usize::MAX).checked_final_len(0).unwrap(), usize::MAX);
    assert_eq!(ReadGrowthLimit::new(0).checked_final_len(usize::MAX).unwrap(), usize::MAX);
}

// 验证增长读取结果分别报告 EOF 与上限停止，并让 getter 与谓词对相同提交
// 字节数给出互斥、一致的停止原因。
#[test]
fn test_read_growth_outcome_reports_stop_reason_and_committed_bytes() {
    let eof = ReadGrowthOutcome::EndOfFile { appended_bytes: 3 };
    let limit = ReadGrowthOutcome::LimitReached { appended_bytes: 5 };

    assert_eq!(eof.appended_bytes(), 3);
    assert!(eof.is_end_of_file());
    assert!(!eof.is_limit_reached());

    assert_eq!(limit.appended_bytes(), 5);
    assert!(!limit.is_end_of_file());
    assert!(limit.is_limit_reached());
}

// 验证结果的 Clone、排序和 Hash 同时纳入停止原因与追加字节数；同一字节数
// 的 EOF 和 LimitReached 不得判等。
#[test]
fn test_read_growth_outcome_clone_order_and_hash_include_stop_reason() {
    let eof_two = ReadGrowthOutcome::EndOfFile { appended_bytes: 2 };
    let same = eof_two.clone();
    let eof_three = ReadGrowthOutcome::EndOfFile { appended_bytes: 3 };
    let limit_zero = ReadGrowthOutcome::LimitReached { appended_bytes: 0 };
    let limit_one = ReadGrowthOutcome::LimitReached { appended_bytes: 1 };

    assert_eq!(eof_two, same);
    assert!(eof_two < eof_three);
    assert!(eof_three < limit_zero, "停止原因顺序优先于字节数");
    assert!(limit_zero < limit_one);

    let mut eof_hasher = DefaultHasher::new();
    eof_two.hash(&mut eof_hasher);
    let mut same_hasher = DefaultHasher::new();
    same.hash(&mut same_hasher);
    assert_eq!(eof_hasher.finish(), same_hasher.finish());
}

// 验证两类停止原因的 Display／Debug 都携带已提交字节数，并使用不同、稳定
// 的文本，供日志在不访问 buffer 时仍能判断终止原因。
#[test]
fn test_read_growth_outcome_formats_each_stop_reason() {
    let eof = ReadGrowthOutcome::EndOfFile { appended_bytes: 3 };
    let limit = ReadGrowthOutcome::LimitReached { appended_bytes: 5 };

    assert_eq!(eof.to_string(), "end of file after appending 3 bytes");
    assert_eq!(
        limit.to_string(),
        "growth limit reached after appending 5 bytes"
    );
    assert_eq!(
        format!("{eof:?}"),
        "EndOfFile { appended_bytes: 3 }"
    );
    assert_eq!(
        format!("{limit:?}"),
        "LimitReached { appended_bytes: 5 }"
    );
}

// 验证结果可以无损表达零进度与 `usize::MAX` 进度，不把零解释为 EOF，也不
// 对最大值做隐式缩小或加一。
#[test]
fn test_read_growth_outcome_preserves_zero_and_usize_maximum_progress() {
    let zero = ReadGrowthOutcome::LimitReached { appended_bytes: 0 };
    let maximum = ReadGrowthOutcome::EndOfFile {
        appended_bytes: usize::MAX,
    };

    assert_eq!(zero.appended_bytes(), 0);
    assert_eq!(maximum.appended_bytes(), usize::MAX);
}
