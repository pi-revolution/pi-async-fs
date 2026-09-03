//! 跨进程协调根配置类型与进程级冻结入口的契约测试。
//!
//! 全局配置只能在一个进程中冻结一次，因此需要重新启动当前测试二进制隔离
//! 各用例；本文件验证值语义、输入校验、幂等性和多线程线性化结果。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::process::Command;

use pi_async_fs::{
    set_cross_process_coordination_root, CrossProcessCoordinationRoot,
};
use pi_result::ErrorKind;

// 标识隔离子进程应执行的全局配置场景。
const CHILD_CASE: &str = "PI_ASYNC_FS_COORDINATION_ROOT_TEST_CASE";

// 用标准哈希器比较相等配置值必须产生一致哈希的公开值语义。
fn hash_of(value: &CrossProcessCoordinationRoot) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

// 生成不要求实际存在、但在 Windows 与 Unix 上都具有完整根前缀的测试路径。
fn absolute_test_path(name: &str) -> PathBuf {
    std::env::current_dir()
        .expect("test current directory")
        .join(name)
}

// 在干净进程中运行一个精确测试，避免进程级 OnceLock 在用例间共享状态。
fn run_current_test_in_child(test_name: &str, case: &str) {
    let output = Command::new(std::env::current_exe().expect("test executable"))
        .arg(test_name)
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_CASE, case)
        .output()
        .expect("spawn isolated test process");
    assert!(
        output.status.success(),
        "isolated child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
// 验证默认值、调试/显示格式以及 Eq、Ord、Hash 的一致公开值语义。
fn test_cross_process_coordination_root_value_semantics() {
    let default = CrossProcessCoordinationRoot::default();
    let file_parent = CrossProcessCoordinationRoot::FileParent;
    let path = absolute_test_path("coord-a");
    let later_path = absolute_test_path("coord-b");
    let custom = CrossProcessCoordinationRoot::Custom(path.clone());
    let same_custom = CrossProcessCoordinationRoot::Custom(path.clone());
    let later_custom = CrossProcessCoordinationRoot::Custom(later_path);

    assert_eq!(default, file_parent);
    assert_eq!(format!("{default:?}"), "FileParent");
    assert_eq!(default.to_string(), "target file parent");
    assert_eq!(format!("{custom:?}"), format!("Custom({path:?})"));
    assert_eq!(custom.to_string(), format!("custom root: {}", path.display()));
    assert_eq!(custom, same_custom);
    assert_ne!(custom, later_custom);
    assert!(file_parent < custom);
    assert!(custom < later_custom);
    assert_eq!(hash_of(&custom), hash_of(&same_custom));
}

#[test]
// 验证自定义根必须为绝对非空路径，首次有效值冻结且同值设置幂等。
fn test_set_cross_process_coordination_root_validates_then_freezes_once() {
    const TEST_NAME: &str =
        "test_set_cross_process_coordination_root_validates_then_freezes_once";
    if std::env::var(CHILD_CASE).as_deref() != Ok("freeze") {
        run_current_test_in_child(TEST_NAME, "freeze");
        return;
    }

    for invalid in [PathBuf::new(), PathBuf::from("relative/root")] {
        let error = set_cross_process_coordination_root(
            CrossProcessCoordinationRoot::Custom(invalid),
        )
        .expect_err("empty and relative custom roots are invalid");
        assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
    }

    let accepted = absolute_test_path("pi-async-fs-coordination-a");
    set_cross_process_coordination_root(
        CrossProcessCoordinationRoot::Custom(accepted.clone()),
    )
    .expect("first valid configuration freezes the process value");
    set_cross_process_coordination_root(
        CrossProcessCoordinationRoot::Custom(accepted),
    )
    .expect("the same representation is idempotent");

    for conflicting in [
        CrossProcessCoordinationRoot::FileParent,
        CrossProcessCoordinationRoot::Custom(absolute_test_path(
            "pi-async-fs-coordination-b",
        )),
    ] {
        let error = set_cross_process_coordination_root(conflicting)
            .expect_err("a frozen process root cannot be changed");
        assert_eq!(error.current_context(), &ErrorKind::Conflict);
    }
}

#[test]
// 验证两个候选配置并发竞争时只有一种完整表示获胜，失败统一为冲突。
fn test_set_cross_process_coordination_root_is_linearizable_across_threads() {
    const TEST_NAME: &str =
        "test_set_cross_process_coordination_root_is_linearizable_across_threads";
    if std::env::var(CHILD_CASE).as_deref() != Ok("concurrent") {
        run_current_test_in_child(TEST_NAME, "concurrent");
        return;
    }

    let start = std::sync::Arc::new(std::sync::Barrier::new(32));
    let roots = [
        absolute_test_path("pi-async-fs-root-even"),
        absolute_test_path("pi-async-fs-root-odd"),
    ];
    let threads = (0..32)
        .map(|index| {
            let start = std::sync::Arc::clone(&start);
            let root = roots[index % 2].clone();
            std::thread::spawn(move || {
                start.wait();
                (index % 2, set_cross_process_coordination_root(
                    CrossProcessCoordinationRoot::Custom(root),
                ))
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .map(|thread| thread.join().expect("configuration thread"))
        .collect::<Vec<_>>();

    let even_successes = results
        .iter()
        .filter(|(parity, result)| *parity == 0 && result.is_ok())
        .count();
    let odd_successes = results
        .iter()
        .filter(|(parity, result)| *parity == 1 && result.is_ok())
        .count();
    assert!(
        (even_successes == 16 && odd_successes == 0)
            || (even_successes == 0 && odd_successes == 16),
        "exactly one complete configuration representation must win"
    );
    for (_, error) in results.into_iter().filter_map(|(parity, result)| {
        result.err().map(|error| (parity, error))
    }) {
        assert_eq!(error.current_context(), &ErrorKind::Conflict);
    }
}
