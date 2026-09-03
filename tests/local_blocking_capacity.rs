//! 本地阻塞准入容量的进程级公开配置集成测试。
//!
//! 每个场景都在独立测试子进程中执行，避免一次性全局配置在不同场景之间共享。
//! 测试只通过公开配置函数与真实目录流观察配置合同，不读取或修改私有信号量。

use std::num::NonZeroUsize;
use std::process::Command;

use futures_lite::future::block_on;
use pi_async_fs::{
    FileNamespace, LocalFileNamespace, set_local_blocking_capacity,
};
use pi_result::ErrorKind;

const CHILD_SCENARIO: &str = "PI_ASYNC_FS_BLOCKING_CAPACITY_CHILD";

// 把每项进程级配置验证隔离到当前测试二进制的新进程；返回 true 表示当前已经
// 位于目标子进程，false 表示父进程已经验证子进程成功并应结束本次测试。
fn enter_isolated_scenario(test_name: &str) -> bool {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(test_name) {
        return true;
    }

    let output = Command::new(
        std::env::current_exe().expect("应能定位当前测试二进制"),
    )
    .arg("--exact")
    .arg(test_name)
    .arg("--nocapture")
    .env(CHILD_SCENARIO, test_name)
    .output()
    .expect("应能启动隔离测试子进程");

    assert!(
        output.status.success(),
        "隔离测试子进程失败\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    false
}

#[test]
// 验证未显式配置时，第一次真实受控目录操作冻结当前默认容量 1000；随后设置
// 相同值幂等成功，不同值明确冲突且不能替换已经生效的配置。
fn test_local_blocking_capacity_defaults_to_one_thousand_and_freezes_on_use() {
    const TEST_NAME: &str =
        "test_local_blocking_capacity_defaults_to_one_thousand_and_freezes_on_use";
    if !enter_isolated_scenario(TEST_NAME) {
        return;
    }

    let temporary = tempfile::tempdir().expect("应能建立真实临时目录");
    let namespace = LocalFileNamespace::new();
    let stream = block_on(namespace.read_dir(temporary.path().to_path_buf()))
        .expect("默认容量应允许建立真实目录流");
    drop(stream);

    set_local_blocking_capacity(
        NonZeroUsize::new(1000).expect("1000 必须是非零容量"),
    )
    .expect("与已经冻结的默认容量相同应幂等成功");

    let conflict = set_local_blocking_capacity(
        NonZeroUsize::new(999).expect("999 必须是非零容量"),
    )
    .expect_err("不同容量不得替换已经冻结的进程级配置");
    assert_eq!(conflict.current_context(), &ErrorKind::Conflict);

    set_local_blocking_capacity(
        NonZeroUsize::new(1000).expect("1000 必须是非零容量"),
    )
    .expect("冲突尝试不得破坏原配置");
}

#[test]
// 验证仅取得 namespace 句柄不会抢先冻结默认容量；调用方可以在第一次受控
// 文件操作前安装非默认值，之后所有操作和幂等重复设置都观察同一配置。
fn test_local_blocking_capacity_can_be_configured_after_namespace_creation_before_io() {
    const TEST_NAME: &str =
        "test_local_blocking_capacity_can_be_configured_after_namespace_creation_before_io";
    if !enter_isolated_scenario(TEST_NAME) {
        return;
    }

    let namespace = LocalFileNamespace::new();
    let explicit = NonZeroUsize::new(7).expect("7 必须是非零容量");
    set_local_blocking_capacity(explicit)
        .expect("第一次受控文件操作前应接受显式容量");
    set_local_blocking_capacity(explicit)
        .expect("相同显式容量应幂等成功");

    let temporary = tempfile::tempdir().expect("应能建立真实临时目录");
    let stream = block_on(namespace.read_dir(temporary.path().to_path_buf()))
        .expect("显式容量应供之后的真实目录流使用");
    drop(stream);

    set_local_blocking_capacity(explicit)
        .expect("使用之后重复相同配置仍应成功");
    let conflict = set_local_blocking_capacity(
        NonZeroUsize::new(8).expect("8 必须是非零容量"),
    )
    .expect_err("使用之后不得替换显式容量");
    assert_eq!(conflict.current_context(), &ErrorKind::Conflict);
}

#[test]
// 验证两个线程首次设置不同容量时只有一个值能够线性化成为进程配置；失败方
// 得到冲突，胜方随后仍可幂等确认，证明不存在部分更新或最后写入者覆盖。
fn test_local_blocking_capacity_concurrent_first_configuration_has_one_winner() {
    const TEST_NAME: &str =
        "test_local_blocking_capacity_concurrent_first_configuration_has_one_winner";
    if !enter_isolated_scenario(TEST_NAME) {
        return;
    }

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
    let mut workers = Vec::new();
    for raw_capacity in [17_usize, 19_usize] {
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            let capacity = NonZeroUsize::new(raw_capacity)
                .expect("测试容量必须非零");
            (raw_capacity, set_local_blocking_capacity(capacity).is_ok())
        }));
    }
    barrier.wait();

    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().expect("配置线程不得 panic"))
        .collect();
    let winners: Vec<_> = results
        .iter()
        .filter_map(|(capacity, succeeded)| succeeded.then_some(*capacity))
        .collect();
    assert_eq!(winners.len(), 1, "不同首次配置必须恰有一个胜方");

    let winner = winners[0];
    let loser = if winner == 17 { 19 } else { 17 };
    set_local_blocking_capacity(
        NonZeroUsize::new(winner).expect("胜方容量必须非零"),
    )
    .expect("胜方配置应保持幂等可见");
    let conflict = set_local_blocking_capacity(
        NonZeroUsize::new(loser).expect("败方容量必须非零"),
    )
    .expect_err("败方配置不得覆盖胜方");
    assert_eq!(conflict.current_context(), &ErrorKind::Conflict);
}
