//! 协调根配置模块（EXO-RCF-M01/RCF-F01~03）。
//!
//! 本模块唯一拥有进程配置槽；只依赖已验收根策略纯值、标准槽及原错误能力。
//! 三入口保持原签名/函数体；不打开物理根、不取得文件锁/资格、不反向调用local。
//! set/get_or_init可能同步等待初始化，get只观察；默认闭包不得访问I/O或高层。
//! 发布的配置不因上层失败/取消回滚，static纯值借用不保活物理目录。
//! 配置模块只提供固定出口；普通创建/打开/复制在PSA冷取得首poll调用，
//! 固定时序由该消费层负责，不给数据操作热路径增加步骤。
//!
//! 完整合同、RCF-T01~05/RCF-A01~08及ORG/DEP/CALL三图：
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_COORDINATION_ROOT_CONFIG_LAYER_PLAN_2026-10-10.md
//! #exclusive-root-config-layer-api / #exclusive-root-config-layer-validation
//! #exclusive-root-config-layer-organization / #exclusive-root-config-layer-dependencies
//! #exclusive-root-config-layer-calls。

use std::sync::OnceLock;

use crate::coordination_root::CrossProcessCoordinationRoot;

// 为当前进程安装本地跨进程协调目录的全局根位置策略。
//
// # 调用目的与作用域
//
// 应用使用本函数在任何本地文件资源创建、打开或复制开始前，统一决定全部标准本地
// adapter 的协调目录放置方式。配置覆盖当前进程中共享这份库全局状态的
// namespace 和文件资源，不会传播到其它进程，也不影响采用独立协议的远端
// 后端。
//
// 不调用本函数时，首次文件资源创建、打开或复制的首poll会原子地冻结
// [`CrossProcessCoordinationRoot::FileParent`]。文件创建、打开和映射接口不会
// 再提供逐文件覆盖参数。
//
// # 参数、所有权与结构验证
//
// `root` 按值进入全局状态，不借用调用方路径，也不要求 `Clone`。本函数对
// `Custom` 只执行无需文件系统 I/O 的结构验证，例如拒绝空路径和相对路径。
// 无效输入返回粗粒度 [`pi_result::ErrorKind::InvalidInput`]，且不会冻结全局
// 状态；调用方可以修正后重新设置。
//
// 本函数不检查目录是否存在，不创建目录，不查询文件系统类型，不解析符号
// 链接或目录联接点，也不取得文件锁。上述可能阻塞且会随时间失效的事实必须
// 在实际取得普通文件访问资格或建立协调授权时重新核验。因此本函数保持同步，并且可以在启动
// 异步运行时之前调用。
//
// # 并发、重复调用与线性化
//
// 多个线程可以并发调用本函数，也可以与首次文件资源取得竞争。实现必须产生
// 唯一线性化结果：只有一个策略可以成为全局值，任何目标文件都不能观察到
// 中途切换。
//
// 与已冻结策略按本类型 `PartialEq` 判等的重复调用返回 `Ok(())`，因此该情况
// 幂等。不相等的策略返回粗粒度 [`pi_result::ErrorKind::Conflict`]，原值不变。
// `Custom` 继承标准 `PathBuf` 的路径片段判等，部分分隔符/点片段等形式差异
// 可能判为相等；具体规则由平台标准路径语义决定，不作原生文本逐字节比较。
// 判等不访问文件系统，也不解析符号链接或其它物理目录别名；按值不相等的
// 两个策略仍会冲突，即使随后可能被系统解析为同一目录。
//
// 本库不提供重置、替换或强制覆盖接口。需要验证多个全局配置的测试必须
// 使用隔离子进程，不能通过测试专用公开接口破坏生产不变量。
//
// # 返回值、错误与 panic
//
// `Ok(())` 只证明配置表示已经成为或本来就是当前全局策略，不证明自定义
// 根目录存在、受支持、安全或可写。完整物理核验失败会由实际资源取得或协调授权
// 的操作报告；实现不得自动退回默认目录。
//
// 所有可报告的输入和配置冲突通过 `pi_result::Error` 返回，精细错误上下文
// 必须由 `thiserror` 定义并按项目统一方式转换。本函数不得因这些条件 panic。
// 全局分配器终止进程等不可恢复行为不属于普通错误保证。
//
// # 副作用与成本
//
// 成功可能永久写入进程级配置，因此本函数不是纯函数；相同配置的重复成功
// 不产生新的配置变化。它不执行文件系统 I/O、不取得文件锁、不调用用户
// 代码。时间成本至多与路径表示长度线性相关，并包含常数次进程内同步；
// 配置冻结后的普通文件 I/O 热路径不得重复复制或比较完整路径。
/// 设置当前进程的跨进程协调根位置策略。
///
/// 相同配置可以重复设置；不同配置与已经冻结的策略冲突。`Custom` 的空路径
/// 或相对路径返回非法输入错误。成功只表示配置已经接受，不证明目标目录
/// 当前存在、可写、可信或具备所需文件系统能力；这些条件会在实际建立协调
/// 关系时核验。本函数不会传播配置到其它进程，也不提供重置接口。
///
/// 相同配置按策略值的判等规则判断，不要求路径文本逐字节相同，也不证明
/// 两个配置指向同一物理目录。
///
/// 必须在任何本地文件创建、打开或复制首次被轮询前设置。未设置时，首次
/// 尝试即固定 [`CrossProcessCoordinationRoot::FileParent`]，即使该尝试失败也
/// 不回滚。只创建未轮询的 Future、查询元信息、枚举目录或查询剩余空间不会
/// 因此固定配置。普通资源使用 `Custom` 时也要求根位置实际可用，不会静默
/// 退回默认位置；参加打开资格检查不等于获得跨进程内容或映射安全保证。
pub fn set_cross_process_coordination_root(
    root: CrossProcessCoordinationRoot,
) -> pi_result::Result<()> {
    if let CrossProcessCoordinationRoot::Custom(path) = &root {
        if path.as_os_str().is_empty() || !path.is_absolute() {
            return Err(pi_result::error_stack::Report::new(
                pi_result::ErrorKind::InvalidInput,
            ));
        }
    }

    match CROSS_PROCESS_COORDINATION_ROOT.set(root) {
        Ok(()) => Ok(()),
        Err(rejected) if CROSS_PROCESS_COORDINATION_ROOT.get() == Some(&rejected) => {
            Ok(())
        }
        Err(_) => Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Conflict,
        )),
    }
}

static CROSS_PROCESS_COORDINATION_ROOT: OnceLock<
    CrossProcessCoordinationRoot,
> = OnceLock::new();

/// RCF-F03：同槽取得真正发布胜者；竞争可能同步等待，只允许纯值Default初始化。
/// static借用不保活物理根，上层失败/取消不回滚配置；RCF-T03/04，调用图RCF-CALL-001。
pub(crate) fn frozen_cross_process_coordination_root(
) -> &'static CrossProcessCoordinationRoot {
    CROSS_PROCESS_COORDINATION_ROOT
        .get_or_init(CrossProcessCoordinationRoot::default)
}

/// RCF-F02：只观察，不安装默认或等待；None不能作为免协调凭据。
/// 返回只读纯值借用，不保存物理资源；RCF-T02/04，调用图RCF-CALL-001。
pub(crate) fn configured_cross_process_coordination_root(
) -> Option<&'static CrossProcessCoordinationRoot> {
    CROSS_PROCESS_COORDINATION_ROOT.get()
}

#[cfg(test)]
mod root_config_tests {
    // EXO-RCF-M01/RCF-F01~03，RCF-T01~05/RCF-A03~04：只调用三个真实配置出口。
    // 合同与独立ORG/DEP/CALL图：.scratch/pi-async-fs-mainline/
    // EXCLUSIVE_COORDINATION_ROOT_CONFIG_LAYER_PLAN_2026-10-10.md
    // #exclusive-root-config-layer-api / #exclusive-root-config-layer-validation
    // #exclusive-root-config-layer-organization / #exclusive-root-config-layer-dependencies
    // #exclusive-root-config-layer-calls。
    // 子进程隔离实际静态配置；不读取/重置槽，不复制实现或注入替代默认值。
    use super::{
        configured_cross_process_coordination_root,
        frozen_cross_process_coordination_root,
        set_cross_process_coordination_root,
    };
    use crate::CrossProcessCoordinationRoot;
    use std::path::PathBuf;
    use std::process::Command;

    const CHILD_CASE: &str = "PI_ASYNC_FS_ROOT_CONFIG_UNIT_CASE";

    // 只隔离测试进程，不代理配置业务；同时核验精确运行一个真实用例，防止空跑。
    fn run_isolated_case(test_name: &str, case: &str) -> bool {
        if std::env::var(CHILD_CASE).as_deref() == Ok(case) {
            return true;
        }
        let (_, module) = module_path!().split_once("::")
            .expect("unit test module includes its crate prefix");
        let output = Command::new(std::env::current_exe().expect("unit test executable"))
            .arg(format!("{module}::{test_name}"))
            .arg("--exact")
            .arg("--test-threads=1")
            .env(CHILD_CASE, case)
            .output()
            .expect("spawn isolated configuration test");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "isolated configuration test failed\n{stdout}\n{stderr}");
        assert!(stdout.contains("running 1 test"), "isolated test must not be empty: {stdout}");
        assert!(stdout.contains("test result: ok. 1 passed"), "one exact test must pass: {stdout}");
        false
    }

    // 只生成本机绝对表示，不建立目录；成功设置不应要求该路径已存在。
    fn root_config_absolute_path(leaf: &str) -> PathBuf {
        std::env::current_dir().expect("test current directory").join(leaf)
    }

    #[test]
    // RCF-T02：观察不冻结、非法不发布；成功后唯一借用/幂等/冲突均经真实出口核验。
    fn test_root_config_observation_and_invalid_input_do_not_publish() {
        if !run_isolated_case(
            "test_root_config_observation_and_invalid_input_do_not_publish",
            "observe-then-custom",
        ) {
            return;
        }
        assert!(configured_cross_process_coordination_root().is_none());
        assert!(configured_cross_process_coordination_root().is_none());
        for path in [PathBuf::new(), PathBuf::from("relative/协调根")] {
            let error = set_cross_process_coordination_root(
                CrossProcessCoordinationRoot::Custom(path),
            )
            .expect_err("invalid custom root must not publish");
            assert_eq!(error.current_context(), &pi_result::ErrorKind::InvalidInput);
            assert!(configured_cross_process_coordination_root().is_none());
        }
        let path = root_config_absolute_path("root-config-observe-协调根");
        set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(path.clone()))
            .expect("observation must leave explicit publication available");
        let observed = configured_cross_process_coordination_root()
            .expect("successful set must be visible");
        assert_eq!(observed, &CrossProcessCoordinationRoot::Custom(path.clone()));
        assert!(std::ptr::eq(observed, frozen_cross_process_coordination_root()));
        set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(path))
            .expect("equal custom configuration is idempotent");
        let error = set_cross_process_coordination_root(CrossProcessCoordinationRoot::FileParent)
            .expect_err("different strategy must not replace the published value");
        assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
        assert!(std::ptr::eq(
            observed,
            configured_cross_process_coordination_root().expect("published value remains"),
        ));
    }

    #[test]
    // RCF-T03：默认固定成功后始终同一实例；重复设置默认幂等，不同Custom冲突。
    fn test_root_config_default_freeze_publishes_one_permanent_value() {
        if !run_isolated_case(
            "test_root_config_default_freeze_publishes_one_permanent_value",
            "freeze-default",
        ) {
            return;
        }
        assert!(configured_cross_process_coordination_root().is_none());
        let frozen = frozen_cross_process_coordination_root();
        assert_eq!(frozen, &CrossProcessCoordinationRoot::FileParent);
        for _ in 0..3 {
            assert!(std::ptr::eq(frozen, frozen_cross_process_coordination_root()));
            assert!(std::ptr::eq(
                frozen,
                configured_cross_process_coordination_root().expect("default is published"),
            ));
        }
        set_cross_process_coordination_root(CrossProcessCoordinationRoot::FileParent)
            .expect("setting the published default is idempotent");
        let error = set_cross_process_coordination_root(CrossProcessCoordinationRoot::Custom(
            root_config_absolute_path("root-config-too-late"),
        ))
        .expect_err("custom configuration must not replace the default winner");
        assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
        assert_eq!(frozen_cross_process_coordination_root(), &CrossProcessCoordinationRoot::FileParent);
        assert!(std::ptr::eq(frozen, frozen_cross_process_coordination_root()));
    }

    #[test]
    // RCF-T04：四显式设置与四默认固定真实竞争；精确检查全部结果和唯一发布实例。
    // 不暂停初始化或伪造槽状态；两种最终胜者都合法，不声称穷举所有调度。
    fn test_root_config_set_and_default_freeze_choose_one_linearizable_winner() {
        if !run_isolated_case(
            "test_root_config_set_and_default_freeze_choose_one_linearizable_winner",
            "set-versus-default",
        ) {
            return;
        }
        assert!(configured_cross_process_coordination_root().is_none());
        let path = root_config_absolute_path("root-config-racing-custom");
        let start = std::sync::Arc::new(std::sync::Barrier::new(8));
        let threads = (0..8)
            .map(|index| {
                let start = std::sync::Arc::clone(&start);
                let candidate = path.clone();
                std::thread::spawn(move || {
                    start.wait();
                    let set_result = (index < 4).then(|| {
                        set_cross_process_coordination_root(
                            CrossProcessCoordinationRoot::Custom(candidate),
                        )
                    });
                    (set_result, frozen_cross_process_coordination_root())
                })
            })
            .collect::<Vec<_>>();
        let results = threads
            .into_iter()
            .map(|thread| thread.join().expect("configuration contender must finish"))
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 8);
        let final_value = configured_cross_process_coordination_root()
            .expect("all completed contenders must observe a published winner");
        let expected_successes = match final_value {
            CrossProcessCoordinationRoot::FileParent => 0,
            CrossProcessCoordinationRoot::Custom(actual) => {
                assert_eq!(actual, &path);
                4
            }
        };
        let mut successes = 0;
        let mut conflicts = 0;
        let mut default_contenders = 0;
        for (set_result, observed) in results {
            assert!(std::ptr::eq(observed, final_value));
            match set_result {
                Some(Ok(())) => successes += 1,
                Some(Err(error)) => {
                    assert_eq!(error.current_context(), &pi_result::ErrorKind::Conflict);
                    conflicts += 1;
                }
                None => default_contenders += 1,
            }
        }
        assert_eq!(successes, expected_successes);
        assert_eq!(conflicts, 4 - expected_successes);
        assert_eq!(successes + conflicts, 4);
        assert_eq!(default_contenders, 4);
        assert!(std::ptr::eq(final_value, frozen_cross_process_coordination_root()));
    }
}
