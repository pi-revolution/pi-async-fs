//! 协调根策略纯值层（EXO-CRV-M01/EXO-CRV-T01）。
//!
//! 只拥有放置策略及可选原生路径，不拥有目录、文件、配置槽或任何资格。
//! 既有配置/布局向下消费本值；本模块只使用标准库，不反向读取全局状态。
//! 默认值、原生路径比较、哈希和诊断保持原行为；标准输出端失败照常传播。
//! 输出端可以有自己的副作用；无本层 I/O 不等于任意输出端固定时延。
//!
//! 合同、CRV-T01~07/CRV-A01~08 及 ORG/DEP/CALL 三类图：
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_COORDINATION_ROOT_VALUE_LAYER_PLAN_2026-10-10.md
//! #exclusive-root-value-layer-api / #exclusive-root-value-layer-validation
//! #exclusive-root-value-layer-organization / #exclusive-root-value-layer-dependencies
//! #exclusive-root-value-layer-calls。

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use std::path::PathBuf;

// 当前进程中所有本地跨进程协调目录的根位置策略。
//
// # 作用与使用位置
//
// 本类型只作为 [`set_cross_process_coordination_root`] 的全局配置值，以及
// 内部协调模块冻结后的只读策略。它不会出现在文件创建、打开、读取、追加、
// 刷新或映射方法的参数中。这样可以保证同一进程中的不同 namespace、文件
// 资源和映射不会各自选择互不相干的锁域。
//
// [`Self::FileParent`] 是默认策略：每个目标文件在与自身所在的同级目录下拥有专属
// 协调目录。[`Self::Custom`] 则让所有需要标准本地跨进程协调的目标文件都
// 在同一个物理根目录下派生各自的协调子目录。调用方提供的路径只是根目录，
// 不是最终锁文件、租约文件或目标文件专属子目录的完整路径。
//
// # 全局冻结与跨进程一致性
//
// 当前进程中的策略由显式设置或首次文件创建/打开/复制的首poll原子地冻结。
// 冻结后不能从 `FileParent` 切换到 `Custom`，也不能更换自定义根目录；按本
// 类型判等的同配置重复设置可以成功。此处的“全局”只覆盖当前进程加载的
// 这一份库状态，不能自动传播给其它进程或同一进程中的另一份库副本。
//
// 所有合作进程必须分别选择能够解析到相同物理锁域的策略。不同进程选择
// 不同自定义根目录不会被进程内静态状态自动发现，并会破坏
// [`CrossProcessFileAuthority`] 所依赖的外部安全承诺。
//
// # 自定义根目录
//
// `Custom` 中的路径必须拥有、非空并采用当前平台的绝对路径表示。公开枚举
// 允许先承载尚未验证的 `PathBuf`；全局设置函数只进行无需 I/O 的结构检查，
// 实际取得文件访问资格或建立协调授权时才打开目录并核验存在性、目录类型、权限、文件系统
// 能力、稳定目录身份以及符号链接或目录联接点限制。
//
// 调用方负责预先创建自定义根目录。本库可以在其中创建目标文件专属协调
// 子目录和协议文件，但不会递归创建调用方给出的根目录。根目录失效时不能
// 悄悄退回 `FileParent`，也不能在活动资源存续期间自动重新绑定到同名的新
// 目录。
//
// # 所有权与基本能力
//
// 本类型刻意不实现 `Clone` 或 `Copy`；设置操作按值取得并长期拥有唯一配置
// 值。它实现 `Default`，因为本项目已经明确冻结 `FileParent` 为唯一默认
// 策略。判等、全序和哈希只比较配置表示，不证明两个不同路径指向相同物理
// 目录；哈希值也不是稳定存储编码或跨进程协议标识。
//
// `Send` 与 `Sync` 由字段自动获得，不使用手写 `unsafe impl`。本类型不声明
// 稳定 ABI、序列化格式、`AsRef<Path>` 或无条件 `From<PathBuf>` 转换；前者
// 无法合理表达无路径的 `FileParent`，后者会让无效相对路径看似已通过验证。
/// 当前进程中本地跨进程协调数据的全局放置策略。
///
/// 策略必须在第一次文件创建、打开或复制被轮询之前设置；首次尝试即冻结，不能在进程
/// 存续期间切换。所有合作进程必须选择能够解析到同一物理协调位置的策略。
/// 本类型只描述根位置，不是某个文件的协调数据完整路径。
pub enum CrossProcessCoordinationRoot {
    // 在与每个目标文件所在的同级目录中保存其专属协调目录。
    //
    // 这是进程尚未显式设置策略时，首次资源尝试自动冻结的默认值。不同父
    // 目录中的文件使用不同物理根目录，但都遵守同一个全局放置规则。
    /// 在与目标文件所在的同级目录中放置该目标的协调数据。
    ///
    /// 这是未显式设置全局策略时使用的默认值。
    FileParent,

    // 在指定全局根目录下保存所有目标文件各自的协调子目录。
    //
    // 路径必须是非空绝对路径，并由调用方在首次资源取得前创建。所有合作
    // 进程必须解析到同一个物理目录；仅仅具有相同字符串表示并不足以证明
    // 它们共享锁域。
    /// 在指定的全局根目录下放置所有目标的协调数据。
    ///
    /// 路径必须是非空绝对路径，并应在首次资源取得前由调用方创建。所有合作
    /// 进程必须解析到同一物理目录；相同文本不单独证明这一点。
    Custom(PathBuf),
}

// 默认策略是冻结的公开语义；保留显式实现和就地说明，不让派生写法隐藏它。
#[allow(clippy::derivable_impls)]
impl Default for CrossProcessCoordinationRoot {
    // 返回 [`CrossProcessCoordinationRoot::FileParent`] 默认策略。
    fn default() -> Self {
        Self::FileParent
    }
}

impl fmt::Debug for CrossProcessCoordinationRoot {
    // 显示策略变体以及自定义根目录的调试表示，不访问文件系统。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileParent => formatter.write_str("FileParent"),
            Self::Custom(path) => formatter
                .debug_tuple("Custom")
                .field(path)
                .finish(),
        }
    }
}

impl fmt::Display for CrossProcessCoordinationRoot {
    // 显示供人阅读的放置策略，不把输出承诺为可反向解析的配置格式。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FileParent => formatter.write_str("target file parent"),
            Self::Custom(path) => {
                write!(formatter, "custom root: {}", path.display())
            }
        }
    }
}

impl PartialEq for CrossProcessCoordinationRoot {
    // 按策略变体和 `PathBuf` 的值语义判等，不解析物理目录身份。
    // 标准路径片段比较可忽略部分形式差异，不是原生文本逐字节比较。
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::FileParent, Self::FileParent) => true,
            (Self::Custom(left), Self::Custom(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for CrossProcessCoordinationRoot {}

impl PartialOrd for CrossProcessCoordinationRoot {
    // 返回与 [`Ord`] 一致的配置表示全序。
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CrossProcessCoordinationRoot {
    // 仅为确定性诊断和容器索引比较配置表示，不表达策略优先级。
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::FileParent, Self::FileParent) => Ordering::Equal,
            (Self::FileParent, Self::Custom(_)) => Ordering::Less,
            (Self::Custom(_), Self::FileParent) => Ordering::Greater,
            (Self::Custom(left), Self::Custom(right)) => left.cmp(right),
        }
    }
}

impl Hash for CrossProcessCoordinationRoot {
    // 按与判等一致的配置表示向调用方哈希器写入状态。
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::FileParent => 0_u8.hash(state),
            Self::Custom(path) => {
                1_u8.hash(state);
                path.hash(state);
            }
        }
    }
}

#[cfg(test)]
mod root_value_tests {
    // EXO-CRV-T01，CRV-T01~06/CRV-A03~04：只经过根策略公开值和标准 trait。
    // 冻结方案与 ORG/DEP/CALL 三图：
    // .scratch/pi-async-fs-mainline/EXCLUSIVE_COORDINATION_ROOT_VALUE_LAYER_PLAN_2026-10-10.md
    // #exclusive-root-value-layer-validation / #exclusive-root-value-layer-diagrams。
    // 不读取全局槽、不打开文件；迁移前后沿用同一断言，不制造人为红线。
    use super::CrossProcessCoordinationRoot;
    use core::fmt;
    use std::collections::{hash_map::DefaultHasher, BTreeMap, HashMap};
    use std::hash::{Hash, Hasher};
    use std::path::PathBuf;

    // 标准哈希消费者；只核验相等键一致性，不冻结哈希字节或排除碰撞。
    fn root_value_hash(value: &CrossProcessCoordinationRoot) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }

    // 只作为标准格式化输出消费者：容量不足时保留已接收字节并返回 fmt::Error。
    // 它不代替配置、目录或任何文件系统生产功能。
    struct BoundedOutput {
        bytes: [u8; 128],
        len: usize,
        limit: usize,
        failed: bool,
        writes_after_failure: usize,
    }

    impl fmt::Write for BoundedOutput {
        fn write_str(&mut self, value: &str) -> fmt::Result {
            if self.failed {
                self.writes_after_failure += 1;
                return Err(fmt::Error);
            }
            let accepted = (self.limit - self.len).min(value.len());
            self.bytes[self.len..self.len + accepted]
                .copy_from_slice(&value.as_bytes()[..accepted]);
            self.len += accepted;
            if accepted != value.len() {
                self.failed = true;
                Err(fmt::Error)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    // CRV-T01：默认值固定；纯值按原生表示保存路径，不提前验证或规范化。
    fn test_root_value_default_and_custom_preserve_unvalidated_native_paths() {
        assert_eq!(
            CrossProcessCoordinationRoot::default(),
            CrossProcessCoordinationRoot::FileParent,
        );
        #[cfg(unix)]
        let absolute = PathBuf::from("/root-value/not-created/协调根");
        #[cfg(windows)]
        let absolute = PathBuf::from(r"C:\root-value\not-created\协调根");
        let paths = [
            PathBuf::new(),
            PathBuf::from("relative/./root/../leaf"),
            absolute,
            PathBuf::from("long-native-path-".repeat(1024)),
            PathBuf::from("contains\0nul"),
        ];
        for path in paths {
            let expected = path.as_os_str().to_owned();
            let value = CrossProcessCoordinationRoot::Custom(path);
            match value {
                CrossProcessCoordinationRoot::Custom(actual) => {
                    assert_eq!(actual.as_os_str(), expected.as_os_str());
                }
                CrossProcessCoordinationRoot::FileParent => {
                    panic!("custom path must preserve its variant");
                }
            }
        }
    }

    #[test]
    // CRV-T02：固定排序样例覆盖全部变体配对；复杂片段只委托标准路径参考。
    fn test_root_value_equality_order_and_partial_order_cover_variants() {
        let values = [
            (CrossProcessCoordinationRoot::FileParent, 0),
            (CrossProcessCoordinationRoot::FileParent, 0),
            (CrossProcessCoordinationRoot::Custom(PathBuf::new()), 1),
            (CrossProcessCoordinationRoot::Custom(PathBuf::from("a")), 2),
            (CrossProcessCoordinationRoot::Custom(PathBuf::from("a")), 2),
            (CrossProcessCoordinationRoot::Custom(PathBuf::from("b")), 3),
        ];
        for (left, left_rank) in &values {
            for (right, right_rank) in &values {
                let expected = left_rank.cmp(right_rank);
                assert_eq!(left == right, left_rank == right_rank);
                assert_eq!(left != right, left_rank != right_rank);
                assert_eq!(left.cmp(right), expected);
                assert_eq!(left.partial_cmp(right), Some(expected));
            }
        }
        for (left_text, right_text) in [
            ("root/./leaf", "root/leaf"),
            ("root//leaf/", "root/leaf"),
            ("root/../leaf", "leaf"),
            ("协调根/甲", "协调根/乙"),
        ] {
            let left_path = PathBuf::from(left_text);
            let right_path = PathBuf::from(right_text);
            let expected_equal = left_path == right_path;
            let expected_order = left_path.cmp(&right_path);
            let left = CrossProcessCoordinationRoot::Custom(left_path);
            let right = CrossProcessCoordinationRoot::Custom(right_path);
            assert_eq!(left == right, expected_equal);
            assert_eq!(left.cmp(&right), expected_order);
            assert_eq!(left.partial_cmp(&right), Some(expected_order));
        }
    }

    #[test]
    // CRV-T03：相等键具有相同哈希；两类真实标准容器取回独立给定的值。
    fn test_root_value_hash_and_ordered_containers_preserve_keys() {
        let mut hashed = HashMap::new();
        let mut ordered = BTreeMap::new();
        assert_eq!(hashed.insert(CrossProcessCoordinationRoot::FileParent, 7), None);
        assert_eq!(ordered.insert(CrossProcessCoordinationRoot::FileParent, 7), None);
        for (text, expected) in [("", 11), ("a", 13), ("b", 17)] {
            let first = CrossProcessCoordinationRoot::Custom(PathBuf::from(text));
            let second = CrossProcessCoordinationRoot::Custom(PathBuf::from(text));
            assert_eq!(root_value_hash(&first), root_value_hash(&second));
            assert_eq!(hashed.insert(first, expected), None);
            assert_eq!(ordered.insert(second, expected), None);
        }
        assert_eq!(
            root_value_hash(&CrossProcessCoordinationRoot::FileParent),
            root_value_hash(&CrossProcessCoordinationRoot::default()),
        );
        assert_eq!(hashed.get(&CrossProcessCoordinationRoot::FileParent), Some(&7));
        assert_eq!(ordered.get(&CrossProcessCoordinationRoot::FileParent), Some(&7));
        for (text, expected) in [("", 11), ("a", 13), ("b", 17)] {
            let key = CrossProcessCoordinationRoot::Custom(PathBuf::from(text));
            assert_eq!(hashed.get(&key), Some(&expected));
            assert_eq!(ordered.get(&key), Some(&expected));
        }
        let replacement = CrossProcessCoordinationRoot::Custom(PathBuf::from("a"));
        assert_eq!(hashed.insert(replacement, 19), Some(13));
        assert_eq!(hashed.len(), 4);
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered.values().copied().collect::<Vec<_>>(), [7, 11, 13, 17]);
        assert_eq!(
            hashed.get(&CrossProcessCoordinationRoot::Custom(PathBuf::from("a"))),
            Some(&19),
        );
    }

    #[test]
    // CRV-T04：普通格式用固定文本；复杂原生路径和美化输出对照标准路径格式。
    fn test_root_value_debug_and_display_preserve_diagnostic_text() {
        let cases = [
            (
                CrossProcessCoordinationRoot::FileParent,
                "FileParent",
                "target file parent",
            ),
            (
                CrossProcessCoordinationRoot::Custom(PathBuf::new()),
                "Custom(\"\")",
                "custom root: ",
            ),
            (
                CrossProcessCoordinationRoot::Custom(PathBuf::from("coord/root")),
                "Custom(\"coord/root\")",
                "custom root: coord/root",
            ),
            (
                CrossProcessCoordinationRoot::Custom(PathBuf::from("协调根")),
                "Custom(\"协调根\")",
                "custom root: 协调根",
            ),
        ];
        for (value, expected_debug, expected_display) in cases {
            assert_eq!(format!("{value:?}"), expected_debug);
            assert_eq!(value.to_string(), expected_display);
            assert_eq!(format!("[{value:>50.4}]"), format!("[{expected_display}]"));
        }
        let path = PathBuf::from("quote\"newline\nleaf\tend");
        let expected_debug = format!("Custom({path:?})");
        let expected_pretty = format!("Custom(\n    {path:#?},\n)");
        let expected_display = format!("custom root: {}", path.display());
        let value = CrossProcessCoordinationRoot::Custom(path);
        assert_eq!(format!("{value:?}"), expected_debug);
        assert_eq!(format!("{value:#?}"), expected_pretty);
        assert_eq!(value.to_string(), expected_display);
    }

    #[test]
    // CRV-T04：逐字节容量覆盖立即失败、部分输出、恰好成功和多余容量。
    // 固定预期独立于格式实现；初始前缀和错误后的停止语义都必须保持。
    fn test_root_value_formatting_propagates_errors_without_erasing_prefix() {
        const PREFIX: &[u8] = b"prefix:";
        let cases = [
            (
                CrossProcessCoordinationRoot::FileParent,
                "FileParent",
                "target file parent",
            ),
            (
                CrossProcessCoordinationRoot::Custom(PathBuf::from("root")),
                "Custom(\"root\")",
                "custom root: root",
            ),
        ];
        for (value, expected_debug, expected_display) in &cases {
            for (debug, expected) in [(true, expected_debug), (false, expected_display)] {
                for budget in 0..=expected.len() + 1 {
                    let mut output = BoundedOutput {
                        bytes: [0; 128],
                        len: PREFIX.len(),
                        limit: PREFIX.len() + budget,
                        failed: false,
                        writes_after_failure: 0,
                    };
                    output.bytes[..PREFIX.len()].copy_from_slice(PREFIX);
                    let result = if debug {
                        fmt::write(&mut output, format_args!("{value:?}"))
                    } else {
                        fmt::write(&mut output, format_args!("{value}"))
                    };
                    let accepted = budget.min(expected.len());
                    assert_eq!(result.is_ok(), budget >= expected.len());
                    assert_eq!(output.failed, budget < expected.len());
                    assert_eq!(output.writes_after_failure, 0);
                    assert_eq!(output.len, PREFIX.len() + accepted);
                    assert_eq!(&output.bytes[..PREFIX.len()], PREFIX);
                    assert_eq!(
                        &output.bytes[PREFIX.len()..output.len],
                        &expected.as_bytes()[..accepted],
                    );
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    // CRV-T05：安全构造真实非 UTF-8 原生路径；只观察值，不要求 OS 可打开。
    fn test_root_value_preserves_non_unicode_native_paths() {
        use std::ffi::OsString;
        use std::os::unix::ffi::{OsStrExt, OsStringExt};

        for units in [vec![0xff], vec![0x80], vec![b'/', b'r', 0xc0, b'/', 0xff]] {
            let path = PathBuf::from(OsString::from_vec(units.clone()));
            let expected = PathBuf::from(OsString::from_vec(units.clone()));
            let expected_debug = format!("Custom({expected:?})");
            let expected_display = format!("custom root: {}", expected.display());
            let value = CrossProcessCoordinationRoot::Custom(path);
            let CrossProcessCoordinationRoot::Custom(actual) = &value else {
                panic!("native path must remain a custom root");
            };
            assert!(actual.to_str().is_none());
            assert_eq!(actual.as_os_str().as_bytes(), units.as_slice());
            let same = CrossProcessCoordinationRoot::Custom(expected);
            assert_eq!(value, same);
            assert_eq!(value.cmp(&same), core::cmp::Ordering::Equal);
            assert_eq!(root_value_hash(&value), root_value_hash(&same));
            assert_eq!(format!("{value:?}"), expected_debug);
            assert_eq!(value.to_string(), expected_display);
        }
    }

    #[test]
    // CRV-T06：真实拥有型跨线程移动和四线程共享借用；精确核验 64 次结果。
    // 不克隆策略值、不新增同步包装或 unsafe impl；这不是性能/压力证据。
    fn test_root_value_moves_and_shares_across_threads_without_changing_value() {
        let value = CrossProcessCoordinationRoot::Custom(PathBuf::from("thread/root"));
        let value = std::thread::spawn(move || {
            assert_eq!(value.to_string(), "custom root: thread/root");
            value
        })
        .join()
        .expect("owned root thread must finish");
        let expected_hash = root_value_hash(&value);
        let results = std::thread::scope(|scope| {
            let threads = (0..4)
                .map(|_| {
                    let borrowed = &value;
                    scope.spawn(move || {
                        let mut completed = 0;
                        for _ in 0..16 {
                            assert_eq!(borrowed.to_string(), "custom root: thread/root");
                            assert_eq!(format!("{borrowed:?}"), "Custom(\"thread/root\")");
                            assert_eq!(root_value_hash(borrowed), expected_hash);
                            completed += 1;
                        }
                        completed
                    })
                })
                .collect::<Vec<_>>();
            threads
                .into_iter()
                .map(|thread| thread.join().expect("shared root thread must finish"))
                .collect::<Vec<_>>()
        });
        assert_eq!(results, [16, 16, 16, 16]);
        assert_eq!(results.iter().sum::<usize>(), 64);
        assert_eq!(
            value,
            CrossProcessCoordinationRoot::Custom(PathBuf::from("thread/root")),
        );
        assert_eq!(root_value_hash(&value), expected_hash);
    }

    #[cfg(windows)]
    #[test]
    // CRV-T05：未配对 UTF-16 单元必须原样保留；损失显示不是路径权威值。
    fn test_root_value_preserves_non_unicode_native_paths() {
        use std::ffi::OsString;
        use std::os::windows::ffi::{OsStrExt, OsStringExt};

        for units in [
            vec![0xd800],
            vec![0xdc00],
            vec![0x0043, 0x003a, 0x005c, 0xd800, 0xffff, 0x005c, 0xdc00],
        ] {
            let path = PathBuf::from(OsString::from_wide(&units));
            let expected = PathBuf::from(OsString::from_wide(&units));
            let expected_debug = format!("Custom({expected:?})");
            let expected_display = format!("custom root: {}", expected.display());
            let value = CrossProcessCoordinationRoot::Custom(path);
            let CrossProcessCoordinationRoot::Custom(actual) = &value else {
                panic!("native path must remain a custom root");
            };
            assert!(actual.to_str().is_none());
            assert_eq!(actual.as_os_str().encode_wide().collect::<Vec<_>>(), units);
            let same = CrossProcessCoordinationRoot::Custom(expected);
            assert_eq!(value, same);
            assert_eq!(value.cmp(&same), core::cmp::Ordering::Equal);
            assert_eq!(root_value_hash(&value), root_value_hash(&same));
            assert_eq!(format!("{value:?}"), expected_debug);
            assert_eq!(value.to_string(), expected_display);
        }
    }
}
