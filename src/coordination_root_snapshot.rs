//! 协调根快照值模块（EXO-RSV-M01/T01）。
//! 保存取得时实际目录的原生位置与同次身份，不保存句柄或授予资格。
//! 仅向下使用已验收身份纯值和标准路径/值能力，不访问配置、查询、local、锁或任务。
//! 合法生产来源由真实取得者先核验；安全保存过期快照不等于仍可凭它访问目录。
//! 值移动沿用输入存储，Drop只释放字段；不实现Clone/Copy/Default、转换或资源Drop。
//! 全合同、RSV-T01~07/A01~08与独立ORG/DEP/CALL图：.scratch/pi-async-fs-mainline/
//! EXCLUSIVE_COORDINATION_ROOT_SNAPSHOT_LAYER_PLAN_2026-10-10.md
//! #exclusive-root-snapshot-layer-api / #exclusive-root-snapshot-layer-validation
//! #exclusive-root-snapshot-layer-organization / #exclusive-root-snapshot-layer-dependencies
//! #exclusive-root-snapshot-layer-calls。

use crate::file_identity::StableFileIdentity;
use std::path::{Path, PathBuf};

/// 标准全字段值能力：path优先，再identity；允许Hash碰撞，诊断不作为协议。
/// RSV-T02/T03，不引入Clone/Copy/Default或资源/配置行为。
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct CoordinationRootSnapshot {
    // 取得时非空绝对位置；不保证之后相同名字仍指向同一目录，也不是锁文件位置。
    path: PathBuf,
    // 同次真实目录的完整身份；值不保活对象或阻止编号复用，不附加非零要求。
    identity: StableFileIdentity,
}

impl CoordinationRootSnapshot {
    /// EXO-RSV-F01：按path、identity顺序移动既有字段，不扫描/复制/分配/验证来源。
    /// 真实生产者先保证非空绝对及同次目录来源，异常在取得层报告；这里不panic/回调。
    /// 返回Self不证明目录当前有效，不查询/锁/固定配置；同步无取消点，Drop仅字段。
    /// 追踪：上述正式合同F01，RSV-T01/T06、RSV-A02~04及RSV-CALL-001。
    #[must_use]
    pub(crate) fn from_parts(path: PathBuf, identity: StableFileIdentity) -> Self {
        Self { path, identity }
    }

    /// EXO-RSV-F02：只借用已有原生Path视图，寿命绑定self，不重新定位/分配/复制。
    /// 无可写透传/普通错误/主动panic/状态变化；不保活目录或给资格，RSV-T01/T06。
    #[must_use]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// EXO-RSV-F03：只借用同一身份字段，寿命绑定self；不查询/验证/复制或改变状态。
    /// 无普通错误/主动panic；身份有效期/资格仍由上层负责，RSV-T01/T06及CALL-001。
    #[must_use]
    pub(crate) fn identity(&self) -> &StableFileIdentity {
        &self.identity
    }
}

// M15/RSV-T03/T04：借用原字段直接完整写入，路径用标准Debug避免有损转换。
// 外层宽度/精度/#不裁剪；仅传播输出端fmt::Error，前缀不回滚/重试，不主动panic。
// 输出端自己的分配/阻塞/panic另计；本值不查询根/配置/锁或保存输出端。
impl core::fmt::Display for CoordinationRootSnapshot {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            formatter,
            "CoordinationRootSnapshot(path={:?}, identity={})",
            self.path, self.identity,
        )
    }
}

#[cfg(all(test, any(unix, windows)))]
mod root_snapshot_tests {
    // EXO-RSV-T01/F01~03，RSV-T01/A03~04：经真实生产构造/借用出口验证。
    // 本测试层消费L02取得真实来源；生产快照不依赖查询或文件设施。
    // 完整合同/ORG/DEP/CALL图：.scratch/pi-async-fs-mainline/
    // EXCLUSIVE_COORDINATION_ROOT_SNAPSHOT_LAYER_PLAN_2026-10-10.md
    // #exclusive-root-snapshot-layer-api / #exclusive-root-snapshot-layer-validation
    // #exclusive-root-snapshot-layer-organization / #exclusive-root-snapshot-layer-dependencies
    // #exclusive-root-snapshot-layer-calls。
    use super::CoordinationRootSnapshot;
    use crate::file_identity::StableFileIdentity;
    use std::path::{Path, PathBuf};

    // 夹具仅准备真实目录，不实现根取得/校验/资格；小型内容留项目target并自动清理。
    fn fixture() -> tempfile::TempDir {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("root-snapshot-test-fixtures");
        std::fs::create_dir_all(&root).expect("create snapshot fixture root");
        tempfile::Builder::new().prefix("snapshot-")
            .tempdir_in(root).expect("create real snapshot fixture")
    }

    // 身份来自同一真实目录的L02出口，不以路径/内容/手写编号代替查询。
    fn real_directory_parts(path: &Path) -> (PathBuf, StableFileIdentity) {
        let path = std::fs::canonicalize(path).expect("absolute real directory location");
        #[cfg(unix)]
        let identity = crate::file_identity_query::identity_from_metadata(
            &std::fs::metadata(&path).expect("real directory metadata"),
        ).expect("real directory identity");
        #[cfg(windows)]
        let identity = {
            use std::os::windows::fs::OpenOptionsExt;
            use std::os::windows::io::AsHandle;
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
            let directory = std::fs::OpenOptions::new().read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS).open(&path)
                .expect("open real directory");
            crate::file_identity_query::identity_from_handle(directory.as_handle())
                .expect("real directory identity")
        };
        (path, identity)
    }

    #[test]
    // RSV-T01：按值移动不复制路径存储；只读getter借用原字段；Drop记录不删除目录/内容。
    fn test_root_snapshot_moves_real_directory_parts_and_borrows_without_resource_side_effects() {
        let temporary = fixture();
        let file = temporary.path().join("preserved.txt");
        std::fs::write(&file, b"snapshot is not a directory owner").expect("prepare known content");
        let (path, identity) = real_directory_parts(temporary.path());
        let expected_path = path.clone();
        let expected_identity = identity.clone();
        let path_storage = path.as_os_str().as_encoded_bytes().as_ptr();
        let value = CoordinationRootSnapshot::from_parts(path, identity);
        assert_eq!(value.path(), expected_path.as_path());
        assert_eq!(value.identity(), &expected_identity);
        assert_eq!(value.path().as_os_str().as_encoded_bytes().as_ptr(), path_storage);
        assert!(std::ptr::eq(value.path(), value.path()));
        assert!(std::ptr::eq(value.identity(), value.identity()));
        drop(value);
        assert!(temporary.path().is_dir());
        assert_eq!(std::fs::read(&file).expect("directory remains usable"),
            b"snapshot is not a directory owner");
    }

    // 仅产生纯值规律用例，不冒充真实OS返回或合法根取得；RSV-T02/T03数值表示边界。
    fn sample_identity(number: u64) -> StableFileIdentity {
        #[cfg(unix)]
        { StableFileIdentity::Unix { device: 7, inode: number } }
        #[cfg(windows)]
        {
            let mut file_id = [0; 16];
            file_id[0] = number as u8;
            StableFileIdentity::Windows { volume_serial_number: 7, file_id }
        }
    }

    // 测试输出消费者只强制真实HashMap产生碰撞，不实现快照判等/Hash或任何文件逻辑。
    #[derive(Default)]
    struct CollidingHasher;
    impl std::hash::Hasher for CollidingHasher {
        fn finish(&self) -> u64 { 0 }
        fn write(&mut self, _bytes: &[u8]) {}
    }

    #[test]
    // RSV-T02：独立rank表检验全字段/路径优先全序及trait规律；标准等价路径相等。
    // 真实HashMap故意全碰撞仍精确查值，BTreeMap精确排序；纯表示样例不是活根证书。
    fn test_root_snapshot_value_relations_ordering_and_collision_safe_containers() {
        use std::collections::{BTreeMap, HashMap};
        use std::hash::{BuildHasherDefault, Hash, Hasher};
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let values = [
            CoordinationRootSnapshot::from_parts(root.join("a"), sample_identity(1)),
            CoordinationRootSnapshot::from_parts(root.join("a"), sample_identity(2)),
            CoordinationRootSnapshot::from_parts(root.join("b"), sample_identity(1)),
            CoordinationRootSnapshot::from_parts(root.join("b"), sample_identity(2)),
            CoordinationRootSnapshot::from_parts(root.join(".").join("a"), sample_identity(1)),
        ];
        let ranks = [0_u8, 1, 2, 3, 0];
        for (i, left) in values.iter().enumerate() {
            for (j, right) in values.iter().enumerate() {
                assert_eq!(left == right, ranks[i] == ranks[j]);
                assert_eq!(left.cmp(right), ranks[i].cmp(&ranks[j]));
                assert_eq!(left.partial_cmp(right), Some(ranks[i].cmp(&ranks[j])));
                assert_eq!(left.cmp(right), right.cmp(left).reverse());
                if left == right {
                    let mut left_hash = std::collections::hash_map::DefaultHasher::new();
                    let mut right_hash = std::collections::hash_map::DefaultHasher::new();
                    left.hash(&mut left_hash);
                    right.hash(&mut right_hash);
                    assert_eq!(left_hash.finish(), right_hash.finish());
                }
                for other in &values {
                    if left == right && right == other { assert_eq!(left, other); }
                    if left <= right && right <= other { assert!(left <= other); }
                }
            }
        }
        let mut hashed: HashMap<_, _, BuildHasherDefault<CollidingHasher>> = HashMap::default();
        let mut ordered = BTreeMap::new();
        for (index, value) in values.into_iter().enumerate() {
            let second = CoordinationRootSnapshot::from_parts(
                value.path().to_owned(), value.identity().clone(),
            );
            let expected_previous = if index == 4 { Some(0) } else { None };
            assert_eq!(hashed.insert(value, index), expected_previous);
            assert_eq!(ordered.insert(second, index), expected_previous);
        }
        assert_eq!(hashed.len(), 4);
        assert_eq!(ordered.len(), 4);
        assert_eq!(ordered.values().copied().collect::<Vec<_>>(), [4, 1, 2, 3]);
        for (leaf, number, expected) in [("a", 1, 4), ("a", 2, 1), ("b", 1, 2), ("b", 2, 3)] {
            let query = CoordinationRootSnapshot::from_parts(root.join(leaf), sample_identity(number));
            assert_eq!(hashed.get(&query), Some(&expected));
            assert_eq!(ordered.get(&query), Some(&expected));
        }
        let absent = CoordinationRootSnapshot::from_parts(root.join("a"), sample_identity(3));
        assert_eq!(hashed.get(&absent), None);
        assert_eq!(ordered.get(&absent), None);
    }

    // 独立固定字面身份只供纯值诊断，不证明当前OS有对应目录；沿已冻结M15示例。
    fn diagnostic_identity() -> StableFileIdentity {
        #[cfg(unix)]
        { StableFileIdentity::Unix { device: 1, inode: 42 } }
        #[cfg(windows)]
        { StableFileIdentity::Windows {
            volume_serial_number: 1,
            file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        } }
    }

    #[test]
    // RSV-T03/M15：独立完整字面量检查普通诊断；美化必须保持两字段和标准嵌套表示。
    fn test_root_snapshot_diagnostics_follow_complete_frozen_field_formats() {
        #[cfg(unix)]
        let path = PathBuf::from("/data/协调");
        #[cfg(windows)]
        let path = PathBuf::from(r"E:\data\协调");
        let value = CoordinationRootSnapshot::from_parts(path, diagnostic_identity());
        #[cfg(unix)]
        let expected_display = r#"CoordinationRootSnapshot(path="/data/协调", identity=StableFileIdentity::Unix(device=0x0000000000000001, inode=0x000000000000002a))"#;
        #[cfg(windows)]
        let expected_display = r#"CoordinationRootSnapshot(path="E:\\data\\协调", identity=StableFileIdentity::Windows(volume_serial_number=0x0000000000000001, file_id=0x000102030405060708090a0b0c0d0e0f))"#;
        #[cfg(unix)]
        let expected_debug = r#"CoordinationRootSnapshot { path: "/data/协调", identity: Unix { device: 1, inode: 42 } }"#;
        #[cfg(windows)]
        let expected_debug = r#"CoordinationRootSnapshot { path: "E:\\data\\协调", identity: Windows { volume_serial_number: 1, file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] } }"#;
        assert_eq!(format!("{value}"), expected_display);
        assert_eq!(format!("{value:?}"), expected_debug);
        let pretty = format!("{value:#?}");
        assert!(pretty.starts_with("CoordinationRootSnapshot {\n    path: "));
        assert!(pretty.ends_with("\n}"));
        assert!(pretty.contains(&format!("\n    path: {:?},\n", value.path())));
        // 身份的标准美化由已验收身份能力独立产生，再核对实际完整嵌套字段。
        let nested_identity = format!("{:#?}", value.identity())
            .replace('\n', "\n    ");
        assert!(pretty.contains(&format!("    identity: {nested_identity},\n")));
        assert_eq!(value.identity(), &diagnostic_identity());
    }

    #[test]
    // RSV-T03：引号/控制/中文原样保留；空/相对/异常原生数据只测纯表示，不是合法生产根。
    // 零/极值身份只测完整显示，不冒充真实系统查询或资格来源。
    fn test_root_snapshot_native_paths_and_identity_boundaries_remain_complete() {
        #[cfg(unix)]
        let special = PathBuf::from("/data/中文 \"引号\"\\目录\n\t");
        #[cfg(unix)]
        let escaped = r#""/data/中文 \"引号\"\\目录\n\t""#;
        #[cfg(windows)]
        let special = PathBuf::from("E:\\中文 \"引号\"\\目录\n\t");
        #[cfg(windows)]
        let escaped = r#""E:\\中文 \"引号\"\\目录\n\t""#;
        let value = CoordinationRootSnapshot::from_parts(special.clone(), diagnostic_identity());
        assert_eq!(value.path().as_os_str(), special.as_os_str());
        assert!(format!("{value}").starts_with(&format!("CoordinationRootSnapshot(path={escaped}, identity=")));
        assert!(format!("{value:?}").starts_with(&format!("CoordinationRootSnapshot {{ path: {escaped}, identity: ")));

        #[cfg(unix)]
        let native = {
            use std::os::unix::ffi::OsStringExt;
            PathBuf::from(std::ffi::OsString::from_vec(b"/data/\xff\n\0".to_vec()))
        };
        #[cfg(windows)]
        let native = {
            use std::os::windows::ffi::OsStringExt;
            PathBuf::from(std::ffi::OsString::from_wide(&[69, 58, 92, 0xd800, 0x4e2d, 0, 92]))
        };
        for path in [PathBuf::new(), PathBuf::from("relative/中文"), native, PathBuf::from("a".repeat(8192))] {
            let expected_units = path.as_os_str().to_owned();
            // 原生边界的精确转义委托独立标准Path参考，不写测试专用路径编码算法。
            let expected_native_debug = format!("{path:?}");
            let value = CoordinationRootSnapshot::from_parts(path, diagnostic_identity());
            assert_eq!(value.path().as_os_str(), expected_units);
            assert!(format!("{value}").starts_with(&format!(
                "CoordinationRootSnapshot(path={expected_native_debug}, identity=",
            )));
            assert!(format!("{value:?}").starts_with(&format!(
                "CoordinationRootSnapshot {{ path: {expected_native_debug}, identity: ",
            )));
        }

        #[cfg(unix)]
        let cases = [
            (StableFileIdentity::Unix { device: 0, inode: 0 },
                "StableFileIdentity::Unix(device=0x0000000000000000, inode=0x0000000000000000)"),
            (StableFileIdentity::Unix { device: u64::MAX, inode: u64::MAX },
                "StableFileIdentity::Unix(device=0xffffffffffffffff, inode=0xffffffffffffffff)"),
        ];
        #[cfg(windows)]
        let cases = [
            (StableFileIdentity::Windows { volume_serial_number: 0, file_id: [0; 16] },
                "StableFileIdentity::Windows(volume_serial_number=0x0000000000000000, file_id=0x00000000000000000000000000000000)"),
            (StableFileIdentity::Windows { volume_serial_number: u64::MAX, file_id: [255; 16] },
                "StableFileIdentity::Windows(volume_serial_number=0xffffffffffffffff, file_id=0xffffffffffffffffffffffffffffffff)"),
        ];
        for (identity, expected) in cases {
            let value = CoordinationRootSnapshot::from_parts(PathBuf::from("/"), identity);
            assert!(format!("{value}").ends_with(&format!("identity={expected})")));
        }
    }

    // 此输出端只接收/拒绝fmt字节，不实现路径/身份/快照/文件逻辑。
    // bytes可截在UTF-8中间，属于被截断输出证据，不把它当合法String。
    struct LimitedOutput {
        bytes: Vec<u8>,
        limit: usize,
        failed: bool,
        writes_after_failure: usize,
    }
    impl core::fmt::Write for LimitedOutput {
        fn write_str(&mut self, text: &str) -> core::fmt::Result {
            if self.failed {
                self.writes_after_failure += 1;
                return Err(core::fmt::Error);
            }
            let accepted = (self.limit - self.bytes.len()).min(text.len());
            self.bytes.extend_from_slice(&text.as_bytes()[..accepted]);
            if accepted < text.len() {
                self.failed = true;
                Err(core::fmt::Error)
            } else {
                Ok(())
            }
        }
    }

    #[test]
    // RSV-T04：三种真实fmt在每个输出字节边界失败，精确前缀/立即停止/原值不变。
    // 完整文本/精确前缀都来自独立字面量；不用任意长度门槛或生产输出充当预期。
    fn test_root_snapshot_format_errors_preserve_every_prefix_and_stop_immediately() {
        use core::fmt::Write;
        let path = PathBuf::from("/data/协调");
        let value = CoordinationRootSnapshot::from_parts(path.clone(), diagnostic_identity());
        #[cfg(unix)]
        let complete = [
            r#"CoordinationRootSnapshot(path="/data/协调", identity=StableFileIdentity::Unix(device=0x0000000000000001, inode=0x000000000000002a))"#,
            r#"CoordinationRootSnapshot { path: "/data/协调", identity: Unix { device: 1, inode: 42 } }"#,
            "CoordinationRootSnapshot {\n    path: \"/data/协调\",\n    identity: Unix {\n        device: 1,\n        inode: 42,\n    },\n}",
        ];
        #[cfg(windows)]
        let complete = [
            r#"CoordinationRootSnapshot(path="/data/协调", identity=StableFileIdentity::Windows(volume_serial_number=0x0000000000000001, file_id=0x000102030405060708090a0b0c0d0e0f))"#,
            r#"CoordinationRootSnapshot { path: "/data/协调", identity: Windows { volume_serial_number: 1, file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] } }"#,
            concat!(
                "CoordinationRootSnapshot {\n    path: \"/data/协调\",\n    identity: Windows {\n",
                "        volume_serial_number: 1,\n        file_id: [\n",
                "            0,\n            1,\n            2,\n            3,\n",
                "            4,\n            5,\n            6,\n            7,\n",
                "            8,\n            9,\n            10,\n            11,\n",
                "            12,\n            13,\n            14,\n            15,\n",
                "        ],\n    },\n}",
            ),
        ];
        assert_eq!(format!("{value}"), complete[0]);
        assert_eq!(format!("{value:?}"), complete[1]);
        assert_eq!(format!("{value:#?}"), complete[2]);
        for (mode, expected) in complete.iter().enumerate() {
            for budget in 0..=expected.len() + 1 {
                let prefix = b"already written:";
                let mut output = LimitedOutput {
                    bytes: prefix.to_vec(),
                    limit: prefix.len() + budget,
                    failed: false,
                    writes_after_failure: 0,
                };
                let result = match mode {
                    0 => write!(&mut output, "{value}"),
                    1 => write!(&mut output, "{value:?}"),
                    _ => write!(&mut output, "{value:#?}"),
                };
                let accepted = budget.min(expected.len());
                assert_eq!(result.is_err(), budget < expected.len(), "mode={mode}, budget={budget}");
                assert_eq!(&output.bytes[..prefix.len()], prefix);
                assert_eq!(&output.bytes[prefix.len()..], &expected.as_bytes()[..accepted]);
                assert_eq!(output.writes_after_failure, 0, "mode={mode}, budget={budget}");
                assert_eq!(value.path(), path.as_path());
                assert_eq!(value.identity(), &diagnostic_identity());
            }
        }
    }

    #[test]
    // RSV-T03：Display外层宽度/精度/#不填充或截断；正常内容由独立字面格式测试保护。
    fn test_root_snapshot_display_flags_never_truncate_saved_fields() {
        let value = CoordinationRootSnapshot::from_parts(
            PathBuf::from("/data/协调"), diagnostic_identity(),
        );
        let complete = format!("{value}");
        for rendered in [
            format!("{value:>400}"), format!("{value:<400}"), format!("{value:^400}"),
            format!("{value:.0}"), format!("{value:.1}"), format!("{value:.255}"),
            format!("{value:#}"), format!("{value:+#0400.1}"),
        ] {
            assert_eq!(rendered, complete);
        }
    }

    #[test]
    // RSV-T05：真实目录数据拥有型移动到线程再返回；4线程共享借用各16次，精确64次完成。
    // 只借用同一快照，无Clone/资格登记；自动Send/Sync不证明真实目录/锁已被保活。
    fn test_root_snapshot_owned_move_and_shared_borrows_cross_threads() {
        fn assert_thread_traits<T: Send + Sync + 'static>() {}
        assert_thread_traits::<CoordinationRootSnapshot>();
        let temporary = fixture();
        let (path, identity) = real_directory_parts(temporary.path());
        let expected_path = path.clone();
        let expected_identity = identity.clone();
        let value = CoordinationRootSnapshot::from_parts(path, identity);
        let value = std::thread::spawn(move || {
            assert!(value.path().is_absolute());
            value
        }).join().expect("owned snapshot thread finishes");
        let completed = std::thread::scope(|scope| {
            let handles = (0..4).map(|_| {
                let borrowed = &value;
                let path = &expected_path;
                let identity = &expected_identity;
                scope.spawn(move || {
                    for _ in 0..16 {
                        assert_eq!(borrowed.path(), path.as_path());
                        assert_eq!(borrowed.identity(), identity);
                        assert!(std::ptr::eq(borrowed.identity(), borrowed.identity()));
                    }
                    16
                })
            }).collect::<Vec<_>>();
            handles.into_iter().map(|handle| handle.join().expect("shared reader finishes"))
                .sum::<usize>()
        });
        assert_eq!(completed, 64);
        assert_eq!(value.path(), expected_path.as_path());
        assert_eq!(value.identity(), &expected_identity);
        drop(value);
        assert!(temporary.path().is_dir());
    }
}
