//! 打开资格对象键的私有纯值模块（EXO-OAK-M01）。只记录同一物理协调根内
//! 的真实目标身份；不打开/保活文件，不读取配置，不取得锁或访问高层状态。
//! 冻结合同、三个出口和三图：
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_OPEN_ADMISSION_KEY_LAYER_2026-10-10.md。
//! 测试 open_admission_key_tests 仅证明值语义；真实资格另由上层实现并验证。

use crate::file_identity::StableFileIdentity;

/// 同一物理根内的完整目标身份键；只保存不可变编号，不保活资源或授予资格。
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct OpenAdmissionKey {
    // 所在物理根的身份；与文件编号一起比较，不按目录路径分裂别名。
    root_identity: StableFileIdentity,
    // 非破坏性探测得到的目标编号，失效/保活不由纯值承担。
    file_identity: StableFileIdentity,
}

impl OpenAdmissionKey {
    /// OAK-F01：上层保证来源配对；只移动固定字段，不取得系统资源或校验资格。
    #[must_use]
    pub(crate) fn from_parts(
        root_identity: StableFileIdentity,
        file_identity: StableFileIdentity,
    ) -> Self {
        Self { root_identity, file_identity }
    }

    /// OAK-F02：返回绑定self的已有字段，没有克隆/查询或可写状态出口。
    #[must_use]
    pub(crate) fn root_identity(&self) -> &StableFileIdentity {
        &self.root_identity
    }

    /// OAK-F03：借用编号不延长目标文件寿命，也不表示已经取得共享/独占资格。
    #[must_use]
    pub(crate) fn file_identity(&self) -> &StableFileIdentity {
        &self.file_identity
    }
}

// 完整诊断直接借用字段写端点，不造中间String，不用作稳定协调资产名。
// fmt失败立即向上传播，既有前缀不回滚，外层标志不能裁剪身份。
impl core::fmt::Display for OpenAdmissionKey {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(formatter, "OpenAdmissionKey(root_identity={}, file_identity={})",
            self.root_identity, self.file_identity)
    }
}

#[cfg(all(test, any(unix, windows)))]
mod open_admission_key_tests {
    use super::OpenAdmissionKey;
    use crate::file_identity::StableFileIdentity;

    // 手写身份只准备已冻结纯值样例，绝不当作操作系统查询或准入证明。
    fn identity(number: u64) -> StableFileIdentity {
        #[cfg(unix)]
        { StableFileIdentity::Unix { device: 7, inode: number } }
        #[cfg(windows)]
        {
            let mut file_id = [0; 16];
            file_id[8..].copy_from_slice(&number.to_be_bytes());
            StableFileIdentity::Windows { volume_serial_number: 7, file_id }
        }
    }

    // OAK-T01：通过生产出口辨别根/文件角色，重复借用指向同一字段且不改值。
    // 不检查构造器私有布局，不给编号赋予目录能力或文件寿命。
    #[test]
    fn test_open_admission_key_preserves_distinct_root_and_file_roles() {
        let value = OpenAdmissionKey::from_parts(identity(2), identity(9));
        assert_eq!(value.root_identity(), &identity(2));
        assert_eq!(value.file_identity(), &identity(9));
        assert!(std::ptr::eq(value.root_identity(), value.root_identity()));
        assert!(std::ptr::eq(value.file_identity(), value.file_identity()));
        assert_ne!(value.root_identity(), value.file_identity());
    }

    // OAK-T02：独立rank描述“先根后文件”，相同文件跨根必须分离。
    // HashMap故意让所有散列碰撞，只把它当标准消费者，不能以Hash唯一性判等。
    #[test]
    fn test_open_admission_key_full_values_order_and_colliding_containers() {
        use std::collections::{BTreeMap, HashMap};
        use std::hash::{BuildHasherDefault, DefaultHasher, Hash, Hasher};

        #[derive(Default)]
        struct CollisionHasher;
        impl Hasher for CollisionHasher {
            fn finish(&self) -> u64 { 0 }
            fn write(&mut self, _bytes: &[u8]) {}
        }

        let values = [
            OpenAdmissionKey::from_parts(identity(0), identity(0)),
            OpenAdmissionKey::from_parts(identity(u64::MAX), identity(0)),
            OpenAdmissionKey::from_parts(identity(0), identity(u64::MAX)),
            OpenAdmissionKey::from_parts(identity(1), identity(0)),
            OpenAdmissionKey::from_parts(identity(0), identity(0)),
        ];
        let ranks = [0, 3, 1, 2, 0];
        for (left_index, left) in values.iter().enumerate() {
            for (right_index, right) in values.iter().enumerate() {
                let expected = ranks[left_index].cmp(&ranks[right_index]);
                assert_eq!(left.cmp(right), expected);
                assert_eq!(left.partial_cmp(right), Some(expected));
                assert_eq!(left == right, ranks[left_index] == ranks[right_index]);
                assert_eq!(left == right, right == left);
            }
        }
        let mut first = DefaultHasher::new();
        let mut repeated = DefaultHasher::new();
        values[0].hash(&mut first);
        values[4].hash(&mut repeated);
        assert_eq!(first.finish(), repeated.finish());
        let mut indexed = HashMap::with_hasher(BuildHasherDefault::<CollisionHasher>::default());
        let mut sorted = BTreeMap::new();
        for (index, value) in values.iter().enumerate() {
            assert_eq!(indexed.insert(value, index), if index == 4 { Some(0) } else { None });
            assert_eq!(sorted.insert(value, index), if index == 4 { Some(0) } else { None });
        }
        assert_eq!(indexed.len(), 4);
        assert_eq!(sorted.values().copied().collect::<Vec<_>>(), [4, 2, 3, 1]);
        for (index, expected) in [4, 1, 2, 3, 4].into_iter().enumerate() {
            assert_eq!(indexed.get(&values[index]), Some(&expected));
        }
    }

    // OAK-T03：独立字面量保护完整角色/编号，格式化不是资产名或协议。
    // 接收端只观察fmt前缀/错误，不代替生产文件或锁实现。
    #[test]
    fn test_open_admission_key_complete_diagnostics_and_output_failure() {
        use core::fmt::Write;
        let value = OpenAdmissionKey::from_parts(identity(0), identity(u64::MAX));
        #[cfg(unix)]
        let expected = "OpenAdmissionKey(root_identity=StableFileIdentity::Unix(device=0x0000000000000007, inode=0x0000000000000000), file_identity=StableFileIdentity::Unix(device=0x0000000000000007, inode=0xffffffffffffffff))";
        #[cfg(windows)]
        let expected = "OpenAdmissionKey(root_identity=StableFileIdentity::Windows(volume_serial_number=0x0000000000000007, file_id=0x00000000000000000000000000000000), file_identity=StableFileIdentity::Windows(volume_serial_number=0x0000000000000007, file_id=0x0000000000000000ffffffffffffffff))";
        #[cfg(unix)]
        let debug_expected = "OpenAdmissionKey { root_identity: Unix { device: 7, inode: 0 }, file_identity: Unix { device: 7, inode: 18446744073709551615 } }";
        #[cfg(windows)]
        let debug_expected = "OpenAdmissionKey { root_identity: Windows { volume_serial_number: 7, file_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }, file_identity: Windows { volume_serial_number: 7, file_id: [0, 0, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255] } }";
        assert_eq!(format!("{value:?}"), debug_expected);
        for text in [format!("{value}"), format!("{value:.0}"), format!("{value:#}"), format!("{value:+#0400.1}")] {
            assert_eq!(text, expected);
        }

        struct Output { text: String, remaining: usize, failed: bool, after_failure: usize }
        impl Write for Output {
            fn write_str(&mut self, text: &str) -> core::fmt::Result {
                if self.failed {
                    self.after_failure += 1;
                    return Err(core::fmt::Error);
                }
                let accepted = self.remaining.min(text.len());
                // 本固定诊断样例全ASCII，任意字节界限都也是合法UTF8边界。
                self.text.push_str(&text[..accepted]);
                self.remaining -= accepted;
                if accepted < text.len() {
                    self.failed = true;
                    Err(core::fmt::Error)
                } else { Ok(()) }
            }
        }
        for limit in 0..=expected.len() + 1 {
            let mut output = Output { text: "prefix:".into(), remaining: limit, failed: false, after_failure: 0 };
            assert_eq!(write!(&mut output, "{value}").is_err(), limit < expected.len());
            assert_eq!(output.text, format!("prefix:{}", &expected[..limit.min(expected.len())]));
            assert_eq!(output.after_failure, 0);
        }
        assert_eq!(value.root_identity(), &identity(0));
        assert_eq!(value.file_identity(), &identity(u64::MAX));
    }

    // OAK-T04：真实线程移动拥有值并共享只读借用，编译器自动推导Send/Sync。
    // 不增加资格/锁/引用计数，返回仍为同一组根/文件数据。
    #[test]
    fn test_open_admission_key_owned_and_borrowed_thread_use() {
        fn traits<T: Send + Sync + 'static>() {}
        traits::<OpenAdmissionKey>();
        let value = OpenAdmissionKey::from_parts(identity(2), identity(9));
        let value = std::thread::spawn(move || value).join().expect("owned move");
        std::thread::scope(|scope| {
            let root = scope.spawn(|| assert_eq!(value.root_identity(), &identity(2)));
            let file = scope.spawn(|| assert_eq!(value.file_identity(), &identity(9)));
            root.join().expect("root reader");
            file.join().expect("file reader");
        });
        assert_eq!(value.root_identity(), &identity(2));
        assert_eq!(value.file_identity(), &identity(9));
    }
}
