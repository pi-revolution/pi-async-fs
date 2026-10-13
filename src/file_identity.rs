//! 稳定文件身份的内部纯值层。只保存编号并提供标准值/诊断能力，不查询文件、
//! 解析协议、操作配置/锁/注册表或调用较高层适配器；这些职责由调用方分别承担。
//! 已冻结合同：EXO-T01/M01/M02/M03/L01；设计、验证和限制见
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_IDENTITY_VALUE_LAYER_PLAN_2026-10-10.md。
//!
//! Unix 的 device 是设备号，inode 是索引节点号；Windows 的 volume_serial_number
//! 是卷序列号，file_id 按系统返回顺序保存完整 16 字节，不按 GUID 重排。
//! 本值不握住句柄或保活对象；对象消失后编号可能被系统复用，不能当作永久历史身份。
//! Clone 只显式复制固定字段，不复制资源或资格；Eq/Ord/Hash 包含完整字段，排序
//! 仅用于容器，不代表访问优先级。Send/Sync 由字段自动获得，无 Copy/Default。
//! 诊断与协议编码分离；同步查询已归file_identity_query，异步适配/协议仍归local。
//! 底层纯值不反向调用上述消费者；本模块验收不代替整体独占签发。

/// 保存完整平台身份编号的内部纯值，不保活资源或授予访问资格；标准能力见模块合同。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum StableFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows {
        volume_serial_number: u64,
        file_id: [u8; 16],
    },
}

// 仅在调用方明确要求诊断时同步借用字段；直接写输出端，不创建中间 String。
// 所有整数/字节值都可显示，来源合法性属于取得身份的上层；外层宽度、精度
// 和替代格式标志不改变完整输出。输出端失败立即传播，已有前缀不回滚；
// 不主动 panic，也不承诺输出端自身无分配、IO、阻塞或 panic。
impl core::fmt::Display for StableFileIdentity {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            #[cfg(unix)]
            Self::Unix { device, inode } => write!(
                formatter,
                "StableFileIdentity::Unix(device=0x{device:016x}, inode=0x{inode:016x})",
            ),
            #[cfg(windows)]
            Self::Windows { volume_serial_number, file_id } => {
                write!(
                    formatter,
                    "StableFileIdentity::Windows(volume_serial_number=0x{volume_serial_number:016x}, file_id=0x",
                )?;
                for byte in file_id {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str(")")
            }
        }
    }
}

#[cfg(all(test, any(unix, windows)))]
mod identity_value_tests {
    // 仅验证身份纯值给 crate 内调用方提供的基本能力，不调用查询/协议或文件操作。
    // 这些手写数据不是当前真实文件身份，不代替真实查询/协议/文件操作测试。
    use super::StableFileIdentity;

    #[cfg(unix)]
    fn sample_identity() -> StableFileIdentity {
        StableFileIdentity::Unix { device: 1, inode: 42 }
    }

    #[cfg(windows)]
    fn sample_identity() -> StableFileIdentity {
        StableFileIdentity::Windows {
            volume_serial_number: 1,
            file_id: [
                0, 1, 2, 3, 4, 5, 6, 7,
                8, 9, 10, 11, 12, 13, 14, 15,
            ],
        }
    }

    // 使用 M03 的独立手写示例核验完整诊断格式，不能复用生产协议编码作预期值。
    #[test]
    fn test_identity_value_display_uses_complete_diagnostic_format() {
        #[cfg(unix)]
        let expected = "StableFileIdentity::Unix(device=0x0000000000000001, inode=0x000000000000002a)";
        #[cfg(windows)]
        let expected = "StableFileIdentity::Windows(volume_serial_number=0x0000000000000001, file_id=0x000102030405060708090a0b0c0d0e0f)";
        assert_eq!(format!("{}", sample_identity()), expected);
    }

    // 零值/最大值仍是可显示数据；检查完整前导零和 Windows 字节原序，
    // 不由格式化职责拒绝虚构身份，也不把该结果视为真实文件有效性的证明。
    #[test]
    fn test_identity_value_display_preserves_zero_maximum_and_byte_order() {
        #[cfg(unix)]
        let cases = [
            (StableFileIdentity::Unix { device: 0, inode: 0 }, "StableFileIdentity::Unix(device=0x0000000000000000, inode=0x0000000000000000)"),
            (StableFileIdentity::Unix { device: u64::MAX, inode: u64::MAX }, "StableFileIdentity::Unix(device=0xffffffffffffffff, inode=0xffffffffffffffff)"),
            (StableFileIdentity::Unix { device: 0x0123456789abcdef, inode: 0xfedcba9876543210 }, "StableFileIdentity::Unix(device=0x0123456789abcdef, inode=0xfedcba9876543210)"),
        ];
        #[cfg(windows)]
        let cases = [
            (StableFileIdentity::Windows { volume_serial_number: 0, file_id: [0; 16] }, "StableFileIdentity::Windows(volume_serial_number=0x0000000000000000, file_id=0x00000000000000000000000000000000)"),
            (StableFileIdentity::Windows { volume_serial_number: u64::MAX, file_id: [255; 16] }, "StableFileIdentity::Windows(volume_serial_number=0xffffffffffffffff, file_id=0xffffffffffffffffffffffffffffffff)"),
            (StableFileIdentity::Windows {
                volume_serial_number: 0x0123456789abcdef,
                file_id: [0, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 255],
            }, "StableFileIdentity::Windows(volume_serial_number=0x0123456789abcdef, file_id=0x00102030405060708090a0b0c0d0e0ff)"),
        ];
        for (value, expected) in cases {
            assert_eq!(format!("{value}"), expected);
        }
    }

    // 标志不能删去字段、裁剪编号或偷偷产生另一种协议；每种请求仍输出原示例。
    #[test]
    fn test_identity_value_display_ignores_outer_width_precision_and_flags() {
        let value = sample_identity();
        #[cfg(unix)]
        let expected = "StableFileIdentity::Unix(device=0x0000000000000001, inode=0x000000000000002a)";
        #[cfg(windows)]
        let expected = "StableFileIdentity::Windows(volume_serial_number=0x0000000000000001, file_id=0x000102030405060708090a0b0c0d0e0f)";
        for rendered in [
            format!("{value:>200}"),
            format!("{value:<200}"),
            format!("{value:^200}"),
            format!("{value:.0}"),
            format!("{value:.1}"),
            format!("{value:.255}"),
            format!("{value:#}"),
            format!("{value:+#0200.1}"),
        ] {
            assert_eq!(rendered, expected);
        }
    }

    // 受控输出端只负责观察标准 fmt 失败合同；不替代身份查询、协议或文件功能。
    // limit 始终由下面的已知短 ASCII 示例限定在 bytes 容量内，成功接收长度不超过 limit。
    struct BoundedOutput {
        bytes: [u8; 256],
        len: usize,
        limit: usize,
        failed: bool,
        writes_after_failure: usize,
    }

    impl core::fmt::Write for BoundedOutput {
        fn write_str(&mut self, value: &str) -> core::fmt::Result {
            if self.failed {
                self.writes_after_failure += 1;
                return Err(core::fmt::Error);
            }
            let accepted = (self.limit - self.len).min(value.len());
            self.bytes[self.len..self.len + accepted]
                .copy_from_slice(&value.as_bytes()[..accepted]);
            self.len += accepted;
            if accepted != value.len() {
                self.failed = true;
                Err(core::fmt::Error)
            } else {
                Ok(())
            }
        }
    }

    // 在独立字面量的每个字节边界让输出端中途失败；必须立即返回错误并保留
    // 精确前缀，不能吞错后继续写、回滚前缀或改变输入。容量恰好足够/多一字节则成功。
    #[test]
    fn test_identity_value_display_propagates_every_output_failure_prefix() {
        use core::fmt::Write;
        let value = sample_identity();
        #[cfg(unix)]
        let expected = "StableFileIdentity::Unix(device=0x0000000000000001, inode=0x000000000000002a)";
        #[cfg(windows)]
        let expected = "StableFileIdentity::Windows(volume_serial_number=0x0000000000000001, file_id=0x000102030405060708090a0b0c0d0e0f)";
        for limit in 0..=expected.len() + 1 {
            let mut output = BoundedOutput {
                bytes: [0xa5; 256],
                len: 0,
                limit,
                failed: false,
                writes_after_failure: 0,
            };
            let result = write!(&mut output, "{value}");
            let accepted = limit.min(expected.len());
            assert_eq!(result.is_err(), limit < expected.len(), "limit={limit}");
            assert_eq!(output.len, accepted, "limit={limit}");
            assert_eq!(&output.bytes[..accepted], &expected.as_bytes()[..accepted]);
            assert!(output.bytes[accepted..].iter().all(|byte| *byte == 0xa5));
            assert_eq!(output.writes_after_failure, 0, "失败后不得继续输出");
        }
        assert_eq!(value, sample_identity(), "失败输出不能改变输入身份");
    }

    // 判等必须包含每个身份字段；克隆只复制纯值，不能忽略不同设备/卷或编号。
    #[test]
    fn test_identity_value_clone_and_equality_include_every_field() {
        let original = sample_identity();
        let cloned = original.clone();
        assert_eq!(original, cloned);
        assert_eq!(original.partial_cmp(&cloned), Some(std::cmp::Ordering::Equal));

        #[cfg(unix)]
        let different = [
            StableFileIdentity::Unix { device: 0, inode: 42 },
            StableFileIdentity::Unix { device: u64::MAX, inode: 42 },
            StableFileIdentity::Unix { device: 1, inode: 0 },
            StableFileIdentity::Unix { device: 1, inode: u64::MAX },
        ];
        #[cfg(windows)]
        let different = {
            let mut values = vec![
                StableFileIdentity::Windows {
                    volume_serial_number: 0,
                    file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                },
                StableFileIdentity::Windows {
                    volume_serial_number: u64::MAX,
                    file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                },
            ];
            for index in [0, 8, 15] {
                let mut file_id = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
                file_id[index] = 255;
                values.push(StableFileIdentity::Windows {
                    volume_serial_number: 1,
                    file_id,
                });
            }
            values
        };
        for value in different {
            assert_ne!(original, value);
            assert_ne!(value, original);
        }
    }

    // 手册规定按完整字段的字典序排序；散列只要求相等值一致，不要求无碰撞。
    #[test]
    fn test_identity_value_order_and_hash_preserve_all_distinct_keys() {
        use std::collections::{BTreeSet, HashMap};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        #[cfg(unix)]
        let ordered = [
            StableFileIdentity::Unix { device: 0, inode: 0 },
            StableFileIdentity::Unix { device: 0, inode: u64::MAX },
            StableFileIdentity::Unix { device: 1, inode: 0 },
            StableFileIdentity::Unix { device: 1, inode: 42 },
            StableFileIdentity::Unix { device: u64::MAX, inode: u64::MAX },
        ];
        #[cfg(windows)]
        let ordered = [
            StableFileIdentity::Windows { volume_serial_number: 0, file_id: [0; 16] },
            StableFileIdentity::Windows { volume_serial_number: 0, file_id: [255; 16] },
            StableFileIdentity::Windows { volume_serial_number: 1, file_id: [0; 16] },
            StableFileIdentity::Windows {
                volume_serial_number: 1,
                file_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
            },
            StableFileIdentity::Windows {
                volume_serial_number: 1,
                file_id: [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
            },
            StableFileIdentity::Windows {
                volume_serial_number: 1,
                file_id: [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            },
            StableFileIdentity::Windows { volume_serial_number: u64::MAX, file_id: [255; 16] },
        ];
        let mut indexed = HashMap::new();
        let mut sorted = BTreeSet::new();
        for (index, value) in ordered.iter().enumerate().rev() {
            assert_eq!(indexed.insert(value.clone(), index), None);
            assert!(sorted.insert(value.clone()));
        }
        assert_eq!(indexed.len(), ordered.len());
        assert_eq!(sorted.len(), ordered.len());
        assert!(sorted.iter().eq(ordered.iter()));
        for (index, value) in ordered.iter().enumerate() {
            assert_eq!(indexed.get(&value.clone()), Some(&index));
            let mut original_hash = DefaultHasher::new();
            let mut clone_hash = DefaultHasher::new();
            value.hash(&mut original_hash);
            value.clone().hash(&mut clone_hash);
            assert_eq!(original_hash.finish(), clone_hash.finish());
            for (other_index, other) in ordered.iter().enumerate() {
                let expected = index.cmp(&other_index);
                assert_eq!(value.cmp(other), expected);
                assert_eq!(value.partial_cmp(other), Some(expected));
                assert_eq!(value == other, index == other_index);
            }
        }
    }

    // 派生调试表示必须仍能看到平台和完整字段；预期字面量独立于生产编码。
    #[test]
    fn test_identity_value_debug_keeps_complete_platform_fields() {
        #[cfg(unix)]
        let cases = [
            (sample_identity(), "Unix { device: 1, inode: 42 }"),
            (StableFileIdentity::Unix { device: 0, inode: 0 }, "Unix { device: 0, inode: 0 }"),
            (StableFileIdentity::Unix { device: u64::MAX, inode: u64::MAX }, "Unix { device: 18446744073709551615, inode: 18446744073709551615 }"),
        ];
        #[cfg(windows)]
        let cases = [
            (sample_identity(), "Windows { volume_serial_number: 1, file_id: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] }"),
            (StableFileIdentity::Windows { volume_serial_number: 0, file_id: [0; 16] }, "Windows { volume_serial_number: 0, file_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] }"),
            (StableFileIdentity::Windows { volume_serial_number: u64::MAX, file_id: [255; 16] }, "Windows { volume_serial_number: 18446744073709551615, file_id: [255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255] }"),
        ];
        for (value, expected) in cases {
            assert_eq!(format!("{value:?}"), expected);
            assert_eq!(value, value.clone(), "调试不能改变输入值");
        }
    }

    // 编译约束加真实线程分别核验移动与共享借用；不扩大其它资源的线程保证。
    #[test]
    fn test_identity_value_is_send_and_sync_across_real_threads() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StableFileIdentity>();
        let original = sample_identity();
        let owned = original.clone();
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| original.clone());
            let mover = scope.spawn(move || owned);
            assert_eq!(reader.join().expect("只读线程应正常结束"), original);
            assert_eq!(mover.join().expect("拥有型线程应正常结束"), original);
        });
    }
}
