//! 本地公开 API 的资源与并发压力专项。
//!
//! 本文件不重复单次 happy-path：它使用真实文件、真实独立资源和生产协调逻辑，
//! 以精确参考模型验证大量并发随机读取，以及多轮不相交映射的准入和释放。
//! 两个目标都会争用线程、文件句柄和映射资源，必须串行独占执行。

use futures_lite::future::block_on;
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace, MmapRange,
    ReadTargetRegion,
};
use pi_result::ErrorKind;

const READ_FILE_BYTES: usize = 512 * 1024;
const READ_BYTES: usize = 64;
const READ_WORKERS: usize = 16;
const READS_PER_WORKER: usize = 64;

// 生成与位置一一对应的确定性字节，作为生产读取结果之外的独立参考模型。
fn expected_byte(position: usize) -> u8 {
    ((position * 131 + 17) % 251) as u8
}

// 压力验证多个独立文件资源可跨工作线程并发执行定位精确读取；每个字节均与
// 独立位置模型比较，并精确断言成功调用总数，不能用“未崩溃”代替正确性。
#[test]
fn test_local_concurrent_random_reads_match_reference_model_under_pressure() {
    let temporary_directory = tempfile::tempdir().expect("应能建立独立临时目录");
    let path = temporary_directory.path().join("read-source");
    let content = (0..READ_FILE_BYTES).map(expected_byte).collect::<Vec<_>>();
    std::fs::write(&path, &content).expect("应能建立确定性真实测试文件");
    let namespace = LocalFileNamespace::new();

    let tasks = (0..READ_WORKERS)
        .map(|worker| {
            let file = block_on(namespace.open(&path, FileAccessMode::Read))
                .expect("每个工作者应取得独立只读资源");
            async_global_executor::spawn(async move {
                for operation in 0..READS_PER_WORKER {
                    let offset = (worker * 7_919 + operation * 4_099)
                        % (READ_FILE_BYTES - READ_BYTES + 1);
                    let mut buffer = [0_u8; READ_BYTES];
                    file.read_exact_at(
                        offset as u64,
                        &mut buffer,
                        ReadTargetRegion::full(),
                    )
                    .await
                    .expect("并发定位精确读取应成功");
                    for (relative, actual) in buffer.into_iter().enumerate() {
                        assert_eq!(
                            actual,
                            expected_byte(offset + relative),
                            "worker={worker}, operation={operation}, relative={relative}",
                        );
                    }
                }
                READS_PER_WORKER
            })
        })
        .collect::<Vec<_>>();

    let completed = block_on(async move {
        let mut completed = 0;
        for task in tasks {
            completed += task.await;
        }
        completed
    });
    assert_eq!(completed, READ_WORKERS * READS_PER_WORKER);
}

// 压力验证每轮 64 个相接而不相交的真实映射均可并发建立；活动集合拒绝跨越
// 端点的相交范围，整轮释放后相同范围立即可重建，证明登记没有残留或泄漏。
#[test]
fn test_local_disjoint_mmap_leases_repeatedly_admit_and_release_under_pressure() {
    const RANGE_BYTES: u64 = 4 * 1024;
    const RANGE_COUNT: u64 = 64;
    const ROUNDS: usize = 4;

    let temporary_directory = tempfile::tempdir().expect("应能建立独立临时目录");
    let path = temporary_directory.path().join("mapping-source");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("应能建立真实映射文件");
    file.set_len(RANGE_BYTES * RANGE_COUNT)
        .expect("应能设置映射文件长度");
    drop(file);
    let namespace = LocalFileNamespace::new();

    for round in 0..ROUNDS {
        let tasks = (0..RANGE_COUNT)
            .map(|index| {
                let resource = block_on(
                    namespace.open(&path, FileAccessMode::ReadMmap),
                )
                .expect("每个范围应取得独立映射资源");
                async_global_executor::spawn(async move {
                    let start = index * RANGE_BYTES;
                    // SAFETY: 测试独占临时目录，所有访问都通过本库；映射只读、
                    // 范围两两相接但不相交，存活期内没有截断、替换或库外写入。
                    unsafe {
                        resource.map_read_only_uncoordinated(
                            MmapRange::new(start, start + RANGE_BYTES)
                                .expect("固定非空范围应有效"),
                        )
                    }
                    .await
                    .expect("不相交范围应并发准入")
                })
            })
            .collect::<Vec<_>>();
        let mappings = block_on(async move {
            let mut mappings = Vec::with_capacity(tasks.len());
            for task in tasks {
                mappings.push(task.await);
            }
            mappings
        });
        assert_eq!(mappings.len(), RANGE_COUNT as usize);
        assert!(mappings.iter().all(|mapping| mapping.len() == RANGE_BYTES as usize));

        let conflict_resource = block_on(
            namespace.open(&path, FileAccessMode::ReadMmap),
        )
        .expect("应能打开相交检查资源");
        // SAFETY: 与上述相同，唯一预期冲突来自本库登记的活动映射集合；调用在
        // 建立任何原生相交映射前必须失败。
        let conflict = unsafe {
            block_on(conflict_resource.map_read_only_uncoordinated(
                MmapRange::new(RANGE_BYTES - 1, RANGE_BYTES + 1)
                    .expect("跨端点范围应有效"),
            ))
        }
        .expect_err("与前两个活动范围均相交的映射必须被拒绝");
        assert_eq!(conflict.current_context(), &ErrorKind::Conflict);

        drop(mappings);
        // SAFETY: 所有映射句柄已经释放，测试仍独占文件且不执行库外修改。
        let replacement = unsafe {
            block_on(conflict_resource.map_read_only_uncoordinated(
                MmapRange::new(0, RANGE_BYTES).expect("首个范围应有效"),
            ))
        }
        .expect("整轮释放后范围登记应立即归还");
        assert_eq!(replacement.len(), RANGE_BYTES as usize, "round={round}");
    }
}
