//! 本地文件热路径的端到端基准。
//!
//! 基准只通过公开 API 驱动真实文件系统，分别测量 4 KiB 定位精确读取和
//! 4 KiB 映射建立／释放。结果包含异步调度与操作系统成本，不能解释为某个
//! 私有锁或数据结构的孤立耗时；运行时必须串行独占并记录平台与文件系统。

#![feature(test)]

extern crate test;

use futures_lite::future::block_on;
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFileNamespace, MmapRange,
    ReadTargetRegion,
};
use test::{Bencher, black_box};

// 测量重复 4 KiB 定位精确读取的端到端耗时，重点观察安全取消模型引入的
// 任务提交、游标归位和资源交还总成本；夹具建立与首次正确性检查不计入迭代。
#[bench]
fn bench_local_read_exact_at_4_kib(bencher: &mut Bencher) {
    const FILE_BYTES: usize = 1024 * 1024;
    const READ_BYTES: usize = 4 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立基准临时目录");
    let path = temporary_directory.path().join("read-source");
    let content = (0..FILE_BYTES)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    std::fs::write(&path, &content).expect("应能建立基准文件");
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("应能打开基准只读资源");
    let mut buffer = [0_u8; READ_BYTES];
    block_on(file.read_exact_at(
        128 * 1024,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("基准预热读取应成功");
    assert_eq!(&buffer[..], &content[128 * 1024..132 * 1024]);

    bencher.bytes = READ_BYTES as u64;
    bencher.iter(|| {
        block_on(file.read_exact_at(
            128 * 1024,
            &mut buffer,
            ReadTargetRegion::full(),
        ))
        .expect("基准读取应成功");
        black_box(buffer[0])
    });
}

// 测量 4 KiB 只读映射从公开准入到最后句柄释放的端到端成本；每轮句柄立即
// Drop，下一轮复用同一资源，以同时覆盖原生映射和公开生命周期闭环。
#[bench]
fn bench_local_read_mmap_create_drop_4_kib(bencher: &mut Bencher) {
    const MAP_BYTES: usize = 4 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立基准临时目录");
    let path = temporary_directory.path().join("mapping-source");
    std::fs::write(&path, vec![0x5a_u8; MAP_BYTES])
        .expect("应能建立映射基准文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(namespace.open(&path, FileAccessMode::ReadMmap))
        .expect("应能打开基准映射资源");

    bencher.bytes = MAP_BYTES as u64;
    bencher.iter(|| {
        // SAFETY: 基准独占临时目录；映射只读且每轮在下一轮开始前释放，期间
        // 没有截断、替换、库外句柄或其它进程访问。
        let mapping = unsafe {
            block_on(resource.map_read_only_uncoordinated(
                MmapRange::new(0, MAP_BYTES as u64)
                    .expect("固定基准范围应有效"),
            ))
        }
        .expect("基准映射应成功");
        black_box(mapping.as_bytes()[0]);
        drop(mapping);
    });
}
