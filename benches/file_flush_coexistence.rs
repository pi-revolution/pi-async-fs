//! 追加后普通文件刷新的端到端基准。
//!
//! 对比没有映射、持续只读映射及持续可写映射三个合法调用上下文，每轮追加
//! 4 KiB 后等待 Data 刷新。结果包含真实设备同步、文件增长与异步调度成本，
//! 不是私有许可的孤立耗时。必须串行独占执行并归档平台、文件系统和全部原始值。

#![feature(test)]

extern crate test;

use futures_lite::future::block_on;
use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace, LocalFileNamespace,
    MmapRange, ReadTargetRegion,
};
use test::{black_box, Bencher};

enum MappingContext {
    None,
    ReadOnly,
    ReadWrite,
}

// 只复用夹具与测量入口；实际追加、刷新和映射都来自公开生产 API。映射始终
// 只覆盖初始 4 KiB 前缀，后续每次追加都在其之外，不以映射作为同文件写入源。
fn measure_append_and_flush(bencher: &mut Bencher, context: MappingContext) {
    const BYTES: usize = 4096;
    let temporary_directory = tempfile::tempdir().expect("应能建立基准临时目录");
    let path = temporary_directory.path().join("target");
    std::fs::write(&path, vec![0x5a; BYTES]).expect("应能建立真实基准文件");
    let namespace = LocalFileNamespace::new();
    let mapper = match &context {
        MappingContext::None => None,
        MappingContext::ReadOnly => Some(
            block_on(namespace.open(&path, FileAccessMode::ReadMmap)).unwrap(),
        ),
        MappingContext::ReadWrite => Some(
            block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap(),
        ),
    };
    let readonly = if matches!(&context, MappingContext::ReadOnly) {
        // SAFETY: 基准独占临时文件，映射期间只在原文件尾部进行受管追加。
        Some(unsafe {
            block_on(mapper.as_ref().unwrap().map_read_only_uncoordinated(
                MmapRange::new(0, BYTES as u64).unwrap(),
            ))
            .expect("应能持续保留只读映射")
        })
    } else {
        None
    };
    let readwrite = if matches!(&context, MappingContext::ReadWrite) {
        // SAFETY: 基准独占临时文件，不修改映射字节、不截断，追加范围不与映射相交。
        Some(unsafe {
            block_on(mapper.as_ref().unwrap().map_read_write_uncoordinated(
                MmapRange::new(0, BYTES as u64).unwrap(),
            ))
            .expect("应能持续保留可写映射")
        })
    } else {
        None
    };
    let mut appender = block_on(namespace.open(&path, FileAccessMode::Append))
        .expect("应能独立打开严格追加资源");
    let payload: std::sync::Arc<[u8]> = vec![0x41; BYTES].into();
    block_on(appender.append(std::sync::Arc::clone(&payload))).unwrap();
    block_on(appender.flush(FileFlushMode::Data)).expect("预热刷新应成功");
    let mut iterations = 0_u64;
    bencher.bytes = BYTES as u64;
    bencher.iter(|| {
        block_on(appender.append(std::sync::Arc::clone(&payload)))
            .expect("完整追加应成功");
        block_on(appender.flush(FileFlushMode::Data))
            .expect("活动映射不能阻止普通数据刷新");
        iterations += 1;
        black_box(());
    });
    if let Some(mapping) = readonly {
        assert!(mapping.as_bytes().iter().all(|byte| *byte == 0x5a));
    }
    if let Some(mapping) = readwrite {
        assert!(mapping.as_bytes().iter().all(|byte| *byte == 0x5a));
    }
    let length = block_on(appender.byte_len()).expect("应能核验精确追加长度");
    assert_eq!(length, (iterations + 2) * BYTES as u64);
    let reader = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    let mut final_block = [0_u8; BYTES];
    block_on(reader.read_exact_at(
        length - BYTES as u64,
        &mut final_block,
        ReadTargetRegion::full(),
    ))
    .expect("映射释放后应能独立核验最后完整记录");
    assert!(final_block.iter().all(|byte| *byte == 0x41));
}

// 无映射的同会话控制组，记录追加与设备同步本身的成本。
#[bench]
fn bench_local_append_flush_data_4_kib(bencher: &mut Bencher) {
    measure_append_and_flush(bencher, MappingContext::None);
}

// 保留只读映射，在不反复解映射/重新建图的情况下周期性刷新追加记录。
#[bench]
fn bench_local_append_flush_data_with_read_only_mmap_4_kib(bencher: &mut Bencher) {
    measure_append_and_flush(bencher, MappingContext::ReadOnly);
}

// 保留可写映射，但不额外修改旧前缀，以控制变量对比映射权限与普通刷新共存成本。
#[bench]
fn bench_local_append_flush_data_with_read_write_mmap_4_kib(bencher: &mut Bencher) {
    measure_append_and_flush(bencher, MappingContext::ReadWrite);
}
