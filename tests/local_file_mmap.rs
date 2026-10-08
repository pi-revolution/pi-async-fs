//! 本地只读／读写 MMAP 句柄、范围、buffer 组合与并发的真实集成测试。
//!
//! 未协调入口仅用于各自独立且无库外参与者的夹具；每处 unsafe 均说明前置条件。
//! 16～512 MiB 时序／稀疏映射用例消耗地址空间与 I/O，必须串行独占运行。

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileFlushMode, FileIo, FileNamespace, LocalFile,
    LocalFileNamespace, MmapRange, OverwriteTargetEvidence, ReadMmapHandle,
    ReadTargetRegion, ReadWriteMmapHandle, TransferProgress,
};
use pi_result::ErrorKind;

// 编译期断言 helper：只验证目标类型同时满足 Send、Sync、'static，不执行
// 生产行为，也不替代后续真实跨线程／生命周期测试。
fn assert_send_sync_static<T: Send + Sync + 'static>() {}

// 编译期断言 helper：用于读写映射只要求 Send + 'static、刻意不要求 Sync。
fn assert_send_static<T: Send + 'static>() {}

// 仅在独立夹具中调用未协调只读映射的测试入口；它构造公开范围并转发真实
// 生产调用，不实现范围检查、身份协调或映射逻辑。
fn map_read_only_in_isolated_test(
    file: &LocalFile,
    start: u64,
    end_exclusive: u64,
) -> pi_result::Result<ReadMmapHandle> {
    // 所有调用方都使用独立临时目录，且建图期间没有本库协调域之外的文件
    // 参与者，因此满足未协调入口要求的外部稳定性前置条件。
    unsafe {
        block_on(file.map_read_only_uncoordinated(
            MmapRange::new(start, end_exclusive)
                .expect("测试范围端点本身应有效"),
        ))
    }
}

// 仅在独立夹具中调用未协调读写映射的测试入口；安全前提与只读 helper 相同，
// 所有权限、范围和生命周期仍由生产实现验证。
fn map_read_write_in_isolated_test(
    file: &LocalFile,
    start: u64,
    end_exclusive: u64,
) -> pi_result::Result<ReadWriteMmapHandle> {
    // 所有调用方都使用独立临时目录，且建图期间没有本库协调域之外的文件
    // 参与者，因此满足未协调入口要求的外部稳定性前置条件。
    unsafe {
        block_on(file.map_read_write_uncoordinated(
            MmapRange::new(start, end_exclusive)
                .expect("测试范围端点本身应有效"),
        ))
    }
}

// 验证未按页对齐的逻辑 `[2, 7)` 精确暴露五字节，只读句柄为 Send + Sync、
// Clone 零成本共享同一视图，最后一个克隆 Drop 后普通读取恢复。
#[test]
fn test_local_read_only_mmap_exposes_exact_unaligned_range_and_clone_lifetime() {
    assert_send_sync_static::<ReadMmapHandle>();

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("只读映射资源应能打开");

    // 测试期间不存在其它进程、库外句柄或并发文件修改，因而持续满足未协调
    // 映射入口要求调用方承担的外部稳定性前置条件。
    let handle = unsafe {
        block_on(mmap_resource.map_read_only_uncoordinated(
            MmapRange::new(2, 7).expect("测试范围应有效"),
        ))
    }
    .expect("未对齐页边界的精确只读范围应能建立映射");

    assert_eq!(handle.range().start(), 2);
    assert_eq!(handle.range().end_exclusive(), 7);
    assert_eq!(handle.len(), 5);
    assert_eq!(handle.as_bytes(), b"23456");
    let ordinary_view: &[u8] = handle.as_ref();
    assert_eq!(ordinary_view, b"23456");

    let cloned = handle.clone();
    assert_eq!(cloned.as_bytes().as_ptr(), handle.as_bytes().as_ptr());
    drop(handle);
    assert_eq!(cloned.as_bytes(), b"23456");
    drop(cloned);

    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("最后一个映射句柄释放后应能打开普通读取资源");
    let mut buffer = [0_u8; 4];
    block_on(read_resource.read_exact_at(
        3,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("最后一个映射句柄释放后普通读取应恢复准入");
    assert_eq!(&buffer, b"3456");
}

// 验证只读映射句柄及其派生子切片不能作为同一文件 append 的来源：失败返回
// 原对象／引用、Exact(0) 且文件不变，防止自相干写源。
#[test]
fn test_local_read_only_mmap_view_is_rejected_as_same_file_append_source() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("只读映射资源应能打开");
    let mut append_resource = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("同文件追加资源应能独立打开");

    // 测试期间不存在本库协调域之外的文件参与者。
    let handle = unsafe {
        block_on(mmap_resource.map_read_only_uncoordinated(
            MmapRange::new(2, 7).expect("测试范围应有效"),
        ))
    }
    .expect("只读映射应能建立");

    let failure = block_on(append_resource.append(&handle))
        .expect_err("同文件映射及其借用视图不得作为追加源");
    let (error, returned, progress) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(returned.as_bytes().as_ptr(), handle.as_bytes().as_ptr());
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");

    let mapped_subslice = &handle.as_bytes()[1..4];
    let failure = block_on(append_resource.append(mapped_subslice))
        .expect_err("同文件映射派生的普通子切片也必须被拒绝");
    let (error, returned, progress) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(returned.as_ptr(), mapped_subslice.as_ptr());
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");
}

// 验证相接半开范围 `[0,2)`、`[2,7)`、`[7,10)` 可并存，四类相交范围均
// 冲突；任一只读克隆存活时登记持续，最后释放后原范围可重建。
#[test]
fn test_local_read_only_mmap_accepts_touching_ranges_and_rejects_overlap() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let first_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("首个映射资源应能打开");
    let second_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("同文件独立映射资源应能打开");

    let middle = map_read_only_in_isolated_test(&first_resource, 2, 7)
        .expect("中间范围应能映射");
    let left = map_read_only_in_isolated_test(&second_resource, 0, 2)
        .expect("左侧相接但不相交范围应能映射");
    let right = map_read_only_in_isolated_test(&second_resource, 7, 10)
        .expect("右侧相接但不相交范围应能映射");
    assert_eq!(left.as_bytes(), b"01");
    assert_eq!(middle.as_bytes(), b"23456");
    assert_eq!(right.as_bytes(), b"789");

    for (start, end_exclusive) in [(1, 3), (3, 5), (6, 8), (2, 7)] {
        let error = match map_read_only_in_isolated_test(
            &second_resource,
            start,
            end_exclusive,
        ) {
            Err(error) => error,
            Ok(_) => panic!("任何相交半开范围都必须被拒绝"),
        };
        assert_eq!(error.current_context(), &ErrorKind::Conflict);
    }

    let clone = middle.clone();
    drop(middle);
    let error = match map_read_only_in_isolated_test(&first_resource, 2, 7) {
        Err(error) => error,
        Ok(_) => panic!("任一克隆存活时原范围仍必须保持登记"),
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    drop(clone);
    let replacement = map_read_only_in_isolated_test(&first_resource, 2, 7)
        .expect("最后一个克隆释放后相同范围应可重新映射");
    assert_eq!(replacement.as_bytes(), b"23456");
}

// 验证只读建图角色矩阵及文件长度快照：普通读／写角色拒绝，ReadWriteMmap
// 可建只读图，越界失败撤销预留且随后完整合法范围成功。
#[test]
fn test_local_read_only_mmap_enforces_role_and_snapshot_boundaries() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    for access in [
        FileAccessMode::Read,
        FileAccessMode::Append,
        FileAccessMode::Overwrite,
        FileAccessMode::Truncate,
    ] {
        let resource = block_on(namespace.open(&path, access))
            .expect("测试角色资源应能打开");
        let error = match map_read_only_in_isolated_test(&resource, 0, 1) {
            Err(error) => error,
            Ok(_) => panic!("非映射角色不得建立只读映射"),
        };
        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
    }

    let read_write_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射角色应能打开");
    let final_byte = map_read_only_in_isolated_test(&read_write_resource, 9, 10)
        .expect("读写映射角色应允许建立只读映射");
    assert_eq!(final_byte.as_bytes(), b"9");
    drop(final_byte);

    let read_only_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("只读映射角色应能打开");
    let error = match map_read_only_in_isolated_test(
        &read_only_resource,
        9,
        11,
    ) {
        Err(error) => error,
        Ok(_) => panic!("超过建图时文件长度快照的范围必须失败"),
    };
    assert_eq!(error.current_context(), &ErrorKind::InvalidInput);

    let whole = map_read_only_in_isolated_test(&read_only_resource, 0, 10)
        .expect("越界失败必须完整撤销待建图状态");
    assert_eq!(whole.as_bytes(), b"0123456789");
}

// 验证活动只读映射允许元信息、普通 buffer 追加与文件刷新；读、覆盖和截断
// 仍冲突且无副作用，映射释放后这些破坏性/普通读取操作恢复。
#[test]
fn test_local_read_only_mmap_operation_matrix_allows_metadata_append_and_flush() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("映射资源应能打开");
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("读取资源应能独立打开");
    let mut append_resource = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("追加资源应能独立打开");
    let mut overwrite_resource = block_on(
        namespace.open(&path, FileAccessMode::Overwrite),
    )
    .expect("覆盖资源应能独立打开");
    let mut truncate_resource = block_on(
        namespace.open(&path, FileAccessMode::Truncate),
    )
    .expect("截断资源应能独立打开");

    let handle = map_read_only_in_isolated_test(&mmap_resource, 2, 7)
        .expect("只读映射应能建立");
    assert_eq!(block_on(mmap_resource.byte_len()).unwrap(), 10);

    let mut read_buffer = [0_u8; 1];
    let error = block_on(read_resource.read_exact_at(
        0,
        &mut read_buffer,
        ReadTargetRegion::full(),
    ))
    .expect_err("活动映射必须阻止普通读取");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("活动映射必须阻止全文件覆盖");
    let (error, returned, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(returned, b"replacement");
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);

    let error = block_on(truncate_resource.truncate(5))
        .expect_err("活动映射必须阻止截断");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    block_on(append_resource.flush(FileFlushMode::Data))
        .expect("活动映射应允许普通文件强刷新");

    block_on(append_resource.append(b"XY".to_vec()))
        .expect("既有映射必须允许后续严格尾部追加");
    assert_eq!(handle.as_bytes(), b"23456");
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789XY");

    drop(handle);
    block_on(append_resource.flush(FileFlushMode::Data))
        .expect("映射释放后普通文件强刷新应恢复准入");
    block_on(read_resource.read_exact_at(
        10,
        &mut read_buffer,
        ReadTargetRegion::full(),
    ))
    .expect("映射释放后普通读取应恢复准入");
    assert_eq!(&read_buffer, b"X");
}

// 验证只读映射可作为不同文件 append 的零额外复制来源，目标得到精确映射字节，
// 源句柄内容与生命周期保持可用。
#[test]
fn test_local_read_only_mmap_is_valid_append_source_for_different_file() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let source = temporary_directory.path().join("source");
    let target = temporary_directory.path().join("target");
    std::fs::write(&source, b"0123456789").expect("应能建立源文件");
    std::fs::write(&target, b"prefix|").expect("应能建立目标文件");
    let namespace = LocalFileNamespace::new();
    let source_resource = block_on(
        namespace.open(&source, FileAccessMode::ReadMmap),
    )
    .expect("源映射资源应能打开");
    let mut target_resource = block_on(
        namespace.open(&target, FileAccessMode::Append),
    )
    .expect("目标追加资源应能打开");
    let handle = map_read_only_in_isolated_test(&source_resource, 2, 7)
        .expect("源映射应能建立");

    block_on(target_resource.append(&handle))
        .expect("不同文件的只读映射应能作为追加源");
    assert_eq!(handle.as_bytes(), b"23456");
    assert_eq!(std::fs::read(&target).unwrap(), b"prefix|23456");
}

// 验证只读映射 Debug／Display 只公开逻辑范围与长度，不泄漏映射内容、原生
// 地址、文件身份或底层句柄信息。
#[test]
fn test_local_read_only_mmap_formats_without_content_or_native_identity() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(namespace.open(&path, FileAccessMode::ReadMmap))
        .expect("映射资源应能打开");
    let handle = map_read_only_in_isolated_test(&resource, 2, 7)
        .expect("只读映射应能建立");

    assert_eq!(
        format!("{handle:?}"),
        "ReadMmapHandle { range: MmapRange { start: 2, end_exclusive: 7 }, len: 5 }",
    );
    assert_eq!(format!("{handle}"), "read-only mmap [2, 7)");
}

// 验证普通资源不能调用需要 authority 的协调式 `map_read_only`；失败为
// InvalidState 且不遗留范围预留，随后合法未协调入口仍成功。
#[test]
fn test_local_coordinated_read_only_mmap_rejects_ordinary_resource() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(namespace.open(&path, FileAccessMode::ReadMmap))
        .expect("普通映射资源应能打开");

    let error = block_on(resource.map_read_only(
        MmapRange::new(2, 7).expect("测试范围应有效"),
    ))
    .expect_err("普通资源不得静默使用跨进程协调式映射入口");
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);

    let handle = map_read_only_in_isolated_test(&resource, 2, 7)
        .expect("安全入口失败不得留下映射预留或其它副作用");
    assert_eq!(handle.as_bytes(), b"23456");
}

// 验证读写句柄为 Send 非 Sync，精确可变视图可写并作为固定读目标，所有映射
// 权限间拒绝相交、允许相接，Drop 后原范围和修改内容均可重新观察。
#[test]
fn test_local_read_write_mmap_exposes_exact_mutable_view_and_releases_range() {
    assert_send_static::<ReadWriteMmapHandle>();

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射资源应能打开");

    let mut handle = map_read_write_in_isolated_test(&resource, 2, 7)
        .expect("未对齐页边界的精确读写范围应能建立映射");
    assert_eq!(handle.range().start(), 2);
    assert_eq!(handle.range().end_exclusive(), 7);
    assert_eq!(handle.len(), 5);
    assert_eq!(handle.as_bytes(), b"23456");
    handle.as_bytes_mut().copy_from_slice(b"ABCDE");
    assert_eq!(handle.as_bytes(), b"ABCDE");
    let fixed_read_target: &mut [u8] = handle.as_mut();
    fixed_read_target[4] = b'!';
    assert_eq!(handle.as_bytes(), b"ABCD!");

    let error = match map_read_only_in_isolated_test(&resource, 6, 8) {
        Err(error) => error,
        Ok(_) => panic!("只读与读写映射之间也必须拒绝相交范围"),
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    let touching = map_read_only_in_isolated_test(&resource, 7, 10)
        .expect("与读写映射相接但不相交的只读范围应能共存");
    assert_eq!(touching.as_bytes(), b"789");

    drop(handle);
    let replacement = map_read_write_in_isolated_test(&resource, 2, 7)
        .expect("读写句柄 Drop 后原范围必须重新开放");
    assert_eq!(replacement.as_bytes(), b"ABCD!");
}

// 验证 ReadMmap 不能建可写图、普通资源不能使用协调式入口、越过长度快照失败，
// 且失败完整撤销预留使最后一字节合法映射成功。
#[test]
fn test_local_read_write_mmap_enforces_role_snapshot_and_authority_boundaries() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let read_only_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("只读映射资源应能打开");
    let error = match map_read_write_in_isolated_test(
        &read_only_resource,
        0,
        1,
    ) {
        Err(error) => error,
        Ok(_) => panic!("只读映射角色不得建立读写映射"),
    };
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);

    let read_write_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射资源应能打开");
    let error = block_on(read_write_resource.map_read_write(
        MmapRange::new(0, 1).expect("测试范围应有效"),
    ))
    .expect_err("普通资源不得静默使用跨进程协调式读写映射入口");
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);

    let error = match map_read_write_in_isolated_test(
        &read_write_resource,
        9,
        11,
    ) {
        Err(error) => error,
        Ok(_) => panic!("超过文件长度快照的读写范围必须失败"),
    };
    assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
    let final_byte = map_read_write_in_isolated_test(
        &read_write_resource,
        9,
        10,
    )
    .expect("失败路径必须撤销全部待建图状态");
    assert_eq!(final_byte.as_bytes(), b"9");
}

// 验证读写映射可作为不同文件定位／顺序固定读取目标并推进源游标；同文件读、
// 覆盖、截断和映射派生 append 源被拒，普通追加及文件刷新允许。
#[test]
fn test_local_read_write_mmap_is_fixed_read_target_and_obeys_operation_matrix() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let source_path = temporary_directory.path().join("source");
    let target_path = temporary_directory.path().join("target");
    std::fs::write(&source_path, b"ABCDEFGHIJ").expect("应能建立源文件");
    std::fs::write(&target_path, b"0123456789").expect("应能建立目标文件");
    let namespace = LocalFileNamespace::new();
    let mut source_resource = block_on(
        namespace.open(&source_path, FileAccessMode::Read),
    )
    .expect("源读取资源应能打开");
    let target_mmap_resource = block_on(
        namespace.open(&target_path, FileAccessMode::ReadWriteMmap),
    )
    .expect("目标映射资源应能打开");
    let same_file_read_resource = block_on(
        namespace.open(&target_path, FileAccessMode::Read),
    )
    .expect("同文件读取资源应能独立打开");
    let mut append_resource = block_on(
        namespace.open(&target_path, FileAccessMode::Append),
    )
    .expect("同文件追加资源应能独立打开");
    let mut overwrite_resource = block_on(
        namespace.open(&target_path, FileAccessMode::Overwrite),
    )
    .expect("同文件覆盖资源应能独立打开");
    let mut truncate_resource = block_on(
        namespace.open(&target_path, FileAccessMode::Truncate),
    )
    .expect("同文件截断资源应能独立打开");
    let mut handle = map_read_write_in_isolated_test(
        &target_mmap_resource,
        2,
        7,
    )
    .expect("目标读写映射应能建立");

    block_on(source_resource.read_exact_at(
        0,
        &mut handle,
        ReadTargetRegion::full(),
    ))
    .expect("读写映射应能作为不同文件的固定随机读取目标");
    assert_eq!(handle.as_bytes(), b"ABCDE");
    block_on(source_resource.read_exact(
        &mut handle,
        ReadTargetRegion::full(),
    ))
    .expect("读写映射应能作为固定顺序读取目标");
    assert_eq!(handle.as_bytes(), b"ABCDE");
    block_on(source_resource.read_exact(
        &mut handle,
        ReadTargetRegion::full(),
    ))
    .expect("顺序读取应继续推进源文件游标");
    assert_eq!(handle.as_bytes(), b"FGHIJ");

    let error = block_on(same_file_read_resource.read_exact_at(
        0,
        &mut handle,
        ReadTargetRegion::full(),
    ))
    .expect_err("同文件活动映射必须阻止普通读取");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(handle.as_bytes(), b"FGHIJ");

    let failure = block_on(
        overwrite_resource.overwrite_all(b"replacement".to_vec()),
    )
    .expect_err("读写映射必须阻止同文件全量覆盖");
    let (error, _, progress, evidence) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });
    assert_eq!(evidence, OverwriteTargetEvidence::Unchanged);
    let error = block_on(truncate_resource.truncate(5))
        .expect_err("读写映射必须阻止同文件截断");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    block_on(append_resource.flush(FileFlushMode::Data))
        .expect("读写映射应允许普通文件强刷新");

    let mapped_subslice = &handle.as_bytes()[1..4];
    let failure = block_on(append_resource.append(mapped_subslice))
        .expect_err("读写映射派生视图不得作为同文件追加源");
    let (error, returned, progress) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(returned.as_ptr(), mapped_subslice.as_ptr());
    assert_eq!(progress, TransferProgress::Exact { bytes: 0 });

    block_on(append_resource.append(b"++".to_vec()))
        .expect("普通 buffer 的严格追加应与既有读写映射并存");
    assert_eq!(handle.as_bytes(), b"FGHIJ");
    assert_eq!(block_on(target_mmap_resource.byte_len()).unwrap(), 12);
}

// 验证读写映射 Debug／Display 仅展示逻辑范围和长度，不输出可变内容、地址、
// 文件身份或原生句柄。
#[test]
fn test_local_read_write_mmap_formats_without_content_or_native_identity() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射资源应能打开");
    let handle = map_read_write_in_isolated_test(&resource, 2, 7)
        .expect("读写映射应能建立");

    assert_eq!(
        format!("{handle:?}"),
        "ReadWriteMmapHandle { range: MmapRange { start: 2, end_exclusive: 7 }, len: 5 }",
    );
    assert_eq!(format!("{handle}"), "read-write mmap [2, 7)");
}

// 验证修改映射后 `flush()` 等待完整范围刷新并可从独立路径读到新内容；没有
// 新修改时重复调用安全成功且内容保持不变。
#[test]
fn test_local_read_write_mmap_flush_completes_and_is_repeatable() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射资源应能打开");
    let mut handle = map_read_write_in_isolated_test(&resource, 2, 7)
        .expect("读写映射应能建立");

    handle.as_bytes_mut().copy_from_slice(b"ABCDE");
    block_on(handle.flush()).expect("同步完成型映射刷新应成功");
    assert_eq!(std::fs::read(&path).unwrap(), b"01ABCDE789");

    block_on(handle.flush()).expect("没有新修改时重复刷新仍应安全成功");
    assert_eq!(handle.as_bytes(), b"ABCDE");
    assert_eq!(std::fs::read(&path).unwrap(), b"01ABCDE789");
}

// 验证取消 128 MiB 映射 flush 等待者并 Drop 公开句柄后，完成者仍保留映射
// 和范围租约；普通读持续冲突，15 秒截止内完成后读到 DDDD。
#[test]
fn test_cancelled_local_read_write_mmap_flush_retains_mapping_until_completion() {
    const PAYLOAD_LEN: usize = 128 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("应能建立真实大文件");
    file.set_len(PAYLOAD_LEN as u64).expect("应能设置测试文件长度");
    drop(file);

    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("读写映射资源应能打开");
    let read_resource = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("竞争读取资源应能独立打开");
    let mut handle = map_read_write_in_isolated_test(
        &mmap_resource,
        0,
        PAYLOAD_LEN as u64,
    )
    .expect("大范围读写映射应能建立");
    handle.as_bytes_mut().fill(b'D');

    let flush = handle.flush();
    let first_poll = block_on(future::poll_once(flush));
    assert!(first_poll.is_none(), "大映射同步刷新第一次轮询应仍在执行中");
    drop(handle);

    let mut prefix = [0x55; 4];
    let error = block_on(read_resource.read_exact_at(
        0,
        &mut prefix,
        ReadTargetRegion::full(),
    ))
    .expect_err("取消后的刷新完成者仍访问映射时，普通读取必须冲突");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(prefix, [0x55; 4]);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        match block_on(read_resource.read_exact_at(
            0,
            &mut prefix,
            ReadTargetRegion::full(),
        )) {
            Ok(()) => break,
            Err(error) => {
                assert_eq!(error.current_context(), &ErrorKind::Conflict);
                assert!(
                    std::time::Instant::now() < deadline,
                    "取消后的映射刷新应在截止前完成并释放范围租约",
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
    assert_eq!(&prefix, b"DDDD");
}

// 验证 16 MiB append 正在执行时新建映射因长度尚未冻结而冲突；追加完成并
// 归还资源后相同建图立即恢复，快照只含原五字节。
#[test]
fn test_local_mapping_creation_conflicts_with_in_flight_append_then_recovers() {
    const PAYLOAD_LEN: usize = 16 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mut append_resource = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("追加资源应能打开");
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("映射资源应能独立打开");

    let append = append_resource.append(vec![b'A'; PAYLOAD_LEN]);
    let first_poll = block_on(future::poll_once(append));
    assert!(first_poll.is_none(), "大追加第一次轮询应仍在执行中");

    let error = match map_read_only_in_isolated_test(&mmap_resource, 0, 5) {
        Err(error) => error,
        Ok(_) => panic!("正在执行的追加必须阻止新映射冻结文件长度"),
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);

    assert_eq!(
        block_on(append_resource.byte_len())
            .expect("原追加资源应等待后台完成者归还文件句柄"),
        5 + PAYLOAD_LEN as u64,
    );
    let handle = map_read_only_in_isolated_test(&mmap_resource, 0, 5)
        .expect("追加完成后新映射应恢复准入");
    assert_eq!(handle.as_bytes(), b"base|");
}

// 验证活动只读映射允许两个独立 append 资源并发，但每个 256 KiB 调用保持
// 完整连续；精确两种顺序模型排除穿插，既有映射内容保持 base 前缀。
#[test]
fn test_local_multiple_append_handles_remain_serialized_with_active_mapping() {
    const CHUNK_LEN: usize = 256 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    std::fs::write(&path, b"base|").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("映射资源应能打开");
    let mut first_append = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("首个追加资源应能独立打开");
    let mut second_append = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("第二个追加资源应能独立打开");
    let handle = map_read_only_in_isolated_test(&mmap_resource, 0, 5)
        .expect("只读映射应能建立");

    let (first, second) = block_on(future::zip(
        first_append.append(vec![b'A'; CHUNK_LEN]),
        second_append.append(vec![b'B'; CHUNK_LEN]),
    ));
    first.expect("首个并发提交的追加应成功");
    second.expect("第二个并发提交的追加应成功");

    let content = std::fs::read(&path).expect("应能读取追加结果");
    assert_eq!(&content[..5], b"base|");
    let suffix = &content[5..];
    let a_then_b = suffix[..CHUNK_LEN].iter().all(|byte| *byte == b'A')
        && suffix[CHUNK_LEN..].iter().all(|byte| *byte == b'B');
    let b_then_a = suffix[..CHUNK_LEN].iter().all(|byte| *byte == b'B')
        && suffix[CHUNK_LEN..].iter().all(|byte| *byte == b'A');
    assert!(a_then_b || b_then_a, "每个公开追加调用必须保持完整连续");
    assert_eq!(handle.as_bytes(), b"base|");
}

// 验证硬链接别名按同一底层文件协调：别名不能绕过映射范围相交或普通读取
// 互斥，最后一个映射释放后别名读取恢复。
#[test]
fn test_local_mmap_coordination_uses_stable_identity_across_hard_links() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let original = temporary_directory.path().join("original");
    let alias = temporary_directory.path().join("alias");
    std::fs::write(&original, b"0123456789").expect("应能建立真实测试文件");
    std::fs::hard_link(&original, &alias).expect("应能建立同文件硬链接");
    let namespace = LocalFileNamespace::new();
    let original_resource = block_on(
        namespace.open(&original, FileAccessMode::ReadMmap),
    )
    .expect("原路径映射资源应能打开");
    let alias_resource = block_on(
        namespace.open(&alias, FileAccessMode::ReadMmap),
    )
    .expect("别名路径映射资源应能打开");
    let alias_read_resource = block_on(
        namespace.open(&alias, FileAccessMode::Read),
    )
    .expect("别名路径读取资源应能打开");

    let handle = map_read_only_in_isolated_test(&original_resource, 2, 7)
        .expect("原路径映射应能建立");
    let error = match map_read_only_in_isolated_test(&alias_resource, 6, 8) {
        Err(error) => error,
        Ok(_) => panic!("硬链接别名不得绕过相交范围检查"),
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    let mut byte = [0_u8; 1];
    let error = block_on(alias_read_resource.read_exact_at(
        0,
        &mut byte,
        ReadTargetRegion::full(),
    ))
    .expect_err("硬链接别名不得绕过活动映射的普通读取互斥");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);

    drop(handle);
    block_on(alias_read_resource.read_exact_at(
        0,
        &mut byte,
        ReadTargetRegion::full(),
    ))
    .expect("映射释放后别名路径普通读取应恢复准入");
    assert_eq!(&byte, b"0");
}

// 在 Linux tmpfs（`/dev/shm`）上验证未对齐读写映射、同步刷新与独立追加资源
// 的真实组合。环境未提供该挂载点时明确失败，不能把未执行冒充通过；结果只
// 证明可用 Linux tmpfs，不外推到所有内存文件系统或掉电持久化语义。
#[cfg(target_os = "linux")]
#[test]
fn test_local_read_write_mmap_and_append_compose_on_linux_tmpfs() {
    let tmpfs_root = std::path::Path::new("/dev/shm");
    assert!(
        tmpfs_root.is_dir(),
        "该专项要求 Linux /dev/shm tmpfs；环境缺失时必须记为未满足而非通过",
    );
    let temporary_directory = tempfile::tempdir_in(tmpfs_root)
        .expect("应能在可用 Linux tmpfs 建立独立临时目录");
    let path = temporary_directory.path().join("mapped-file");
    std::fs::write(&path, b"0123456789").expect("应能建立 tmpfs 测试文件");
    let namespace = LocalFileNamespace::new();
    let mmap_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadWriteMmap),
    )
    .expect("应能在 tmpfs 打开读写映射资源");
    let mut append_resource = block_on(
        namespace.open(&path, FileAccessMode::Append),
    )
    .expect("应能为同一 tmpfs 文件打开独立追加资源");

    // SAFETY: 测试独占 tmpfs 临时目录；映射存活期间没有其它进程或绕过本库
    // 的句柄执行截断、覆盖、删除或替换，唯一并发内容操作是本库允许的追加。
    let mut mapping = unsafe {
        block_on(mmap_resource.map_read_write_uncoordinated(
            MmapRange::new(2, 7).expect("未对齐逻辑范围应有效"),
        ))
    }
    .expect("tmpfs 应支持未对齐逻辑读写映射");
    mapping.as_bytes_mut().copy_from_slice(b"ABCDE");
    block_on(mapping.flush()).expect("tmpfs 映射同步刷新应成功");
    block_on(append_resource.append(Vec::from(&b"tail"[..])))
        .expect("活动 tmpfs 映射应允许独立严格追加");
    drop(mapping);

    assert_eq!(
        std::fs::read(&path).expect("应能读取 tmpfs 最终内容"),
        b"01ABCDE789tail",
    );
}

// 验证两个各 256 MiB、彼此相接但不相交的待建图范围可并发创建，证明建图
// 准入按逻辑范围而非粗粒度整文件互斥；使用真实 512 MiB 稀疏文件。
#[test]
fn test_local_disjoint_pending_mappings_can_be_created_concurrently() {
    const HALF_LEN: u64 = 256 * 1024 * 1024;

    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("source");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .expect("应能建立真实稀疏文件");
    file.set_len(HALF_LEN * 2).expect("应能设置测试文件长度");
    drop(file);
    let namespace = LocalFileNamespace::new();
    let first_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("首个映射资源应能打开");
    let second_resource = block_on(
        namespace.open(&path, FileAccessMode::ReadMmap),
    )
    .expect("第二个映射资源应能独立打开");

    // 测试期间没有本库协调域之外的文件参与者；两个 Future 只竞争本库同一
    // 稳定身份的建图准入，逻辑范围彼此相接但完全不相交。
    let first = unsafe {
        first_resource.map_read_only_uncoordinated(
            MmapRange::new(0, HALF_LEN).expect("首个范围应有效"),
        )
    };
    let second = unsafe {
        second_resource.map_read_only_uncoordinated(
            MmapRange::new(HALF_LEN, HALF_LEN * 2)
                .expect("第二个范围应有效"),
        )
    };
    let (first, second) = block_on(future::zip(first, second));
    let first = first.expect("首个待建图范围应成功");
    let second = second.expect("不相交的第二个待建图范围不应被全文件拒绝");
    assert_eq!(first.len(), HALF_LEN as usize);
    assert_eq!(second.len(), HALF_LEN as usize);
}
