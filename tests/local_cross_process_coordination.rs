//! 本地跨进程协调公开 API 的单进程协议集成测试。
//!
//! 本文件在独占临时目录中验证授权、持久协议域、访问角色、映射、创建、删除、
//! 恢复及硬链接身份边界；真正的父子进程互斥证据位于
//! `local_cross_process_integration.rs`。每个 `unsafe` 调用均就地声明外部合作前提。

use futures_lite::future::block_on;
use pi_async_fs::{
    CopyStagingEvidence, CopyTargetEvidence, CreateTargetEvidence,
    FileAccessMode, FileFlushMode, FileIo, FileNamespace,
    LocalFileNamespace, MmapRange, ReadTargetRegion, RemoveTargetEvidence,
    RenameCommitEvidence,
};
use pi_result::ErrorKind;

#[test]
// 验证已有文件授权可打开独立协调式只读资源并执行真实随机读取。
fn test_existing_file_authority_opens_an_independent_coordinated_reader() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("coordinated-source");
    std::fs::write(&path, b"coordinated bytes")
        .expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试进程是该临时文件和临时目录的唯一访问者；从授权建立到
    // 所有派生资源释放，测试不会使用任何绕过本库协调协议的文件访问。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("已有普通文件应能建立跨进程协调授权");
    let reader = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Read,
    ))
    .expect("授权应能独立打开协调式只读资源");

    let mut buffer = [0_u8; 11];
    block_on(reader.read_exact_at(
        0,
        &mut buffer,
        ReadTargetRegion::full(),
    ))
    .expect("协调式只读资源应执行真实读取");
    assert_eq!(&buffer, b"coordinated");
}

#[test]
// 验证协调域一经发布便持久拒绝普通打开，不因公开授权值释放而失效。
fn test_published_authority_domain_permanently_rejects_ordinary_open() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("managed-target");
    std::fs::write(&path, b"managed").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录，并只通过本库协调入口访问目标。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("应能发布目标的协调域");
    let active_error = block_on(namespace.open(&path, FileAccessMode::Read))
        .err()
        .expect("授权存活时普通打开必须被拒绝");
    assert_eq!(active_error.current_context(), &ErrorKind::Conflict);

    drop(authority);
    let persisted_error = block_on(namespace.open(&path, FileAccessMode::Read))
        .err()
        .expect("协调记录持久存在时普通打开仍不得绕过协议");
    assert_eq!(persisted_error.current_context(), &ErrorKind::Conflict);
}

#[test]
// 验证持久协调域拒绝普通安全删除，并给出“本次未删除”的明确证据。
fn test_published_authority_domain_rejects_ordinary_safe_removal() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("managed-remove-target");
    std::fs::write(&path, b"managed").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录，并只用协调入口建立协议域。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("应能发布目标的协调域");
    drop(authority);

    let failure = block_on(namespace.remove_file(&path))
        .expect_err("普通安全删除不得绕过已经持久化的协调域");
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation,
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"managed");
}

#[test]
// 验证普通 copy_new 既不能读取受管源，也不能向已退役协调位置发布目标。
fn test_persisted_authority_domain_rejects_ordinary_copy_source_and_target() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let managed_source = temporary_directory.path().join("managed-source");
    let source_destination = temporary_directory.path().join("source-copy");
    let plain_source = temporary_directory.path().join("plain-source");
    let managed_destination = temporary_directory.path().join("managed-destination");
    std::fs::write(&managed_source, b"managed-source")
        .expect("应能建立受管源文件");
    std::fs::write(&plain_source, b"plain-source")
        .expect("应能建立普通源文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占目录，并仅使用协调入口发布源文件的协议域。
    let source_authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&managed_source))
    }
    .expect("应能发布受管源文件");
    drop(source_authority);
    let source_failure = block_on(namespace.copy_new(
        &managed_source,
        &source_destination,
    ))
    .expect_err("普通复制不得把协调域文件当作未协调读取源");
    assert_eq!(source_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        source_failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation,
    );
    assert_eq!(
        source_failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation,
    );
    assert!(!source_destination.exists());

    // SAFETY: 目标从创建到删除均由测试独占并使用同一协调协议。
    let destination = unsafe {
        block_on(namespace.create_new_coordinated(
            managed_destination.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("应能创建随后退役的受管目标位置");
    let (destination_authority, destination_resource) = destination.into_parts();
    drop(destination_resource);
    block_on(namespace.remove_file_with_cross_process_authority(
        &destination_authority,
    ))
    .expect("应能把目标位置合法推进到 Removed");
    drop(destination_authority);

    let target_failure = block_on(namespace.copy_new(
        &plain_source,
        &managed_destination,
    ))
    .expect_err("普通复制不得向持久协调域发布新目标");
    assert_eq!(target_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        target_failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation,
    );
    assert_eq!(
        target_failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation,
    );
    assert!(!managed_destination.exists());
}

#[test]
// 验证普通改名既不能迁移受管源，也不能占用已经退役但仍受协议保护的目标位置。
fn test_persisted_authority_domain_rejects_ordinary_rename_source_and_target() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let managed_source = temporary_directory.path().join("managed-rename-source");
    let source_destination = temporary_directory.path().join("source-destination");
    let plain_source = temporary_directory.path().join("plain-rename-source");
    let managed_destination = temporary_directory.path().join("managed-rename-target");
    std::fs::write(&managed_source, b"managed-source")
        .expect("应能建立受管改名源");
    std::fs::write(&plain_source, b"plain-source")
        .expect("应能建立普通改名源");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占目录；从授权建立起不再通过普通入口修改该受管源。
    let source_authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&managed_source))
    }
    .expect("应能发布受管改名源的协调域");
    drop(source_authority);

    let source_failure = block_on(namespace.rename(
        &managed_source,
        &source_destination,
    ))
    .expect_err("普通改名不得迁移已经进入协调域的源");
    assert_eq!(source_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        source_failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation,
    );
    assert_eq!(std::fs::read(&managed_source).unwrap(), b"managed-source");
    assert!(!source_destination.exists());

    // SAFETY: 目标位置由本测试独占，并从创建到退役始终使用同一协调协议。
    let destination = unsafe {
        block_on(namespace.create_new_coordinated(
            managed_destination.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("应能建立随后退役的受管改名目标");
    let (destination_authority, destination_resource) = destination.into_parts();
    drop(destination_resource);
    block_on(namespace.remove_file_with_cross_process_authority(
        &destination_authority,
    ))
    .expect("应能把受管改名目标合法推进到 Removed");
    drop(destination_authority);

    let target_failure = block_on(namespace.rename(
        &plain_source,
        &managed_destination,
    ))
    .expect_err("普通改名不得占用仍受协议保护的退役目标位置");
    assert_eq!(target_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        target_failure.commit_evidence(),
        &RenameCommitEvidence::NotRenamedByOperation,
    );
    assert_eq!(std::fs::read(&plain_source).unwrap(), b"plain-source");
    assert!(!managed_destination.exists());
}

#[cfg(unix)]
#[test]
// 验证持久协议按底层文件身份覆盖硬链接别名，防止普通入口旁路访问。
fn test_managed_stable_identity_rejects_ordinary_hard_link_aliases() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let managed_path = temporary_directory.path().join("managed-identity");
    let hard_link_alias = temporary_directory.path().join("managed-alias");
    let copy_destination = temporary_directory.path().join("alias-copy");
    std::fs::write(&managed_path, b"managed-by-identity")
        .expect("应能建立真实测试文件");
    std::fs::hard_link(&managed_path, &hard_link_alias)
        .expect("当前文件系统应能建立真实硬链接别名");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录；硬链接只用于验证同一进程的稳定身份汇合，
    // 不会被另一进程或绕过协议的并发参与者访问。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&managed_path))
    }
    .expect("应能为原始定位符建立协调域");
    drop(authority);

    let open_error = block_on(namespace.open(
        &hard_link_alias,
        FileAccessMode::Read,
    ))
    .err()
    .expect("授权值释放后，普通打开仍不得通过硬链接别名绕过协调域");
    assert_eq!(open_error.current_context(), &ErrorKind::Conflict);

    let copy_failure = block_on(namespace.copy_new(
        &hard_link_alias,
        &copy_destination,
    ))
    .expect_err("普通复制不得通过硬链接别名读取受管稳定身份");
    assert_eq!(copy_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        copy_failure.target_evidence(),
        &CopyTargetEvidence::NotPublishedByOperation,
    );
    assert_eq!(
        copy_failure.staging_evidence(),
        &CopyStagingEvidence::NotCreatedByOperation,
    );
    assert!(!copy_destination.exists());

    let remove_failure = block_on(namespace.remove_file(&hard_link_alias))
        .expect_err("普通安全删除不得通过硬链接别名绕过协调域");
    assert_eq!(remove_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        remove_failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation,
    );
    assert_eq!(std::fs::read(&managed_path).unwrap(), b"managed-by-identity");
    assert_eq!(std::fs::read(&hard_link_alias).unwrap(), b"managed-by-identity");
}

#[test]
// 验证已退役的协调位置拒绝普通严格及递归目录创建，不留下部分目录。
fn test_removed_coordination_domain_rejects_ordinary_directory_creation() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let strict_path = temporary_directory.path().join("strict-directory");
    let recursive_path = temporary_directory.path().join("recursive-directory");
    let namespace = LocalFileNamespace::new();

    for path in [&strict_path, &recursive_path] {
        // SAFETY: 每个目标从出现前到协调删除后都由本测试独占，并且只通过
        // 同一协调协议建立和退役文件实例。
        let created = unsafe {
            block_on(namespace.create_new_coordinated(
                path.clone(),
                FileAccessMode::Read,
            ))
        }
        .expect("应能建立随后退役的协调式目标");
        let (authority, resource) = created.into_parts();
        drop(resource);
        block_on(namespace.remove_file_with_cross_process_authority(
            &authority,
        ))
        .expect("应能把目标推进到 Removed");
    }

    let strict_failure = block_on(namespace.create_dir(&strict_path))
        .expect_err("普通严格目录创建不得占用已退役协调域");
    assert_eq!(strict_failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        strict_failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation,
    );
    assert!(!strict_path.exists());

    let recursive_failure = block_on(namespace.create_dir_all(&recursive_path))
        .expect_err("普通递归目录创建不得占用已退役协调域");
    assert_eq!(
        recursive_failure.error().current_context(),
        &ErrorKind::Conflict,
    );
    assert!(recursive_failure.confirmed_created().is_empty());
    assert!(recursive_failure.uncertain_targets().is_empty());
    assert!(!recursive_path.exists());
}

#[test]
// 验证活动普通资源阻止授权建立，失败无半初始化副作用且释放后可重试。
fn test_ordinary_resource_blocks_authority_establishment_without_side_effects() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("ordinary-target");
    std::fs::write(&path, b"ordinary").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();
    let ordinary = block_on(namespace.open(&path, FileAccessMode::Read))
        .expect("普通资源应先成功打开");

    // SAFETY: 测试没有跨进程或库外访问；此调用预期在发布协调协议前因当前
    // 普通资源冲突而失败。
    let error = match unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    } {
        Err(error) => error,
        Ok(_) => panic!("活动普通资源必须阻止协调授权建立"),
    };
    assert_eq!(error.current_context(), &ErrorKind::Conflict);

    drop(ordinary);
    // SAFETY: 普通资源已经关闭，此后测试只使用协调入口。
    unsafe { block_on(namespace.establish_cross_process_authority(&path)) }
        .expect("冲突失败不得留下半初始化协调状态");
}

#[test]
// 验证重复建立返回可独立使用且不泄露路径的授权值，并汇合至同一目标实例。
fn test_repeated_authority_establishment_returns_independent_redacted_tokens() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("repeated-target");
    std::fs::write(&path, b"repeated").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录，且授权存续期间不存在任何旁路访问。
    let first = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("首次授权建立应成功");
    // SAFETY: 与上一次调用相同，所有参与者继续遵守同一协议。
    let second = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("重复核验同一目标应返回另一个公开授权值");

    let debug = format!("{first:?}");
    let display = first.to_string();
    assert!(debug.contains("CrossProcessFileAuthority"));
    assert!(debug.contains("protocol_version"));
    assert!(display.contains("local cross-process file authority"));
    assert!(!debug.contains(path.to_string_lossy().as_ref()));
    assert!(!display.contains(path.to_string_lossy().as_ref()));

    let first_reader = block_on(namespace.open_with_cross_process_authority(
        &first,
        FileAccessMode::Read,
    ))
    .expect("首个授权应可打开资源");
    let second_reader = block_on(namespace.open_with_cross_process_authority(
        &second,
        FileAccessMode::Read,
    ))
    .expect("第二个独立授权也应可打开资源");
    // SAFETY: 活动资源本身已经属于同一受控协调域；重复建立只重新核验并加入
    // 同一目标实例，不引入普通句柄或外部参与者。
    let third = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("同一实例已有协调资源时仍应允许重新核验并取得独立授权");
    let third_reader = block_on(namespace.open_with_cross_process_authority(
        &third,
        FileAccessMode::Read,
    ))
    .expect("活动资源期间取得的新授权应绑定同一协调核心");
    drop((first_reader, second_reader, third_reader));
}

#[test]
// 验证协调式只读映射暴露精确范围、拒绝旁路入口，并在句柄释放后归还范围。
fn test_coordinated_read_only_mapping_exposes_exact_range_and_releases_lease() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("coordinated-read-mmap");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录；从授权建立开始只使用协调式公开入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("应能建立协调授权");
    let resource = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::ReadMmap,
    ))
    .expect("应能打开协调式只读映射资源");

    let first = block_on(resource.map_read_only(
        MmapRange::new(2, 7).expect("测试范围应有效"),
    ))
    .expect("协调式入口应建立精确只读映射");
    assert_eq!(first.range().start(), 2);
    assert_eq!(first.range().end_exclusive(), 7);
    assert_eq!(first.as_bytes(), b"23456");

    // SAFETY: 此调用只用于验证协调式资源不能绕过其权限域；实现必须在执行
    // 任何建图或留下范围租约之前拒绝它。
    let bypass_error = unsafe {
        block_on(resource.map_read_only_uncoordinated(
            MmapRange::new(7, 9).expect("测试范围应有效"),
        ))
    }
    .expect_err("协调式资源不得改走未协调映射入口");
    assert_eq!(bypass_error.current_context(), &ErrorKind::InvalidState);

    drop(first);
    let replacement = block_on(resource.map_read_only(
        MmapRange::new(2, 7).expect("测试范围应有效"),
    ))
    .expect("最后一个句柄释放后，相同范围租约应立即可重新取得");
    assert_eq!(replacement.as_bytes(), b"23456");
}

#[test]
// 验证协调式读写映射只修改指定范围，显式刷新后真实文件内容持久可见。
fn test_coordinated_read_write_mapping_persists_exact_range() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("coordinated-write-mmap");
    std::fs::write(&path, b"0123456789").expect("应能建立真实测试文件");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占临时目录；从授权建立开始只使用协调式公开入口。
    let authority = unsafe {
        block_on(namespace.establish_cross_process_authority(&path))
    }
    .expect("应能建立协调授权");
    let resource = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::ReadWriteMmap,
    ))
    .expect("应能打开协调式读写映射资源");

    let mut mapping = block_on(resource.map_read_write(
        MmapRange::new(3, 6).expect("测试范围应有效"),
    ))
    .expect("协调式入口应建立精确读写映射");
    mapping.as_bytes_mut().copy_from_slice(b"abc");
    block_on(mapping.flush()).expect("显式刷新应成功完成");
    drop(mapping);

    assert_eq!(std::fs::read(&path).unwrap(), b"012abc6789");
}

#[test]
// 验证协调创建原子返回授权与首个资源，不暴露普通入口可观察的未受管窗口。
fn test_coordinated_create_atomically_returns_authority_and_first_resource() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("new-coordinated-target");
    let existing_directory = temporary_directory.path().join("existing-directory");
    std::fs::create_dir(&existing_directory)
        .expect("应能建立协调式创建的现存目录边界");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试独占整个临时目录，现存目录及其父级没有任何旁路
    // 原生参与者；本调用应在文件创建和协调协议发布前拒绝。
    let directory_failure = unsafe {
        block_on(namespace.create_new_coordinated(
            existing_directory.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect_err("协调式创建必须把现存目录稳定分类为已存在");
    assert_eq!(
        directory_failure.error().current_context(),
        &ErrorKind::AlreadyExists,
    );
    assert_eq!(
        directory_failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation,
    );
    assert!(existing_directory.is_dir());

    // SAFETY: 测试独占临时目录，并从目标出现前开始只使用同一协调协议。
    let created = unsafe {
        block_on(namespace.create_new_coordinated(
            path.clone(),
            FileAccessMode::Append,
        ))
    }
    .expect("不存在的目标应能被协调式排他创建");
    assert_eq!(block_on(created.file().byte_len()).unwrap(), 0);

    let ordinary_error = block_on(namespace.open(&path, FileAccessMode::Read))
        .err()
        .expect("创建提交后普通入口不得观察到未受管窗口");
    assert_eq!(ordinary_error.current_context(), &ErrorKind::Conflict);

    let (authority, mut first_file) = created.into_parts();
    block_on(first_file.append(Vec::from(&b"first"[..])))
        .expect("首个资源应具有请求的追加角色");
    let mut reader = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Read,
    ))
    .expect("同一成功值内的授权应能打开独立协调式资源");
    let mut bytes = [0_u8; 5];
    block_on(reader.read_exact(
        &mut bytes,
        ReadTargetRegion::full(),
    ))
    .expect("独立资源应读取首个资源写入的内容");
    assert_eq!(&bytes, b"first");

    // SAFETY: 仍在同一受控临时目录和同一协调协议内；此调用预期在原生创建
    // 提交前因目标已存在而失败。
    let failure = unsafe {
        block_on(namespace.create_new_coordinated(
            path.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect_err("协调式创建必须保持严格排他语义");
    assert_eq!(
        failure.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation,
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"first");
}

#[test]
// 验证协调创建能交付全部冻结访问角色，并可执行各角色唯一允许的核心操作。
fn test_coordinated_create_delivers_every_requested_access_role() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let namespace = LocalFileNamespace::new();

    for role in [
        "read",
        "append",
        "overwrite",
        "read-mmap",
        "read-write-mmap",
        "truncate",
    ] {
        let path = temporary_directory.path().join(role);
        let access = match role {
            "read" => FileAccessMode::Read,
            "append" => FileAccessMode::Append,
            "overwrite" => FileAccessMode::Overwrite,
            "read-mmap" => FileAccessMode::ReadMmap,
            "read-write-mmap" => FileAccessMode::ReadWriteMmap,
            "truncate" => FileAccessMode::Truncate,
            _ => unreachable!("测试角色表是封闭集合"),
        };

        // SAFETY: 每个目标从出现前到删除后都由本测试独占，并且只使用同一
        // 协调协议；不同角色使用彼此独立的目标文件。
        let created = unsafe {
            block_on(namespace.create_new_coordinated(path.clone(), access))
        }
        .expect("每一种冻结访问角色都应能原子建立首个协调资源");
        let (authority, mut file) = created.into_parts();

        match role {
            "read" => {
                let mut empty = [];
                block_on(file.read_exact_at(
                    0,
                    &mut empty,
                    ReadTargetRegion::full(),
                ))
                .expect("只读首个资源应执行合法空读取");
            }
            "append" => {
                block_on(file.append(Vec::from(&b"a"[..])))
                    .expect("追加首个资源应执行严格尾部追加");
                block_on(file.flush(FileFlushMode::Data))
                    .expect("协调式追加资源应能强刷新数据");
            }
            "overwrite" => {
                block_on(file.overwrite_all(Vec::from(&b"o"[..])))
                    .expect("覆盖首个资源应替换完整文件内容");
                block_on(file.flush(FileFlushMode::DataAndMetadata))
                    .expect("协调式覆盖资源应能强刷新数据和元信息");
            }
            "truncate" => {
                block_on(file.truncate(0))
                    .expect("截断首个资源应接受等长零长度操作");
                block_on(file.flush(FileFlushMode::Data))
                    .expect("协调式截断资源应能强刷新数据");
            }
            "read-mmap" | "read-write-mmap" => {
                let mut appender = block_on(
                    namespace.open_with_cross_process_authority(
                        &authority,
                        FileAccessMode::Append,
                    ),
                )
                .expect("应能用独立协调资源先扩充空文件");
                block_on(appender.append(Vec::from(&b"m"[..])))
                    .expect("准备映射内容的追加应成功");
                drop(appender);

                if role == "read-mmap" {
                    let mapping = block_on(file.map_read_only(
                        MmapRange::new(0, 1).expect("测试范围应有效"),
                    ))
                    .expect("只读映射角色应建立映射");
                    assert_eq!(mapping.as_bytes(), b"m");
                } else {
                    let mut mapping = block_on(file.map_read_write(
                        MmapRange::new(0, 1).expect("测试范围应有效"),
                    ))
                    .expect("读写映射角色应建立可写映射");
                    mapping.as_bytes_mut()[0] = b'w';
                    block_on(mapping.flush()).expect("映射修改应显式刷新");
                }
            }
            _ => unreachable!("测试角色表是封闭集合"),
        }

        drop(file);
        block_on(namespace.remove_file_with_cross_process_authority(
            &authority,
        ))
        .expect("每个角色资源释放后都应能协调删除对应目标");
        assert!(!path.exists());
    }
}

#[test]
// 验证授权删除先与活动资源冲突，再对同一实例幂等，且旧授权不能删除新代次。
fn test_authorized_removal_conflicts_with_resources_then_is_instance_idempotent() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let path = temporary_directory.path().join("coordinated-removal");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试从目标出现前开始独占目录，并只使用同一协调协议。
    let created = unsafe {
        block_on(namespace.create_new_coordinated(
            path.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("应能建立待删除的协调式文件");
    let (authority, resource) = created.into_parts();

    let conflict = block_on(
        namespace.remove_file_with_cross_process_authority(&authority),
    )
    .expect_err("任一活动文件资源都必须让授权删除立即冲突");
    assert_eq!(conflict.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        conflict.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation,
    );
    assert!(path.exists());

    drop(resource);
    block_on(namespace.remove_file_with_cross_process_authority(&authority))
        .expect("最后一个活动资源释放后应能提交协调式删除");
    assert!(!path.exists());

    block_on(namespace.remove_file_with_cross_process_authority(&authority))
        .expect("同一目标实例已删除且位置仍为空时，重试应幂等成功");
    let open_error = block_on(namespace.open_with_cross_process_authority(
        &authority,
        FileAccessMode::Read,
    ))
    .err()
    .expect("旧授权不得重新打开已经退役的目标实例");
    assert_eq!(open_error.current_context(), &ErrorKind::Conflict);

    let ordinary_create = block_on(namespace.create_new(
        &path,
        FileAccessMode::Read,
    ))
    .err()
    .expect("持久协调域不得被普通排他创建绕过");
    assert_eq!(ordinary_create.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        ordinary_create.target_evidence(),
        &CreateTargetEvidence::NotCreatedByOperation,
    );
    assert!(!path.exists());

    // SAFETY: 旧实例已合法退役，测试继续独占目录并使用同一协调协议；只有
    // 协调式创建可以在原位置发布一个全新的目标实例代次。
    let replacement = unsafe {
        block_on(namespace.create_new_coordinated(
            path.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("Removed 位置应能由协调式创建发布新实例");
    let (replacement_authority, replacement_resource) = replacement.into_parts();
    let stale_failure = block_on(
        namespace.remove_file_with_cross_process_authority(&authority),
    )
    .expect_err("旧授权不得删除同名的新目标实例");
    assert_eq!(stale_failure.error().current_context(), &ErrorKind::Conflict);
    assert!(path.exists());
    drop(replacement_resource);
    block_on(namespace.remove_file_with_cross_process_authority(
        &replacement_authority,
    ))
    .expect("新授权应能删除自己绑定的新实例");
}

#[test]
// 验证恢复入口只确认已提交的 Removed 终态，绝不能从 Published 发起新删除。
fn test_removal_recovery_confirms_removed_but_never_starts_from_published() {
    let temporary_directory = tempfile::tempdir().expect("应能建立真实临时目录");
    let removed_path = temporary_directory.path().join("removed-instance");
    let published_path = temporary_directory.path().join("published-instance");
    let namespace = LocalFileNamespace::new();

    // SAFETY: 测试从两个目标出现前开始独占目录，并只使用同一协调协议。
    let removed = unsafe {
        block_on(namespace.create_new_coordinated(
            removed_path.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("应能建立第一个协调式目标");
    let (removed_authority, removed_resource) = removed.into_parts();
    drop(removed_resource);
    block_on(namespace.remove_file_with_cross_process_authority(
        &removed_authority,
    ))
    .expect("应能先把目标推进到 Removed");
    drop(removed_authority);

    // SAFETY: 协调记录可信且目录仍由测试独占；此调用只确认已经提交的终态。
    unsafe {
        block_on(namespace.resume_coordinated_file_removal(&removed_path))
    }
    .expect("没有内存授权时应可幂等确认合法 Removed 状态");

    // SAFETY: 同一独占目录和协调协议下建立第二个正常 Published 目标。
    let published = unsafe {
        block_on(namespace.create_new_coordinated(
            published_path.clone(),
            FileAccessMode::Read,
        ))
    }
    .expect("应能建立第二个协调式目标");
    let (published_authority, published_resource) = published.into_parts();
    drop((published_authority, published_resource));

    // SAFETY: 此调用仍满足外部合作前提，但 Published 不是恢复入口的合法调用
    // 状态；实现必须在任何删除提交点之前明确拒绝。
    let failure = unsafe {
        block_on(namespace.resume_coordinated_file_removal(&published_path))
    }
    .expect_err("恢复入口绝不能从 Published 发起一次新删除");
    assert_eq!(failure.error().current_context(), &ErrorKind::Conflict);
    assert_eq!(
        failure.target_evidence(),
        &RemoveTargetEvidence::NotRemovedByOperation,
    );
    assert!(published_path.exists());
}
