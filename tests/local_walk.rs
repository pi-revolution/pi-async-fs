//! `LocalFileNamespace::walk` 的真实目录树集成测试。
//!
//! 本文件验证深度限制、惰性、所有权、弱一致并发和“不跟随后代符号链接”的
//! 遍历契约；项目顺序不属于公开保证，因此断言先按相对定位符建立独立索引。

#![cfg_attr(windows, feature(junction_point))]

use futures_lite::future::block_on;
use futures_lite::stream::StreamExt;
use pi_async_fs::{
    FileAccessMode, FileNamespace, FileType, LocalFileNamespace,
    WalkDepthLimit, WalkEntry, WalkOptions,
};
use pi_result::ErrorKind;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

// 通过公开流接口收集整次遍历，保留逐项目错误传播语义。
fn collect_walk(
    namespace: &LocalFileNamespace,
    root: PathBuf,
    depth_limit: WalkDepthLimit,
) -> pi_result::Result<Vec<WalkEntry<PathBuf>>> {
    block_on(async {
        let mut stream = namespace
            .walk(root, WalkOptions::new(depth_limit))
            .await?;
        let mut entries = Vec::new();
        while let Some(item) = stream.next().await {
            entries.push(item?);
        }
        Ok(entries)
    })
}

// 以独立、确定性的相对路径模型归一化无顺序保证的遍历结果。
fn indexed_entries(
    root: &Path,
    entries: &[WalkEntry<PathBuf>],
) -> BTreeMap<String, (usize, Option<FileType>)> {
    entries
        .iter()
        .map(|item| {
            let relative = item
                .entry
                .locator
                .strip_prefix(root)
                .expect("项目 locator 应位于遍历根下")
                .to_string_lossy()
                .replace('\\', "/");
            (
                relative,
                (
                    item.depth.get(),
                    item.entry.file_type_hint.clone(),
                ),
            )
        })
        .collect()
}

// 建立同时含空目录、三层后代和平台目录链接的真实测试树。
fn create_tree(root: &Path) {
    std::fs::write(root.join("file"), b"file")
        .expect("应能建立根直接文件");
    std::fs::create_dir(root.join("empty"))
        .expect("应能建立根直接空目录");
    std::fs::create_dir(root.join("directory"))
        .expect("应能建立第一层目录");
    std::fs::write(root.join("directory/child"), b"child")
        .expect("应能建立第二层文件");
    std::fs::create_dir(root.join("directory/subdirectory"))
        .expect("应能建立第二层目录");
    std::fs::write(root.join("directory/subdirectory/deep"), b"deep")
        .expect("应能建立第三层文件");

    #[cfg(unix)]
    std::os::unix::fs::symlink(
        root.join("directory"),
        root.join("directory-link"),
    )
    .expect("应能建立指向目录的符号链接");
    #[cfg(windows)]
    std::os::windows::fs::junction_point(
        root.join("directory"),
        root.join("directory-link"),
    )
    .expect("应能建立指向目录的目录联接");
}

#[test]
// 验证零深度不产出根或后代，但仍严格校验根存在且确为目录。
fn test_local_walk_zero_depth_validates_root_then_returns_empty_stream() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let namespace = LocalFileNamespace::new();

    let entries = collect_walk(
        &namespace,
        temporary_directory.path().to_path_buf(),
        WalkDepthLimit::Limited { max_depth: 0 },
    )
    .expect("合法根的零深度遍历应成功");
    assert!(entries.is_empty());

    let missing = temporary_directory.path().join("missing");
    let missing_error = match block_on(namespace.walk(
        missing,
        WalkOptions::new(WalkDepthLimit::Limited { max_depth: 0 }),
    )) {
        Err(error) => error,
        Ok(_) => panic!("零深度不能跳过缺失根校验"),
    };
    assert_eq!(missing_error.current_context(), &ErrorKind::NotFound);

    let file = temporary_directory.path().join("file-root");
    std::fs::write(&file, b"content").expect("应能建立非目录根");
    let file_error = match block_on(namespace.walk(
        file,
        WalkOptions::new(WalkDepthLimit::Limited { max_depth: 0 }),
    )) {
        Err(error) => error,
        Ok(_) => panic!("零深度不能跳过目录种类校验"),
    };
    assert_eq!(file_error.current_context(), &ErrorKind::Io);
}

#[test]
// 验证最大深度是包含式边界，并在越过边界前停止向下展开目录。
fn test_local_walk_applies_inclusive_depth_before_descending() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path();
    create_tree(root);
    let namespace = LocalFileNamespace::new();

    let depth_one = collect_walk(
        &namespace,
        root.to_path_buf(),
        WalkDepthLimit::Limited { max_depth: 1 },
    )
    .expect("一层遍历应成功");
    let depth_one = indexed_entries(root, &depth_one);
    assert_eq!(depth_one.get("file"), Some(&(1, Some(FileType::RegularFile))));
    assert_eq!(depth_one.get("empty"), Some(&(1, Some(FileType::Directory))));
    assert_eq!(
        depth_one.get("directory"),
        Some(&(1, Some(FileType::Directory))),
    );
    #[cfg(any(unix, windows))]
    assert_eq!(
        depth_one.get("directory-link"),
        Some(&(1, Some(FileType::SymbolicLink))),
    );
    assert!(!depth_one.contains_key("directory/child"));

    let depth_two = collect_walk(
        &namespace,
        root.to_path_buf(),
        WalkDepthLimit::Limited { max_depth: 2 },
    )
    .expect("两层遍历应成功");
    let depth_two = indexed_entries(root, &depth_two);
    assert_eq!(
        depth_two.get("directory/child"),
        Some(&(2, Some(FileType::RegularFile))),
    );
    assert_eq!(
        depth_two.get("directory/subdirectory"),
        Some(&(2, Some(FileType::Directory))),
    );
    assert!(!depth_two.contains_key("directory/subdirectory/deep"));
}

#[test]
// 验证无限遍历返回拥有所有权的全部后代，排除根且不跟随目录符号链接。
fn test_local_walk_unlimited_returns_owned_descendants_without_following_links() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path();
    create_tree(root);
    let namespace = LocalFileNamespace::new();

    let entries = collect_walk(
        &namespace,
        root.to_path_buf(),
        WalkDepthLimit::Unlimited,
    )
    .expect("无限深度遍历应成功");
    let indexed = indexed_entries(root, &entries);
    assert_eq!(
        indexed.get("directory/subdirectory/deep"),
        Some(&(3, Some(FileType::RegularFile))),
    );
    assert!(!indexed.contains_key("directory-link/child"));
    assert!(entries.iter().all(|item| item.depth.get() >= 1));
    assert!(entries.iter().all(|item| item.entry.locator != root));

    std::fs::remove_file(root.join("file"))
        .expect("流释放后应能删除已经枚举的文件");
    assert!(indexed.contains_key("file"), "已经产出的项目必须拥有数据");
}

#[test]
// 验证构造 Future 不访问目录，首次轮询才观察届时的真实文件系统状态。
fn test_local_walk_is_lazy_until_creation_future_is_polled() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path().join("created-after-call");
    let namespace = LocalFileNamespace::new();

    let unpolled = namespace.walk(
        root.clone(),
        WalkOptions::new(WalkDepthLimit::Unlimited),
    );
    std::fs::create_dir(&root)
        .expect("未轮询遍历 Future 不得提前打开缺失根");
    let mut stream = block_on(unpolled)
        .expect("首次轮询时已存在的目录应能建立遍历流");
    assert!(block_on(stream.next()).is_none());
}

#[test]
// 验证递归流产出目录后不会跨 yield 持有目录锁；调用方可在流仍存活时创建
// 后代，再安全地继续拉取。新后代是否可见属于弱一致观察，不是快照保证。
fn test_local_walk_allows_descendant_creation_while_stream_is_live() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path().to_path_buf();
    let anchor = root.join("anchor");
    let created_while_live = anchor.join("created-while-live");
    std::fs::create_dir(&anchor).expect("应能建立初始子目录");
    let namespace = LocalFileNamespace::new();

    block_on(async {
        let mut stream = namespace
            .walk(
                root.clone(),
                WalkOptions::new(WalkDepthLimit::Unlimited),
            )
            .await
            .expect("应能建立递归目录流");
        let first = stream
            .next()
            .await
            .expect("初始子目录必须存在")
            .expect("初始子目录必须可枚举");
        assert_eq!(first.entry.locator, anchor);
        assert_eq!(first.depth.get(), 1);
        assert_eq!(first.entry.file_type_hint, Some(FileType::Directory));

        let created_file = namespace
            .create_new(&created_while_live, FileAccessMode::Append)
            .await
            .expect("存活的普通递归流不得排斥公开文件创建");
        drop(created_file);

        while let Some(item) = stream.next().await {
            let entry = item.expect("并发创建不得破坏后续递归流");
            assert_eq!(
                entry.entry.locator, created_while_live,
                "测试树中除新后代外不得凭空出现其它项目",
            );
            assert_eq!(entry.depth.get(), 2);
            assert_eq!(
                entry.entry.file_type_hint,
                Some(FileType::RegularFile),
            );
        }
    });

    assert!(
        created_while_live.is_file(),
        "创建成功必须独立于递归流是否观察到新后代",
    );
}

#[cfg(any(unix, windows))]
#[test]
// 验证已经产出的目录项在递归下降前被替换为指向根外目录的链接时，后续遍历
// 绝不能跟随该链接产出根外内容。Unix 使用符号链接，Windows 使用无需提升
// 权限的目录联接；测试不限定流选择跳过、报错还是结束，只断言最小安全边界。
fn test_local_walk_never_descends_through_replaced_directory_link_after_yield(
) {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path().join("root");
    let outside = temporary_directory.path().join("outside");
    let child = root.join("child");
    let escaped_descendant = child.join("secret");

    std::fs::create_dir(&root).expect("应能建立遍历根");
    std::fs::create_dir(&outside).expect("应能建立根外目录");
    std::fs::create_dir(&child).expect("应能建立根内唯一子目录");
    std::fs::write(outside.join("secret"), b"outside")
        .expect("应能建立根外标记文件");

    let namespace = LocalFileNamespace::new();
    block_on(async {
        let mut stream = namespace
            .walk(
                root,
                WalkOptions::new(WalkDepthLimit::Unlimited),
            )
            .await
            .expect("应能建立递归目录流");
        let first = stream
            .next()
            .await
            .expect("根内唯一子目录必须被产出")
            .expect("初始子目录必须可枚举");
        assert_eq!(first.entry.locator, child);
        assert_eq!(first.entry.file_type_hint, Some(FileType::Directory));

        std::fs::remove_dir(&child)
            .expect("已产出的空目录应能在递归下降前删除");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &child)
            .expect("应能在原位置建立指向根外目录的符号链接");
        #[cfg(windows)]
        std::os::windows::fs::junction_point(&outside, &child)
            .expect("应能在原位置建立指向根外目录的目录联接");

        while let Some(item) = stream.next().await {
            if let Ok(entry) = item {
                assert_ne!(
                    entry.entry.locator, escaped_descendant,
                    "递归遍历不得跟随并发替换后的目录链接越出根目录",
                );
            }
        }
    });
}
