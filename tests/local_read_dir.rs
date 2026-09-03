//! `LocalFileNamespace::read_dir` 的真实文件系统集成测试。
//!
//! 本文件验证浅层枚举的惰性、所有权、弱一致并发、错误分类、符号链接和原生
//! 名称边界；元信息按需获取与递归遍历分别由 `metadata` 和 `local_walk` 测试负责。

use futures_lite::future::block_on;
use futures_lite::stream::StreamExt;
use pi_async_fs::{
    DirectoryEntry, FileAccessMode, FileNamespace, LocalFileNamespace,
};
#[cfg(unix)]
use pi_async_fs::FileType;
use pi_result::ErrorKind;
use std::path::PathBuf;

// 消费公开目录流并保留每个拥有所有权的项目，供内容契约测试复用。
fn collect_directory(
    namespace: &LocalFileNamespace,
    root: PathBuf,
) -> pi_result::Result<Vec<DirectoryEntry<PathBuf>>> {
    block_on(async {
        let mut stream = namespace.read_dir(root).await?;
        let mut entries = Vec::new();
        while let Some(item) = stream.next().await {
            entries.push(item?);
        }
        Ok(entries)
    })
}

#[test]
// 验证空目录是一次成功枚举，并以流正常结束而非错误项目表达。
fn test_local_read_dir_empty_directory_is_a_successful_empty_stream() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let namespace = LocalFileNamespace::new();
    let root = temporary_directory.path().to_path_buf();

    let mut stream = block_on(namespace.read_dir(root))
        .expect("空目录应成功建立流");
    assert!(
        block_on(stream.next()).is_none(),
        "空目录必须用正常流结束表达",
    );
}

#[test]
// 验证调用本身不访问文件系统，首次轮询返回的 Future 才建立目录流。
fn test_local_read_dir_is_lazy_until_the_creation_future_is_polled() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path().join("created-after-call");
    let namespace = LocalFileNamespace::new();

    let unpolled = namespace.read_dir(root.clone());
    std::fs::create_dir(&root)
        .expect("未轮询 Future 不得提前打开缺失目录");
    let mut stream = block_on(unpolled)
        .expect("首次轮询时已经存在的目录应能建立流");
    assert!(block_on(stream.next()).is_none());
}

#[test]
// 验证浅层流存活期间不会锁住目录；并发创建可以完成，而流只承诺返回一个
// 自洽的弱一致观察，不承诺一定包含或排除创建发生在枚举期间的新项目。
fn test_local_read_dir_allows_creation_while_stream_is_live() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path().to_path_buf();
    let initial = root.join("initial");
    let created_while_live = root.join("created-while-live");
    std::fs::write(&initial, b"initial")
        .expect("应能建立初始目录项");
    let namespace = LocalFileNamespace::new();

    block_on(async {
        let mut stream = namespace
            .read_dir(root.clone())
            .await
            .expect("应能建立浅层目录流");
        let first = stream
            .next()
            .await
            .expect("初始项目必须存在")
            .expect("初始项目必须可枚举");
        assert_eq!(first.name.to_string(), "initial");
        assert_eq!(first.locator, initial);

        let created_file = namespace
            .create_new(&created_while_live, FileAccessMode::Append)
            .await
            .expect("存活的普通目录流不得排斥公开文件创建");
        drop(created_file);

        while let Some(item) = stream.next().await {
            let entry = item.expect("并发创建不得破坏后续目录流");
            let name = entry.name.to_string();
            assert!(
                name == "initial" || name == "created-while-live",
                "弱一致流不得凭空产生其它目录项: {name}",
            );
            assert_eq!(entry.locator, root.join(name));
        }
    });

    assert!(
        created_while_live.is_file(),
        "创建成功必须独立于流是否观察到新项目",
    );
}

#[test]
// 验证仅返回直接子项、结果拥有名称与定位符，且不会隐式读取元信息。
fn test_local_read_dir_returns_only_owned_direct_children_without_prefetching_metadata() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let root = temporary_directory.path();
    let file = root.join("file");
    let hidden = root.join(".hidden");
    let directory = root.join("directory");
    std::fs::write(&file, b"file").expect("应能建立直接文件");
    std::fs::write(&hidden, b"hidden").expect("应能建立隐藏文件");
    std::fs::create_dir(&directory).expect("应能建立直接子目录");
    std::fs::write(directory.join("nested"), b"nested")
        .expect("应能建立非直接后代");

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&file, root.join("link"))
            .expect("应能建立直接符号链接");
    }

    let namespace = LocalFileNamespace::new();
    let mut entries = collect_directory(&namespace, root.to_path_buf())
        .expect("真实浅层目录枚举应成功");
    entries.sort();

    let mut names: Vec<_> = entries
        .iter()
        .map(|entry| entry.name.to_string())
        .collect();
    names.sort();
    let mut expected = vec![
        String::from(".hidden"),
        String::from("directory"),
        String::from("file"),
    ];
    #[cfg(unix)]
    expected.push(String::from("link"));
    expected.sort();
    assert_eq!(names, expected);

    for entry in &entries {
        assert_eq!(entry.locator, root.join(entry.name.to_string()));
        assert_eq!(
            entry.file_type_hint, None,
            "本地枚举不得为了填充提示额外预取元信息",
        );
        assert_ne!(entry.name.to_string(), "nested");
    }

    #[cfg(unix)]
    {
        let link = entries
            .iter()
            .find(|entry| entry.name.to_string() == "link")
            .expect("符号链接应作为直接项目返回");
        let link_metadata = block_on(namespace.symlink_metadata(&link.locator))
            .expect("目录项 locator 应能显式延迟查询链接自身元信息");
        assert_eq!(link_metadata.file_type, FileType::SymbolicLink);
        let target_metadata = block_on(namespace.metadata(&link.locator))
            .expect("同一 locator 应能显式查询最终目标元信息");
        assert_eq!(target_metadata.file_type, FileType::RegularFile);
    }

    std::fs::remove_file(&file).expect("应能在流释放后删除原文件");
    assert!(
        entries.iter().any(|entry| entry.name.to_string() == "file"),
        "已经产出的项目必须拥有名称和 locator",
    );
}

#[test]
// 验证缺失根与“根不是目录”在建立流时得到稳定、可区分的错误分类。
fn test_local_read_dir_classifies_root_creation_errors() {
    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let missing = temporary_directory.path().join("missing");
    let file = temporary_directory.path().join("file");
    std::fs::write(&file, b"content").expect("应能建立非目录根");
    let namespace = LocalFileNamespace::new();

    let missing_error = match block_on(namespace.read_dir(missing)) {
        Err(error) => error,
        Ok(_) => panic!("缺失根不得产生正常空流"),
    };
    assert_eq!(missing_error.current_context(), &ErrorKind::NotFound);

    let file_error = match block_on(namespace.read_dir(file)) {
        Err(error) => error,
        Ok(_) => panic!("常规文件根不得产生目录流"),
    };
    assert_eq!(file_error.current_context(), &ErrorKind::Io);
}

#[cfg(unix)]
#[test]
// 验证 Unix 非 UTF-8 名称在 EntryName 与子项定位符中逐字节无损保留。
fn test_local_read_dir_preserves_non_utf8_name_and_locator_bytes() {
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let raw_name = b"entry-\xff".to_vec();
    let native_name = OsString::from_vec(raw_name.clone());
    let path = temporary_directory.path().join(&native_name);
    std::fs::write(&path, b"content")
        .expect("应能建立非 UTF-8 原生目录项");
    let namespace = LocalFileNamespace::new();

    let entries = collect_directory(
        &namespace,
        temporary_directory.path().to_path_buf(),
    )
    .expect("非 UTF-8 原生目录项应能无损枚举");
    assert_eq!(entries.len(), 1);
    match &entries[0].name {
        pi_async_fs::EntryName::Native(name) => {
            assert_eq!(name.as_os_str().as_bytes(), raw_name)
        }
        pi_async_fs::EntryName::Remote(_) => {
            panic!("本地目录项不得变成远端名称")
        }
        _ => panic!("测试必须识别新增的名称表示"),
    }
    assert_eq!(entries[0].locator.as_os_str().as_bytes(), path.as_os_str().as_bytes());
}

#[cfg(unix)]
#[test]
// 验证根符号链接按平台目录枚举语义跟随，同时子项仍以调用方输入根定位。
fn test_local_read_dir_follows_root_symlink_and_keeps_input_root_in_child_locator() {
    use std::os::unix::fs::symlink;

    let temporary_directory = tempfile::tempdir()
        .expect("应能建立真实临时目录");
    let target = temporary_directory.path().join("target");
    let link = temporary_directory.path().join("root-link");
    std::fs::create_dir(&target).expect("应能建立真实目标目录");
    std::fs::write(target.join("child"), b"content")
        .expect("应能建立目标直接子项");
    symlink(&target, &link).expect("应能建立目录根符号链接");
    let namespace = LocalFileNamespace::new();

    let entries = collect_directory(&namespace, link.clone())
        .expect("目录根最终符号链接应被平台枚举语义跟随");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name.to_string(), "child");
    assert_eq!(entries[0].locator, link.join("child"));
}
