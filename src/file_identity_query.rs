//! 同步身份取得的内部出口。只向下依赖纯身份值、标准资源和既有系统/错误能力；
//! 路径打开、类型策略、异步调度、保活及持有资格均由调用方负责。
//! EXO-L02 / EXO-IDQ-M01：正式合同及三类图谱/读图说明在
//! .scratch/pi-async-fs-mainline/EXCLUSIVE_IDENTITY_QUERY_LAYER_PLAN_2026-10-10.md
//! #exclusive-identity-query-layer-boundaries；稳定出口索引 EXO-IDQ-API-001。
//! 真实验证入口 identity_query_tests；验收 IQV-A01~08，见同目录
//! EXCLUSIVE_IDENTITY_QUERY_LAYER_ACCEPTANCE_2026-10-10.md。
#[cfg(any(unix, windows))]
use crate::file_identity::StableFileIdentity;

#[cfg(unix)]
/// 只读取调用者已经取得的快照；不 stat/open、不保存借用、不保持对象存活。
/// 保留原支持判定：仅零 inode 返回 Unsupported，不额外拒绝设备号或截短字段。
/// EXO-IDQ-U01：冻结 M05；上述正式合同 #exclusive-identity-query-layer-unix-api。
/// 同模块真实测试 IQV-T01~05/T07，公开组合回归 IQV-T08；零 inode/极值仍是
/// 已明示动态缺口，不由纯值测试或高层打开失败冒充；验收 IQV-A02~05。
pub(crate) fn identity_from_metadata(
    metadata: &std::fs::Metadata,
) -> pi_result::Result<StableFileIdentity> {
    use std::os::unix::fs::MetadataExt;

    if metadata.ino() == 0 {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok(StableFileIdentity::Unix {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(windows)]
/// 临时借用调用方持有的文件/目录，结果只拥有编号，不保活原资源。
/// 原生查询可能阻塞；若上层异步提交，必须在真正拥有文件的完成者内借用。
/// 不添加文件类型探测或权限提升，原系统失败保留本次具体原因与原错误分类。
/// EXO-IDQ-W01：冻结 M06；上述正式合同 #exclusive-identity-query-layer-windows-api。
/// 同模块真实测试 IQV-T01~07（含 std/async-fs 三种权限），公开回归 IQV-T08；
/// 联合零/极值/有效源可控系统失败仍明示缺口；验收 IQV-A02~05。
pub(crate) fn identity_from_handle(
    handle: std::os::windows::io::BorrowedHandle<'_>,
) -> pi_result::Result<StableFileIdentity> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use pi_result::InteropResultExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx,
    };

    let mut information = FILE_ID_INFO::default();
    // SAFETY: BorrowedHandle 的所有者在同步调用期间存活；输出存储正确初始化、
    // 对齐和独占，大小匹配 FileIdInfo。借用不保存/复制/关闭原生句柄，
    // 只有查询成功才读取输出字段，失败立即捕获本次系统错误。
    let succeeded = unsafe {
        GetFileInformationByHandleEx(
            handle.as_raw_handle() as usize
                as windows_sys::Win32::Foundation::HANDLE,
            FileIdInfo,
            (&mut information as *mut FILE_ID_INFO).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if succeeded == 0 {
        return core::result::Result::<StableFileIdentity, _>::Err(
            std::io::Error::last_os_error(),
        )
        .into_classified_error();
    }
    if information.VolumeSerialNumber == 0
        && information.FileId.Identifier.iter().all(|byte| *byte == 0)
    {
        return Err(pi_result::error_stack::Report::new(
            pi_result::ErrorKind::Unsupported,
        ));
    }
    Ok(StableFileIdentity::Windows {
        volume_serial_number: information.VolumeSerialNumber,
        file_id: information.FileId.Identifier,
    })
}

#[cfg(all(test, any(unix, windows)))]
mod identity_query_tests {
    // 本模块直接驱动 EXO-IDQ-U01/W01 两个生产出口，IQV-T01~07 对应冻结
    // 计划的真实矩阵；fixture 仅准备/清理文件，不实现身份查询或操作协调。
    // 其它依赖（如 async-fs 打开）只作来源准备，不据此签发其完整语义；
    // 结果/动态缺口及组合回归见 EXO-L02 验收 IQV-A03~05。
    use super::*;
    #[cfg(windows)]
    use std::os::windows::io::AsHandle;

    // 小型真实夹具留在项目 target，TempDir 在正常退出及测试 panic 时清理内容。
    fn fixture() -> tempfile::TempDir {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("identity-query-test-fixtures");
        std::fs::create_dir_all(&root).expect("create fixture root");
        tempfile::Builder::new()
            .prefix("query-")
            .tempdir_in(root)
            .expect("create real fixture")
    }

    #[test]
    // IQV-T01：文件与目录都使用真实来源；重复查询应稳定，Unix 完整字段
    // 直接对照输入快照，不以生产查询生成预期编号，也不把结果当存活资格。
    fn test_identity_query_accepts_real_file_and_directory_sources() {
        let temporary = fixture();
        let path = temporary.path().join("identity.bin");
        std::fs::write(&path, b"identity").expect("prepare file");
        let file = std::fs::File::open(&path).expect("open file");

        #[cfg(unix)]
        let metadata = file.metadata().expect("file snapshot");
        #[cfg(unix)]
        let file_identity = identity_from_metadata(&metadata)
            .expect("file identity");
        #[cfg(windows)]
        let file_identity = identity_from_handle(file.as_handle())
            .expect("file identity");
        #[cfg(unix)]
        let repeated = identity_from_metadata(&metadata)
            .expect("repeat identity");
        #[cfg(windows)]
        let repeated = identity_from_handle(file.as_handle())
            .expect("repeat identity");
        assert_eq!(file_identity, repeated);

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(file_identity, StableFileIdentity::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }

        #[cfg(unix)]
        let directory = std::fs::metadata(temporary.path())
            .expect("directory snapshot");
        #[cfg(windows)]
        let directory = {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(temporary.path())
                .expect("open directory")
        };
        #[cfg(unix)]
        let directory_identity = identity_from_metadata(&directory)
            .expect("directory identity");
        #[cfg(windows)]
        let directory_identity = identity_from_handle(directory.as_handle())
            .expect("directory identity");
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(directory_identity, StableFileIdentity::Unix {
                device: directory.dev(),
                inode: directory.ino(),
            });
        }
        assert_ne!(file_identity, directory_identity);
        assert_eq!(file.metadata().expect("file remains open").len(), 8);
        assert_eq!(std::fs::read(&path).expect("unchanged content"), b"identity");
    }

    #[test]
    // IQV-T02：独立打开与硬链接仍是同一对象；内容完全相同的另一个
    // 存活文件必须不同，避免常量、名称或内容摘要冒充对象身份。
    fn test_identity_query_distinguishes_objects_not_names_or_content() {
        let temporary = fixture();
        let original = temporary.path().join("original");
        let alias = temporary.path().join("alias");
        let separate = temporary.path().join("separate");
        std::fs::write(&original, b"same-content").expect("prepare original");
        std::fs::hard_link(&original, &alias).expect("create real hard link");
        std::fs::write(&separate, b"same-content").expect("prepare separate");
        let first = std::fs::File::open(&original).expect("first open");
        let second = std::fs::File::open(&original).expect("independent open");
        let linked = std::fs::File::open(&alias).expect("alias open");
        let other = std::fs::File::open(&separate).expect("other object open");
        #[cfg(unix)]
        let expected = identity_from_metadata(
            &first.metadata().expect("first snapshot"),
        ).expect("first identity");
        #[cfg(windows)]
        let expected = identity_from_handle(first.as_handle())
            .expect("first identity");

        for file in [&second, &linked] {
            #[cfg(unix)]
            let observed = identity_from_metadata(
                &file.metadata().expect("same object snapshot"),
            ).expect("same object identity");
            #[cfg(windows)]
            let observed = identity_from_handle(file.as_handle())
                .expect("same object identity");
            assert_eq!(observed, expected);
        }
        #[cfg(unix)]
        let different = identity_from_metadata(
            &other.metadata().expect("other snapshot"),
        ).expect("other identity");
        #[cfg(windows)]
        let different = identity_from_handle(other.as_handle())
            .expect("other identity");
        assert_ne!(different, expected);
        for path in [&original, &alias, &separate] {
            assert_eq!(std::fs::read(path).expect("known content"), b"same-content");
        }
    }

    #[test]
    // IQV-T03：改名不改变原对象；同名重建是新对象。原句柄保活至新旧比较完成，
    // 排除原对象先销毁后编号合法复用的干扰，同时核验名字及独立内容。
    fn test_identity_query_preserves_renamed_object_and_distinguishes_recreation() {
        let temporary = fixture();
        let original = temporary.path().join("原名.bin");
        let moved = temporary.path().join("新名.bin");
        std::fs::write(&original, b"old-object").expect("prepare old object");
        let old = std::fs::File::open(&original).expect("hold old object");
        #[cfg(unix)]
        let expected = identity_from_metadata(
            &old.metadata().expect("old snapshot"),
        ).expect("old identity");
        #[cfg(windows)]
        let expected = identity_from_handle(old.as_handle())
            .expect("old identity");

        std::fs::rename(&original, &moved).expect("rename held object");
        assert!(!original.try_exists().expect("old name removed"));
        std::fs::write(&original, b"new-object").expect("recreate original name");
        let new = std::fs::File::open(&original).expect("hold new object");
        #[cfg(unix)]
        let observed = identity_from_metadata(
            &old.metadata().expect("old object snapshot after rename"),
        ).expect("old object identity after rename");
        #[cfg(windows)]
        let observed = identity_from_handle(old.as_handle())
            .expect("old object identity after rename");
        assert_eq!(observed, expected);
        #[cfg(unix)]
        let replacement = identity_from_metadata(
            &new.metadata().expect("new snapshot"),
        ).expect("new identity");
        #[cfg(windows)]
        let replacement = identity_from_handle(new.as_handle())
            .expect("new identity");
        assert_ne!(replacement, expected);

        // 某些共享挂载在改名前的句柄存活时无法重新打开新名称。对象身份的
        // 两项比较已在旧拥有者保活时完成；文件仍有名字，不会因关句柄销毁。
        // 随后继续核验新名称仍对应原身份与原内容，不降低任何身份断言。
        drop(old);
        let renamed = std::fs::File::open(&moved).expect("open moved name");
        #[cfg(unix)]
        let reopened = identity_from_metadata(
            &renamed.metadata().expect("renamed snapshot"),
        ).expect("renamed identity");
        #[cfg(windows)]
        let reopened = identity_from_handle(renamed.as_handle())
            .expect("renamed identity");
        assert_eq!(reopened, expected);
        assert_eq!(std::fs::read(&moved).expect("old content"), b"old-object");
        assert_eq!(std::fs::read(&original).expect("new content"), b"new-object");
    }

    #[test]
    // IQV-T04/T05：查询不关闭文件、不移动游标或改变内容；夹具使用真实
    // 原生覆盖/追加改变内容后仍是同一对象，不代替公开 FileIo 的功能测试。
    fn test_identity_query_does_not_modify_or_consume_file_state() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let temporary = fixture();
        let path = temporary.path().join("content.bin");
        std::fs::write(&path, b"01234567").expect("prepare content");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open read-write source");
        file.seek(SeekFrom::Start(2)).expect("prepare cursor");
        #[cfg(unix)]
        let before = identity_from_metadata(
            &file.metadata().expect("initial snapshot"),
        ).expect("initial identity");
        #[cfg(windows)]
        let before = identity_from_handle(file.as_handle())
            .expect("initial identity");
        assert_eq!(file.stream_position().expect("cursor after query"), 2);
        assert_eq!(file.metadata().expect("source still open").len(), 8);
        assert_eq!(std::fs::read(&path).expect("initial bytes"), b"01234567");
        let mut observed = [0; 2];
        file.read_exact(&mut observed).expect("read after query");
        assert_eq!(&observed, b"23");

        file.seek(SeekFrom::Start(0)).expect("prepare fixture overwrite");
        file.write_all(b"AB").expect("change fixture prefix");
        let mut append = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .expect("independent append-only source");
        append.write_all(b"++").expect("append fixture suffix");
        #[cfg(unix)]
        let after = identity_from_metadata(
            &file.metadata().expect("changed snapshot"),
        ).expect("changed identity");
        #[cfg(windows)]
        let after = identity_from_handle(file.as_handle())
            .expect("changed identity");
        assert_eq!(after, before);
        assert_eq!(file.stream_position().expect("cursor after second query"), 2);
        assert_eq!(file.metadata().expect("changed length").len(), 10);
        assert_eq!(std::fs::read(&path).expect("changed bytes"), b"AB234567++");
    }

    #[test]
    // IQV-T05：两个拥有型结果离开所有输入的作用域后仍能使用；不把编号
    // 的存活当成文件的保活，也不在原句柄关闭后伪造 BorrowedHandle。
    fn test_identity_query_result_outlives_the_source_borrow() {
        let temporary = fixture();
        let path = temporary.path().join("lifetime.bin");
        std::fs::write(&path, b"lifetime").expect("prepare lifetime source");
        let (first, repeated) = {
            let file = std::fs::File::open(&path).expect("open owned source");
            #[cfg(unix)]
            let metadata = file.metadata().expect("owned source snapshot");
            #[cfg(unix)]
            let first = identity_from_metadata(&metadata).expect("first value");
            #[cfg(windows)]
            let first = identity_from_handle(file.as_handle()).expect("first value");
            #[cfg(unix)]
            let repeated = identity_from_metadata(&metadata).expect("second value");
            #[cfg(windows)]
            let repeated = identity_from_handle(file.as_handle()).expect("second value");
            assert_eq!(file.metadata().expect("source still usable").len(), 8);
            (first, repeated)
        };
        assert_eq!(first, repeated);
        assert_eq!(std::fs::read(&path).expect("preserved source"), b"lifetime");
        std::fs::remove_file(&path).expect("remove source after input drop");
        assert_eq!(first, repeated);
    }

    #[cfg(unix)]
    #[test]
    // IQV-T05：安全但过期的 Metadata 仍能提取原快照字段；原文件已关闭/
    // 删除，编号不代表当前有效资格，不比较可能复用该编号的新对象。
    fn test_unix_identity_query_accepts_a_snapshot_after_source_removal() {
        use std::os::unix::fs::MetadataExt;

        let temporary = fixture();
        let path = temporary.path().join("snapshot.bin");
        std::fs::write(&path, b"snapshot").expect("prepare snapshot source");
        let (first, repeated) = {
            let metadata = {
                let file = std::fs::File::open(&path).expect("open snapshot source");
                file.metadata().expect("capture original snapshot")
            };
            std::fs::remove_file(&path).expect("remove original source");
            assert!(!path.try_exists().expect("source no longer named"));
            let first = identity_from_metadata(&metadata).expect("stale snapshot value");
            assert_eq!(first, StableFileIdentity::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
            });
            let repeated = identity_from_metadata(&metadata).expect("same stale snapshot");
            (first, repeated)
        };
        assert_eq!(first, repeated);
    }

    #[test]
    // IQV-T07：四个真实线程各查询十六次同一存活对象，逐项检查全部六十四
    // 个结果。每线程独立打开、不克隆句柄；这不是时延或长期压力测量。
    fn test_identity_query_is_consistent_across_threads() {
        let temporary = fixture();
        let path = temporary.path().join("threads.bin");
        std::fs::write(&path, b"threads").expect("prepare thread source");
        let owner = std::fs::File::open(&path).expect("hold source object");
        #[cfg(unix)]
        let expected = identity_from_metadata(
            &owner.metadata().expect("main snapshot"),
        ).expect("main identity");
        #[cfg(windows)]
        let expected = identity_from_handle(owner.as_handle())
            .expect("main identity");

        let results = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..4 {
                let path = &path;
                workers.push(scope.spawn(move || {
                    let file = std::fs::File::open(path).expect("independent worker source");
                    #[cfg(unix)]
                    let metadata = file.metadata().expect("worker snapshot");
                    let mut results = Vec::new();
                    for _ in 0..16 {
                        #[cfg(unix)]
                        let observed = identity_from_metadata(&metadata)
                            .expect("worker identity");
                        #[cfg(windows)]
                        let observed = identity_from_handle(file.as_handle())
                            .expect("worker identity");
                        results.push(observed);
                    }
                    results
                }));
            }
            workers.into_iter()
                .map(|worker| worker.join().expect("worker completed"))
                .collect::<Vec<_>>()
        });
        assert_eq!(results.len(), 4);
        for worker_results in results {
            assert_eq!(worker_results.len(), 16);
            for observed in worker_results {
                assert_eq!(observed, expected);
            }
        }
        assert_eq!(owner.metadata().expect("owner still open").len(), 7);
        assert_eq!(std::fs::read(&path).expect("thread source unchanged"), b"threads");
    }

    #[test]
    // IQV-T01：空文件与空目录也是有效来源；身份不依赖数据字节数或目录项数。
    fn test_identity_query_accepts_empty_file_and_empty_directory() {
        let temporary = fixture();
        let path = temporary.path().join("empty.bin");
        let directory_path = temporary.path().join("empty-directory");
        std::fs::write(&path, b"").expect("prepare empty file");
        std::fs::create_dir(&directory_path).expect("prepare empty directory");
        let file = std::fs::File::open(&path).expect("open empty file");
        #[cfg(unix)]
        let empty_file = identity_from_metadata(
            &file.metadata().expect("empty file snapshot"),
        ).expect("empty file identity");
        #[cfg(windows)]
        let empty_file = identity_from_handle(file.as_handle())
            .expect("empty file identity");

        #[cfg(unix)]
        let directory = std::fs::metadata(&directory_path)
            .expect("empty directory snapshot");
        #[cfg(windows)]
        let directory = {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
            std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(&directory_path)
                .expect("open empty directory")
        };
        #[cfg(unix)]
        let empty_directory = identity_from_metadata(&directory)
            .expect("empty directory identity");
        #[cfg(windows)]
        let empty_directory = identity_from_handle(directory.as_handle())
            .expect("empty directory identity");
        assert_ne!(empty_file, empty_directory);
        #[cfg(unix)]
        let repeated_directory = identity_from_metadata(&directory)
            .expect("repeat empty directory");
        #[cfg(windows)]
        let repeated_directory = identity_from_handle(directory.as_handle())
            .expect("repeat empty directory");
        assert_eq!(empty_directory, repeated_directory);
        assert!(std::fs::read(&path).expect("empty content").is_empty());
        assert_eq!(std::fs::read_dir(&directory_path).expect("empty entries").count(), 0);
    }

    #[cfg(windows)]
    #[test]
    // IQV-T06：只读、读写、仅追加的独立原生文件都通过安全 AsHandle 借用；
    // 不提升访问权限，查询后原句柄继续可用，追加夹具仍有精确内容断言。
    fn test_windows_identity_query_accepts_std_file_access_modes() {
        let temporary = fixture();
        let path = temporary.path().join("std-modes.bin");
        std::fs::write(&path, b"identity").expect("prepare native modes source");
        let owner = std::fs::File::open(&path).expect("hold reference source");
        let expected = identity_from_handle(owner.as_handle())
            .expect("reference identity");
        for (read, write, append) in [
            (true, false, false),
            (true, true, false),
            (false, false, true),
        ] {
            let mut file = std::fs::OpenOptions::new()
                .read(read)
                .write(write)
                .append(append)
                .open(&path)
                .expect("open requested native mode");
            let observed = identity_from_handle(file.as_handle())
                .expect("native mode identity");
            assert_eq!(observed, expected);
            assert_eq!(file.metadata().expect("native source remains usable").len(), 8);
            if append {
                std::io::Write::write_all(&mut file, b"+")
                    .expect("use append-only source after query");
            }
        }
        assert_eq!(std::fs::read(&path).expect("native mode content"), b"identity+");
    }

    #[cfg(windows)]
    #[test]
    // IQV-T06：真实 async-fs 来源同样在同步执行点安全借用，不把裸句柄移入
    // 脱离所有者的任务。三种权限逐项验证，夹具追加排空后再读取精确内容。
    fn test_windows_identity_query_accepts_async_file_access_modes() {
        futures_lite::future::block_on(async {
            let temporary = fixture();
            let path = temporary.path().join("async-modes.bin");
            std::fs::write(&path, b"identity").expect("prepare async modes source");
            let owner = std::fs::File::open(&path).expect("hold reference source");
            let expected = identity_from_handle(owner.as_handle())
                .expect("reference identity");
            for (read, write, append) in [
                (true, false, false),
                (true, true, false),
                (false, false, true),
            ] {
                let mut file = async_fs::OpenOptions::new()
                    .read(read)
                    .write(write)
                    .append(append)
                    .open(&path)
                    .await
                    .expect("open requested async mode");
                let observed = identity_from_handle(file.as_handle())
                    .expect("async mode identity");
                assert_eq!(observed, expected);
                assert_eq!(file.metadata().await.expect("async source remains usable").len(), 8);
                if append {
                    futures_lite::io::AsyncWriteExt::write_all(&mut file, b"+")
                        .await
                        .expect("use async append-only source after query");
                    futures_lite::io::AsyncWriteExt::flush(&mut file)
                        .await
                        .expect("drain fixture append");
                }
            }
            assert_eq!(std::fs::read(&path).expect("async mode content"), b"identity+");
        });
    }
}
