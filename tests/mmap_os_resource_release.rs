//! 两类透明映射句柄正常释放与真实 OS 资源归还的专项。
//!
//! MMR-A01~08：产品动作只经过已冻结的 FileNamespace/FileIo/映射句柄接口。
//! Linux proc 与 Windows 只读系统查询独立观察完整映射及附带文件资源，
//! 不读取私有 Arc、租约表，不主动解除映射或关闭生产句柄。
//! 所有地址只保存为整数供查询，Drop 后绝不解引用；物理页缓存不是证明目标。
//! 每项隔离到有截止的真实子进程，全部必须串行独占，不与基准或其它压力并行。
//! 合同/矩阵：.scratch/pi-async-fs-mainline/MMAP_OS_RESOURCE_RELEASE_EXECUTION_2026-10-08.md。

#![cfg(any(target_os = "linux", windows))]

use std::io::{BufRead, Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use futures_lite::future::{self, block_on};
use pi_async_fs::{
    FileAccessMode, FileIo, FileNamespace, LocalFile, LocalFileNamespace,
    MmapRange, ReadMmapHandle, ReadTargetRegion, ReadWriteMmapHandle,
};
use pi_result::ErrorKind;

const CHILD_SCENARIO: &str = "PI_ASYNC_FS_MMAP_RELEASE_CHILD";
const PEER_PERMISSION: &str = "PI_ASYNC_FS_MMAP_RELEASE_PEER_PERMISSION";
const PEER_PATH: &str = "PI_ASYNC_FS_MMAP_RELEASE_PEER_PATH";

/// spawn 成功后立即持有清理责任，覆盖读取线程启动和监测阶段的异常展开。
/// 仅管理测试子进程；正常映射证据仍必须在进程结束前通过公开 Drop 获得。
struct TestChild {
    process: Child,
}

impl TestChild {
    /// 异常清理不再 panic；关闭控制管道，并终止/回收仍存活的本测试进程。
    /// 系统拒绝终止时保留诊断，由外层验证硬截止处理，不以清理失败冒称通过。
    fn terminate_and_reap(&mut self) {
        self.process.stdin.take();
        if matches!(self.process.try_wait(), Ok(Some(_))) { return; }
        match self.process.kill() {
            Ok(()) => {
                if let Err(error) = self.process.wait() {
                    eprintln!("测试子进程回收失败：{error}");
                }
            }
            Err(error) => {
                if !matches!(self.process.try_wait(), Ok(Some(_))) {
                    eprintln!("测试子进程终止失败，必须由外层硬截止处理：{error}");
                }
            }
        }
    }
}

impl Drop for TestChild {
    fn drop(&mut self) {
        self.terminate_and_reap();
    }
}

/// 只管理真实测试进程与时序管道，不持有生产文件、映射或协调资产。
/// 正常路径必须在对端仍存活时完成 Drop 证据；kill 仅用于失败/超时清理。
struct PeerProcess {
    child: TestChild,
    lines: std::sync::mpsc::Receiver<String>,
    stdout: Option<std::thread::JoinHandle<Vec<u8>>>,
    stderr: Option<std::thread::JoinHandle<Vec<u8>>>,
}

impl PeerProcess {
    /// 指定同一个公开测试入口与原生路径；两条输出管道持续排空，避免写满死锁。
    fn spawn(test_name: &str, path: &Path, writable: bool) -> Self {
        let mut child = TestChild { process: Command::new(std::env::current_exe().unwrap())
            .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
            .env(PEER_PERMISSION, if writable { "write" } else { "read" })
            .env(PEER_PATH, path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn().expect("应能启动独立合作进程") };
        let output = child.process.stdout.take().unwrap();
        let mut errors = child.process.stderr.take().unwrap();
        let (sender, lines) = std::sync::mpsc::channel();
        let stdout = std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(output);
            let mut bytes = Vec::new();
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).expect("对端输出必须可读") == 0 { break; }
                bytes.extend_from_slice(line.as_bytes());
                let _ = sender.send(line);
            }
            bytes
        });
        let stderr = std::thread::spawn(move || {
            let mut bytes = Vec::new();
            errors.read_to_end(&mut bytes).expect("对端错误管道必须持续排空");
            bytes
        });
        Self { child, lines, stdout: Some(stdout), stderr: Some(stderr) }
    }

    /// 十秒内必须收到真实对端的阶段确认；不是通过文件伪造产品租约状态。
    fn expect_marker(&self, marker: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = self.lines.recv_timeout(remaining)
                .unwrap_or_else(|error| panic!("对端未在截止内确认 {marker}：{error}"));
            if line.contains(marker) { return; }
        }
    }

    /// 只发送测试动作指令；文件状态及 Drop 均由对端实际公开拥有者执行。
    fn send(&mut self, command: &str) {
        let input = self.child.process.stdin.as_mut().expect("对端输入管道必须存活");
        writeln!(input, "{command}").unwrap();
        input.flush().unwrap();
    }

    /// 证据完成后才允许进程退出；保存其完整原始 OS 输出，不以退出作为释放证明。
    fn finish(&mut self) {
        self.send("exit");
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = self.child.process.try_wait().unwrap() { break status; }
            assert!(Instant::now() < deadline, "证据完成后对端必须正常退出");
            std::thread::sleep(Duration::from_millis(1));
        };
        let output = self.stdout.take().unwrap().join().expect("输出观察线程不得 panic");
        let errors = self.stderr.take().unwrap().join().expect("错误观察线程不得 panic");
        print!("{}", String::from_utf8_lossy(&output));
        assert!(status.success(), "合作进程失败：{}", String::from_utf8_lossy(&errors));
    }
}

impl Drop for PeerProcess {
    fn drop(&mut self) {
        // 测试进程清理，不是生产映射清理。异常展开也不得遗留进程或等待输出死锁。
        self.child.terminate_and_reap();
        if let Some(reader) = self.stdout.take() { let _ = reader.join(); }
        if let Some(reader) = self.stderr.take() { let _ = reader.join(); }
    }
}

/// 对端在正常 Drop 前后自行查询真实 OS；结束确认时文件资源与进程都仍存活。
fn run_coordinated_release_peer(writable: bool) {
    let path = std::path::PathBuf::from(std::env::var_os(PEER_PATH).unwrap());
    // 此独立观察句柄在任一参与者建立授权前关闭，不旁路已有协调域。
    let target = platform::Target::new(&path);
    println!("MMR-PEER-OBSERVATION-READY");
    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();
    let mut input = input.lock();
    let expect_command = |input: &mut std::io::StdinLock<'_>, expected: &str| {
        let mut line = String::new();
        assert_ne!(input.read_line(&mut line).unwrap(), 0, "父进程必须仍握有控制管道");
        assert_eq!(line.trim(), expected);
    };
    expect_command(&mut input, "map");
    let namespace = LocalFileNamespace::new();
    // SAFETY: 两个参与者均已完成观察句柄初始化，此后只使用同目录同名合作式协议。
    let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
    let file = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::ReadWriteMmap)).unwrap();
    for _ in 0..8 { drop(map_with_mode(&file, 2, 7, writable, true).unwrap()); }
    let baseline = target.snapshot();
    assert!(baseline.views.is_empty());
    let mut mapping = map_with_mode(&file, 2, 7, writable, true).unwrap();
    assert_eq!(mapping.as_bytes(), b"23456");
    if let TestMapping::ReadWrite(mapping) = &mut mapping {
        mapping.as_bytes_mut().copy_from_slice(b"ABCDE");
        block_on(mapping.flush()).unwrap();
    }
    let witness = ViewWitness::new(mapping.as_bytes(), 2);
    let live = target.snapshot();
    witness.assert_live(&live);
    println!("MMR-PEER-MAPPING-HELD writable={writable} before={baseline:?} live={live:?}");
    std::io::stdout().flush().unwrap();
    expect_command(&mut input, "drop");
    drop(mapping);
    let released = target.snapshot();
    witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    // file 继续存活；只有映射最后拥有者退出，不能靠文件关闭或进程结束回收。
    assert_eq!(block_on(file.byte_len()).unwrap(), 10);
    println!("MMR-PEER-NORMAL-DROP-COMPLETE writable={writable} after={released:?}");
    std::io::stdout().flush().unwrap();
    expect_command(&mut input, "exit");
}

/// 隔离全局执行池和资源计数；父进程同步等待，输出管道由专用读取线程及时排空。
/// 六十秒是功能验证硬截止，不是产品延迟保证；超时必须终止并回收本测试子进程。
fn enter_isolated_scenario(test_name: &str) -> bool {
    if std::env::var(CHILD_SCENARIO).ok().as_deref() == Some(test_name) {
        return true;
    }
    let mut child = TestChild { process: Command::new(std::env::current_exe().expect("应能定位测试二进制"))
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(CHILD_SCENARIO, test_name)
        .env("BLOCKING_MAX_THREADS", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("应能启动真实隔离子进程") };
    let mut stdout = child.process.stdout.take().expect("应能接管输出管道");
    let mut stderr = child.process.stderr.take().expect("应能接管错误管道");
    let stdout_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).expect("应能完整排空输出");
        bytes
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).expect("应能完整排空错误输出");
        bytes
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.process.try_wait().expect("应能观察子进程") {
            break status;
        }
        if Instant::now() >= deadline {
            child.process.kill().expect("超时必须终止本测试子进程");
            break child.process.wait().expect("超时必须回收本测试子进程");
        }
        std::thread::sleep(Duration::from_millis(5));
    };
    let stdout = stdout_reader.join().expect("输出读取线程不得 panic");
    let stderr = stderr_reader.join().expect("错误读取线程不得 panic");
    print!("{}", String::from_utf8_lossy(&stdout));
    assert!(
        status.success(),
        "资源释放专项失败或超时：{test_name}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stdout),
        String::from_utf8_lossy(&stderr),
    );
    false
}

/// OS 中实际文件支持的连续虚拟区间；不把 Rust 逻辑范围当成原生映射边界。
#[derive(Debug)]
struct NativeView {
    start: usize,
    end: usize,
}

/// 同一次独立查询取得的目标视图与文件资源证据；Windows 总句柄只作为辅助。
#[derive(Debug)]
struct NativeSnapshot {
    views: Vec<NativeView>,
    file_resources: usize,
    process_resources: usize,
}

impl NativeSnapshot {
    /// 完整映射与适用平台的资源计数同时归还；Windows 两个计数字段同源，
    /// 只算一条辅助总句柄证据，不把逻辑重建成功或单个计数当作解映射证明。
    fn assert_empty_at_baseline(&self, baseline: &Self) {
        assert!(self.views.is_empty(), "目标原生视图应全部消失：{self:?}");
        assert_eq!(self.file_resources, baseline.file_resources, "目标/平台文件资源应回基线");
        assert_eq!(self.process_resources, baseline.process_resources, "进程附带资源不得累积");
    }
}

/// 完整原生扩大区间的观察凭证；只保存地址整数，不持有/访问已释放的切片。
#[derive(Debug)]
struct ViewWitness {
    start: usize,
    end: usize,
}

impl ViewWitness {
    /// 在句柄有效时取地址，按实际原生页/分配粒度覆盖隐藏前缀与尾页。
    fn new(bytes: &[u8], file_start: u64) -> Self {
        let (page_size, alignment) = platform::page_parameters();
        let address = bytes.as_ptr() as usize;
        let prefix = usize::try_from(file_start % alignment as u64).unwrap();
        let start = address.checked_sub(prefix).expect("扩大区间起点不能下溢");
        let logical_end = address.checked_add(bytes.len()).expect("逻辑终点不能溢出");
        let end = logical_end.checked_add(page_size - 1).unwrap() / page_size * page_size;
        Self { start, end }
    }

    /// 连续覆盖完整原生区间，而非只测首地址；允许内核合并相邻 VMA。
    fn assert_live(&self, snapshot: &NativeSnapshot) {
        let mut covered_until = self.start;
        for view in &snapshot.views {
            if view.end <= covered_until || view.start >= self.end {
                continue;
            }
            assert!(view.start <= covered_until, "原生目标视图不能有空洞：{self:?} {snapshot:?}");
            covered_until = covered_until.max(view.end.min(self.end));
        }
        assert_eq!(covered_until, self.end, "应完整观察到存活映射：{self:?} {snapshot:?}");
    }

    /// 释放后目标对象不再覆盖原生区间；允许无关合法分配复用原地址。
    fn assert_released(&self, snapshot: &NativeSnapshot) {
        assert!(
            snapshot.views.iter().all(|view| view.end <= self.start || view.start >= self.end),
            "最后真实拥有者释放后不得残留任何目标映射页：{self:?} {snapshot:?}",
        );
    }
}

#[cfg(target_os = "linux")]
mod platform {
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    use super::{NativeSnapshot, NativeView};

    /// 设备号与 inode 匹配，不受路径空格、硬链接或 proc 显示路径标记影响。
    pub(super) struct Target {
        device: u64,
        inode: u64,
    }

    impl Target {
        pub(super) fn new(path: &Path) -> Self {
            let metadata = std::fs::metadata(path).expect("应能独立查询目标身份");
            Self { device: metadata.dev(), inode: metadata.ino() }
        }

        /// 完整读取 proc，不吞掉解析错误；消失的枚举 FD 只按正常生命周期边界处理。
        pub(super) fn snapshot(&self) -> NativeSnapshot {
            let maps = std::fs::read_to_string("/proc/self/maps").expect("必须有真实 proc 映射证据");
            let mut views = Vec::new();
            for line in maps.lines() {
                let mut fields = line.split_whitespace();
                let addresses = fields.next().expect("映射地址字段应存在");
                fields.next().expect("权限字段应存在");
                fields.next().expect("偏移字段应存在");
                let device = fields.next().expect("设备字段应存在");
                let inode: u64 = fields.next().expect("inode 字段应存在").parse().unwrap();
                let (major, minor) = device.split_once(':').unwrap();
                if inode != self.inode
                    || u64::from_str_radix(major, 16).unwrap() != u64::from(libc::major(self.device))
                    || u64::from_str_radix(minor, 16).unwrap() != u64::from(libc::minor(self.device))
                {
                    continue;
                }
                let (start, end) = addresses.split_once('-').unwrap();
                views.push(NativeView {
                    start: usize::from_str_radix(start, 16).unwrap(),
                    end: usize::from_str_radix(end, 16).unwrap(),
                });
            }
            let mut file_resources = 0;
            let mut process_resources = 0;
            for entry in std::fs::read_dir("/proc/self/fd").expect("必须有真实 FD 证据") {
                let entry = entry.expect("FD 枚举错误不得忽略");
                // 观察目录自身的一个 FD 在每次快照中都存在；初/终比较同一口径。
                process_resources += 1;
                match std::fs::metadata(entry.path()) {
                    Ok(metadata) if metadata.dev() == self.device && metadata.ino() == self.inode => {
                        file_resources += 1;
                    }
                    Ok(_) => {}
                    // proc 是实时资源目录；枚举后已关闭的非目标 FD 不再占用资源。
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => panic!("无法查询 FD，不能冒称通过：{error}"),
                }
            }
            NativeSnapshot { views, file_resources, process_resources }
        }
    }

    pub(super) fn page_parameters() -> (usize, usize) {
        // SAFETY: sysconf 查询进程公开的页大小，不访问或修改映射内容。
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        let page_size = usize::try_from(page_size).expect("系统必须提供正页大小");
        assert!(page_size.is_power_of_two());
        (page_size, page_size)
    }
}

#[cfg(windows)]
mod platform {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;

    use windows_sys::Win32::Storage::FileSystem::{GetFinalPathNameByHandleW, VOLUME_NAME_NT};

    use super::{NativeSnapshot, NativeView};

    // 测试专用只读 FFI。布局/签名对照 windows-sys 0.61.2 对应模块及微软头文件；
    // 不为观测器扩大生产依赖 feature。保留全部 ABI 字段，未读取字段不是产品状态。
    #[repr(C)]
    #[allow(dead_code)]
    struct MemoryBasicInformation {
        base_address: *mut c_void,
        allocation_base: *mut c_void,
        allocation_protect: u32,
        #[cfg(target_pointer_width = "64")]
        partition_id: u16,
        region_size: usize,
        state: u32,
        protect: u32,
        kind: u32,
    }

    #[repr(C)]
    #[allow(dead_code)]
    struct SystemInfo {
        architecture_and_reserved: u32,
        page_size: u32,
        minimum_address: *mut c_void,
        maximum_address: *mut c_void,
        active_processor_mask: usize,
        processors: u32,
        processor_type: u32,
        allocation_granularity: u32,
        processor_level: u16,
        processor_revision: u16,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn VirtualQuery(address: *const c_void, output: *mut MemoryBasicInformation, length: usize) -> usize;
        fn GetSystemInfo(output: *mut SystemInfo);
        fn GetCurrentProcess() -> *mut c_void;
        fn GetProcessHandleCount(process: *mut c_void, count: *mut u32) -> i32;
        fn K32GetMappedFileNameW(process: *mut c_void, address: *const c_void, name: *mut u16, size: u32) -> u32;
    }

    fn system_info() -> SystemInfo {
        let mut info = std::mem::MaybeUninit::uninit();
        // SAFETY: GetSystemInfo 填充完整的、已按平台 ABI 声明的输出结构。
        unsafe { GetSystemInfo(info.as_mut_ptr()); info.assume_init() }
    }

    pub(super) fn page_parameters() -> (usize, usize) {
        let info = system_info();
        assert!(info.page_size.is_power_of_two());
        assert!(info.allocation_granularity.is_power_of_two());
        (info.page_size as usize, info.allocation_granularity as usize)
    }

    /// 独立原生 NT 路径作为此夹具的匹配键；测试期间不改名/替换目标文件。
    pub(super) struct Target {
        native_name: Vec<u16>,
    }

    impl Target {
        pub(super) fn new(path: &Path) -> Self {
            let file = std::fs::File::open(path).expect("应能独立打开观测对象");
            let mut name = vec![0_u16; 32768];
            // SAFETY: 观测句柄在调用中存活；输出为足够容量的 UTF-16 可写缓冲区。
            let len = unsafe {
                GetFinalPathNameByHandleW(file.as_raw_handle(), name.as_mut_ptr(), name.len() as u32, VOLUME_NAME_NT)
            } as usize;
            assert!(len > 0 && len < name.len(), "独立身份路径查询失败：{}", std::io::Error::last_os_error());
            name.truncate(len);
            Self { native_name: name }
        }

        /// 按 VQ 的 RegionSize 推进全部用户地址空间，精确收集该目标的所有区间。
        pub(super) fn snapshot(&self) -> NativeSnapshot {
            let info = system_info();
            let mut address = info.minimum_address as usize;
            let maximum_address = info.maximum_address as usize;
            let mut views = Vec::new();
            let mut name = vec![0_u16; 32768];
            while address < maximum_address {
                let mut output = std::mem::MaybeUninit::<MemoryBasicInformation>::uninit();
                // SAFETY: 地址仅作为查询键，不解引用；缓冲区大小与已核验 ABI 一致。
                let written = unsafe {
                    VirtualQuery(address as *const c_void, output.as_mut_ptr(), std::mem::size_of::<MemoryBasicInformation>())
                };
                assert_eq!(written, std::mem::size_of::<MemoryBasicInformation>(), "VirtualQuery 失败：{}", std::io::Error::last_os_error());
                // SAFETY: 上一调用已经成功填充整个输出结构。
                let region = unsafe { output.assume_init() };
                let start = region.base_address as usize;
                let end = start.checked_add(region.region_size).expect("OS 区间不能溢出");
                assert!(end > address, "OS 查询必须严格推进");
                if region.state == 0x1000 && region.kind == 0x40000 {
                    // SAFETY: 只查询当前进程的映射名称，不读取映射字节或取得所有权。
                    let len = unsafe {
                        K32GetMappedFileNameW(GetCurrentProcess(), start as *const c_void, name.as_mut_ptr(), name.len() as u32)
                    } as usize;
                    if len == 0 {
                        // 1006 表示该节不是可查询名称的文件对象（如页文件支持节）。
                        // 任意其它观测错误明确失败，不能折叠为“目标没有映射”。
                        let error = std::io::Error::last_os_error();
                        assert_eq!(error.raw_os_error(), Some(1006), "映射名称观测失败：{error}");
                    } else {
                        assert!(len < name.len(), "不能使用截断的映射名称");
                        if name[..len] == self.native_name {
                            views.push(NativeView { start, end });
                        }
                    }
                }
                address = end;
            }
            let mut count = 0;
            // SAFETY: 当前进程伪句柄合法，输出指向一个可写 DWORD；不关闭该句柄。
            let succeeded = unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
            assert_ne!(succeeded, 0, "原生句柄计数失败：{}", std::io::Error::last_os_error());
            NativeSnapshot { views, file_resources: count as usize, process_resources: count as usize }
        }
    }
}

/// 只转发公开未协调映射；夹具独占文件，无库外截断、替换或写入参与者。
fn map_read_only(file: &LocalFile, start: u64, end: u64) -> ReadMmapHandle {
    // SAFETY: 调用方的隔离目录及独占测试时序持续满足未协调入口的外部前提。
    unsafe { block_on(file.map_read_only_uncoordinated(MmapRange::new(start, end).unwrap())) }
        .expect("合法只读映射应建立")
}

/// 两种真实公开拥有型结果的测试收纳；不实现 Drop，不补做任何生产资源清理。
#[derive(Debug)]
enum TestMapping {
    ReadOnly(ReadMmapHandle),
    ReadWrite(ReadWriteMmapHandle),
}

impl TestMapping {
    /// 只转发仍存活句柄的公开视图，不增加所有权，也不访问内部原生对象。
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::ReadOnly(mapping) => mapping.as_bytes(),
            Self::ReadWrite(mapping) => mapping.as_bytes(),
        }
    }
}

/// 转发四个冻结入口；bool 只选择本测试动作，不改变权限或协调规则。
fn map_with_mode(
    file: &LocalFile,
    start: u64,
    end: u64,
    writable: bool,
    coordinated: bool,
) -> pi_result::Result<TestMapping> {
    let range = MmapRange::new(start, end).expect("调用方应提供合法半开区间");
    match (writable, coordinated) {
        (false, true) => block_on(file.map_read_only(range)).map(TestMapping::ReadOnly),
        (true, true) => block_on(file.map_read_write(range)).map(TestMapping::ReadWrite),
        // SAFETY: 所有未协调场景在独占夹具内执行，无库外截断、替换或写入。
        (false, false) => unsafe { block_on(file.map_read_only_uncoordinated(range)) }
            .map(TestMapping::ReadOnly),
        // SAFETY: 同上；可写视图仅通过返回的非 Sync 透明句柄独占操作。
        (true, false) => unsafe { block_on(file.map_read_write_uncoordinated(range)) }
            .map(TestMapping::ReadWrite),
    }
}

/// 创建小型真实文件，所有产品动作仍由公开资源进行；观测器不参与清理。
fn prepare_file(path: &Path) -> (LocalFile, platform::Target) {
    std::fs::write(path, b"0123456789").expect("应能建立真实文件");
    let target = platform::Target::new(path);
    let locator = path.to_path_buf();
    let file = block_on(LocalFileNamespace::new().open(&locator, FileAccessMode::ReadMmap))
        .expect("应能打开真实映射资源");
    (file, target)
}

/// 通过普通读取观察范围资格，不读私有登记；活动映射必须在副作用前冲突。
fn assert_content_read_conflicts(reader: &LocalFile) {
    let mut byte = [0xA5_u8];
    let error = block_on(reader.read_exact_at(0, &mut byte, ReadTargetRegion::full()))
        .expect_err("真实映射持有者存活时普通读取必须冲突");
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(byte, [0xA5]);
}

/// 只占用实际唯一 blocking worker；通道控制执行环境，不模拟任何文件动作。
/// blocker 自身十秒有界，释放后仍由真实生产任务完成刷新/写入与资源清理。
fn occupy_only_worker() -> (
    std::sync::mpsc::Sender<()>,
    impl std::future::Future<Output = ()>,
) {
    let (started_sender, started_receiver) = std::sync::mpsc::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::channel();
    let occupied = blocking::unblock(move || {
        started_sender.send(()).unwrap();
        release_receiver.recv_timeout(Duration::from_secs(10))
            .expect("测试必须在截止内归还真实 worker");
    });
    started_receiver.recv_timeout(Duration::from_secs(10))
        .expect("真实 worker 必须已经被占用");
    (release_sender, occupied)
}

/// 只有公开读取已恢复且得到独立预期字节，才认定后台拥有者完成；不单看空快照。
fn wait_for_content_read(reader: &LocalFile, expected: &[u8]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = vec![0xA5; expected.len()];
    loop {
        match block_on(reader.read_exact_at(0, &mut bytes, ReadTargetRegion::full())) {
            Ok(()) => { assert_eq!(bytes, expected); return; }
            Err(error) => {
                assert_eq!(error.current_context(), &ErrorKind::Conflict);
                assert!(Instant::now() < deadline, "后台必须在截止内正常完成并归还资格");
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

/// 标准 Future 唤醒观测，不读取私有任务状态；用于让真实元信息完成后再占用 worker。
struct ObservedWake(std::sync::atomic::AtomicBool);

impl std::task::Wake for ObservedWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.store(true, std::sync::atomic::Ordering::Release);
    }
}

/// MMR-A01：真实观测器校准；持有者不 Drop 时必须仍映射，最后 Drop 后整个原生
/// 区间消失、目标视图全集为空、附带资源回到预热基线。不是物理缓存/RSS 测试。
#[test]
fn test_read_only_mmap_drop_releases_full_native_view_and_file_resources() {
    let test_name = "test_read_only_mmap_drop_releases_full_native_view_and_file_resources";
    if !enter_isolated_scenario(test_name) {
        return;
    }
    let temporary = tempfile::tempdir().expect("应能建立隔离目录");
    let path = temporary.path().join("release-source");
    let (file, target) = prepare_file(&path);
    for _ in 0..8 {
        drop(map_read_only(&file, 2, 7));
    }
    let baseline = target.snapshot();
    assert!(baseline.views.is_empty(), "建立前不得已有目标映射");
    let mapping = map_read_only(&file, 2, 7);
    assert_eq!(mapping.as_bytes(), b"23456");
    let witness = ViewWitness::new(mapping.as_bytes(), 2);
    let live = target.snapshot();
    witness.assert_live(&live);
    witness.assert_live(&target.snapshot());
    drop(mapping);
    let released = target.snapshot();
    witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    println!("MMR-A01 before={baseline:?} live={live:?} after={released:?} witness={witness:?}");
}

/// MMR-A02：真实两权限的最小、页内未对齐、跨页、分配粒度和文件末端范围。
/// 每一组合同时证明完整原生扩大区间释放与资源回归；可写场景实际修改透明视图。
#[test]
fn test_both_mmap_permissions_release_native_views_at_every_alignment_boundary() {
    let test_name = "test_both_mmap_permissions_release_native_views_at_every_alignment_boundary";
    if !enter_isolated_scenario(test_name) {
        return;
    }
    let (page, alignment) = platform::page_parameters();
    let length = alignment * 4;
    let temporary = tempfile::tempdir().expect("应能建立隔离目录");
    let path = temporary.path().join("alignment-source");
    std::fs::write(&path, vec![b'S'; length]).expect("应能建立小型真实文件");
    let target = platform::Target::new(&path);
    let namespace = LocalFileNamespace::new();
    for writable in [false, true] {
        let access = if writable { FileAccessMode::ReadWriteMmap } else { FileAccessMode::ReadMmap };
        let file = block_on(namespace.open(&path, access)).expect("应能打开对应权限资源");
        for _ in 0..8 {
            drop(map_with_mode(&file, 2, 7, writable, false).unwrap());
        }
        let baseline = target.snapshot();
        assert!(baseline.views.is_empty());
        for (start, end) in [
            (0, 1), (2, 7), (page - 3, page + 5),
            (alignment - 3, alignment + 3), (length - 1, length),
        ] {
            let mut mapping = map_with_mode(&file, start as u64, end as u64, writable, false)
                .expect("所有合法边界应建立真实映射");
            assert_eq!(mapping.as_bytes().len(), end - start);
            assert!(mapping.as_bytes().iter().all(|byte| *byte == b'S'));
            if let TestMapping::ReadWrite(mapping) = &mut mapping {
                mapping.as_bytes_mut()[0] = b'W';
                assert_eq!(mapping.as_bytes()[0], b'W');
                mapping.as_bytes_mut()[0] = b'S';
            }
            let witness = ViewWitness::new(mapping.as_bytes(), start as u64);
            let live = target.snapshot();
            witness.assert_live(&live);
            drop(mapping);
            let released = target.snapshot();
            witness.assert_released(&released);
            released.assert_empty_at_baseline(&baseline);
            println!("MMR-A02 writable={writable} range=[{start},{end}) page={page} alignment={alignment} before={baseline:?} live={live:?} after={released:?}");
        }
    }
}

/// MMR-A03：原件、多个只读克隆分次跨线程释放；非末尾 Drop 不得解映射或
/// 开放普通读取。最后一个真实克隆 Drop 后，OS 完整区间与公开资格同时归还。
#[test]
fn test_read_only_mmap_clones_keep_native_view_until_last_cross_thread_owner_drops() {
    let test_name = "test_read_only_mmap_clones_keep_native_view_until_last_cross_thread_owner_drops";
    if !enter_isolated_scenario(test_name) {
        return;
    }
    let temporary = tempfile::tempdir().expect("应能建立隔离目录");
    let path = temporary.path().join("clone-source");
    let (file, target) = prepare_file(&path);
    let reader = block_on(LocalFileNamespace::new().open(&path, FileAccessMode::Read)).unwrap();
    for _ in 0..8 { drop(map_read_only(&file, 2, 7)); }
    let baseline = target.snapshot();
    let mapping = map_read_only(&file, 2, 7);
    let witness = ViewWitness::new(mapping.as_bytes(), 2);
    let first_clone = mapping.clone();
    let last_clone = mapping.clone();
    assert_eq!(first_clone.as_bytes().as_ptr(), mapping.as_bytes().as_ptr());
    assert_eq!(last_clone.as_bytes().as_ptr(), mapping.as_bytes().as_ptr());
    let original_live = target.snapshot();
    drop(mapping);
    witness.assert_live(&target.snapshot());
    assert_content_read_conflicts(&reader);
    std::thread::spawn(move || {
        assert_eq!(first_clone.as_bytes(), b"23456");
        drop(first_clone);
    }).join().expect("克隆释放线程不得 panic");
    witness.assert_live(&target.snapshot());
    assert_content_read_conflicts(&reader);

    let (ready_sender, ready_receiver) = std::sync::mpsc::channel();
    let (release_sender, release_receiver) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert_eq!(last_clone.as_bytes(), b"23456");
        ready_sender.send(()).unwrap();
        release_receiver.recv_timeout(Duration::from_secs(10)).expect("必须及时允许末尾释放");
        assert_eq!(last_clone.as_bytes(), b"23456");
        drop(last_clone);
    });
    ready_receiver.recv_timeout(Duration::from_secs(10)).expect("末尾克隆应真实存活");
    witness.assert_live(&target.snapshot());
    assert_content_read_conflicts(&reader);
    release_sender.send(()).unwrap();
    worker.join().expect("末尾释放线程不得 panic");
    let released = target.snapshot();
    witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    let mut byte = [0xA5];
    block_on(reader.read_exact_at(0, &mut byte, ReadTargetRegion::full()))
        .expect("末尾释放后普通读取资格应恢复");
    assert_eq!(&byte, b"0");
    println!("MMR-A03 before={baseline:?} shared_live={original_live:?} after={released:?}");
}

/// MMR-A02：同文件三个不相交逻辑范围以混合权限并存；逐个 Drop 只释放自己
/// 的完整原生区间，其它实例仍可访问且仍保护普通读取，不以全部同时 Drop 掩盖错误。
#[test]
fn test_disjoint_mmap_drop_releases_only_its_native_view() {
    let test_name = "test_disjoint_mmap_drop_releases_only_its_native_view";
    if !enter_isolated_scenario(test_name) { return; }
    let (page, alignment) = platform::page_parameters();
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("disjoint-source");
    std::fs::write(&path, vec![b'S'; alignment * 4]).unwrap();
    let target = platform::Target::new(&path);
    let namespace = LocalFileNamespace::new();
    let file = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap();
    let reader = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    for _ in 0..8 { drop(map_with_mode(&file, 2, 7, true, false).unwrap()); }
    let baseline = target.snapshot();
    let first = map_with_mode(&file, 2, 7, false, false).unwrap();
    let middle = map_with_mode(&file, (page + 2) as u64, (page + 7) as u64, true, false).unwrap();
    let last = map_with_mode(&file, (alignment * 2 + 2) as u64, (alignment * 2 + 7) as u64, false, false).unwrap();
    let first_witness = ViewWitness::new(first.as_bytes(), 2);
    let middle_witness = ViewWitness::new(middle.as_bytes(), (page + 2) as u64);
    let last_witness = ViewWitness::new(last.as_bytes(), (alignment * 2 + 2) as u64);
    let all_live = target.snapshot();
    for witness in [&first_witness, &middle_witness, &last_witness] { witness.assert_live(&all_live); }
    assert_content_read_conflicts(&reader);
    drop(middle);
    let middle_released = target.snapshot();
    middle_witness.assert_released(&middle_released);
    first_witness.assert_live(&middle_released);
    last_witness.assert_live(&middle_released);
    assert_eq!(first.as_bytes(), b"SSSSS");
    assert_eq!(last.as_bytes(), b"SSSSS");
    assert_content_read_conflicts(&reader);
    drop(first);
    let first_released = target.snapshot();
    first_witness.assert_released(&first_released);
    last_witness.assert_live(&first_released);
    assert_content_read_conflicts(&reader);
    drop(last);
    let released = target.snapshot();
    last_witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    let mut byte = [0xA5];
    block_on(reader.read_exact_at(0, &mut byte, ReadTargetRegion::full())).unwrap();
    assert_eq!(&byte, b"S");
    println!("MMR-A02-multi before={baseline:?} all_live={all_live:?} middle_drop={middle_released:?} first_drop={first_released:?} after={released:?}");
}

/// MMR-A02：先释放公开 FileIo 资源不应撤销独立透明句柄；两权限都在文件
/// 资源退出后继续读/写。最后句柄释放后原生视图为空，附带文件资源回无打开资源基线。
#[test]
fn test_mmap_outlives_open_file_resource_and_last_drop_returns_remaining_resources() {
    let test_name = "test_mmap_outlives_open_file_resource_and_last_drop_returns_remaining_resources";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("outliving-source");
    std::fs::write(&path, b"0123456789").unwrap();
    let target = platform::Target::new(&path);
    let namespace = LocalFileNamespace::new();
    for writable in [false, true] {
        let access = if writable { FileAccessMode::ReadWriteMmap } else { FileAccessMode::ReadMmap };
        let warm = block_on(namespace.open(&path, access)).unwrap();
        for _ in 0..8 { drop(map_with_mode(&warm, 2, 7, writable, false).unwrap()); }
        drop(warm);
        let baseline = target.snapshot();
        assert!(baseline.views.is_empty());
        let access = if writable { FileAccessMode::ReadWriteMmap } else { FileAccessMode::ReadMmap };
        let file = block_on(namespace.open(&path, access)).unwrap();
        let mut mapping = map_with_mode(&file, 2, 7, writable, false).unwrap();
        let witness = ViewWitness::new(mapping.as_bytes(), 2);
        drop(file);
        assert_eq!(mapping.as_bytes(), b"23456");
        if let TestMapping::ReadWrite(mapping) = &mut mapping {
            mapping.as_bytes_mut()[0] = b'W';
            assert_eq!(mapping.as_bytes(), b"W3456");
            mapping.as_bytes_mut()[0] = b'2';
            block_on(mapping.flush()).unwrap();
        }
        let live_without_file = target.snapshot();
        witness.assert_live(&live_without_file);
        drop(mapping);
        let released = target.snapshot();
        witness.assert_released(&released);
        released.assert_empty_at_baseline(&baseline);
        println!("MMR-A02-outlive writable={writable} before={baseline:?} file_drop={live_without_file:?} after={released:?}");
    }
}

/// MMR-A04：只读映射作为异文件追加源，借用成功不释放原件、owned 成功释放
/// 最后源；同文件失败明确返还原句柄，错误仍保活映射，真正 Drop 后才归还 OS 资源。
#[test]
fn test_mmap_append_source_success_and_returned_failure_have_exact_os_lifetimes() {
    let test_name = "test_mmap_append_source_success_and_returned_failure_have_exact_os_lifetimes";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("buffer-source");
    let destination = temporary.path().join("buffer-target");
    let (file, target) = prepare_file(&source);
    std::fs::write(&destination, b"prefix|").unwrap();
    let namespace = LocalFileNamespace::new();
    let mut append = block_on(namespace.open(&destination, FileAccessMode::Append)).unwrap();
    let mut same_file_append = block_on(namespace.open(&source, FileAccessMode::Append)).unwrap();
    for _ in 0..8 { drop(map_read_only(&file, 2, 7)); }
    block_on(append.append(Vec::<u8>::new())).unwrap();
    let baseline = target.snapshot();

    let borrowed = map_read_only(&file, 2, 7);
    let borrowed_witness = ViewWitness::new(borrowed.as_bytes(), 2);
    block_on(append.append(&borrowed)).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"prefix|23456");
    borrowed_witness.assert_live(&target.snapshot());
    assert_eq!(borrowed.as_bytes(), b"23456");
    drop(borrowed);
    target.snapshot().assert_empty_at_baseline(&baseline);

    let owned = map_read_only(&file, 2, 7);
    let owned_witness = ViewWitness::new(owned.as_bytes(), 2);
    owned_witness.assert_live(&target.snapshot());
    block_on(append.append(owned)).unwrap();
    assert_eq!(std::fs::read(&destination).unwrap(), b"prefix|2345623456");
    let owned_released = target.snapshot();
    owned_witness.assert_released(&owned_released);
    owned_released.assert_empty_at_baseline(&baseline);

    let source_handle = map_read_only(&file, 2, 7);
    let failure_witness = ViewWitness::new(source_handle.as_bytes(), 2);
    let failure = block_on(same_file_append.append(source_handle)).expect_err("同文件映射源必须拒绝");
    let (error, returned, progress) = failure.into_parts();
    assert_eq!(error.current_context(), &ErrorKind::Conflict);
    assert_eq!(progress, pi_async_fs::TransferProgress::Exact { bytes: 0 });
    assert_eq!(std::fs::read(&source).unwrap(), b"0123456789");
    assert_eq!(returned.as_bytes(), b"23456");
    failure_witness.assert_live(&target.snapshot());
    drop(returned);
    let failure_released = target.snapshot();
    failure_witness.assert_released(&failure_released);
    failure_released.assert_empty_at_baseline(&baseline);
    println!("MMR-A04 before={baseline:?} owned_success={owned_released:?} returned_failure_drop={failure_released:?}");
}

/// MMR-A02/A04：可写映射作为异文件固定读取的真实目标，正常及 EOF 错误只结束
/// 借用，不释放调用方仍持有的映射；完成显式刷新并 Drop 后资源全部归还。
#[test]
fn test_mmap_fixed_read_target_remains_live_after_success_or_eof_until_owner_drop() {
    let test_name = "test_mmap_fixed_read_target_remains_live_after_success_or_eof_until_owner_drop";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("read-source");
    let destination = temporary.path().join("read-target");
    std::fs::write(&source, b"ABCDEFGHIJ").unwrap();
    std::fs::write(&destination, b"0123456789").unwrap();
    let namespace = LocalFileNamespace::new();
    let mut reader = block_on(namespace.open(&source, FileAccessMode::Read)).unwrap();
    let mapper = block_on(namespace.open(&destination, FileAccessMode::ReadWriteMmap)).unwrap();
    let target = platform::Target::new(&destination);
    for _ in 0..8 { drop(map_with_mode(&mapper, 2, 7, true, false).unwrap()); }
    let baseline = target.snapshot();
    let mut mapping = match map_with_mode(&mapper, 2, 7, true, false).unwrap() {
        TestMapping::ReadWrite(mapping) => mapping,
        TestMapping::ReadOnly(_) => unreachable!(),
    };
    let witness = ViewWitness::new(mapping.as_bytes(), 2);
    block_on(reader.read_exact_at(0, &mut mapping, ReadTargetRegion::full())).unwrap();
    assert_eq!(mapping.as_bytes(), b"ABCDE");
    witness.assert_live(&target.snapshot());
    block_on(reader.read_exact(&mut mapping, ReadTargetRegion::full())).unwrap();
    block_on(reader.read_exact(&mut mapping, ReadTargetRegion::full())).unwrap();
    assert_eq!(mapping.as_bytes(), b"FGHIJ");
    let error = block_on(reader.read_exact_at(10, &mut mapping, ReadTargetRegion::full()))
        .expect_err("EOF 边界必须明确失败");
    assert_eq!(error.current_context(), &ErrorKind::Io);
    assert_eq!(error.downcast_ref::<std::io::Error>().unwrap().kind(), std::io::ErrorKind::UnexpectedEof);
    assert_eq!(mapping.as_bytes(), b"FGHIJ");
    witness.assert_live(&target.snapshot());
    block_on(mapping.flush()).unwrap();
    drop(mapping);
    let released = target.snapshot();
    witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    assert_eq!(std::fs::read(&destination).unwrap(), b"01FGHIJ789");
    println!("MMR-A04-fixed-target before={baseline:?} after={released:?}");
}

/// MMR-A05：未轮询刷新不提交后台工作；已实际排队的刷新取消并 Drop 公开
/// 句柄后仍保活整个原生映射，不能提前开放读取；真实完成后 OS/资格/句柄均归还。
#[test]
fn test_cancelled_mmap_flush_keeps_native_mapping_until_real_queued_work_finishes() {
    let test_name = "test_cancelled_mmap_flush_keeps_native_mapping_until_real_queued_work_finishes";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("flush-source");
    std::fs::write(&path, b"0123456789").unwrap();
    let namespace = LocalFileNamespace::new();
    let mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap();
    let reader = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    let target = platform::Target::new(&path);
    for _ in 0..8 {
        let TestMapping::ReadWrite(mut mapping) = map_with_mode(&mapper, 2, 7, true, false).unwrap() else { unreachable!() };
        block_on(mapping.flush()).unwrap();
    }
    let baseline = target.snapshot();
    let TestMapping::ReadWrite(mut unpolled) = map_with_mode(&mapper, 2, 7, true, false).unwrap() else { unreachable!() };
    let unpolled_witness = ViewWitness::new(unpolled.as_bytes(), 2);
    drop(unpolled.flush());
    unpolled_witness.assert_live(&target.snapshot());
    drop(unpolled);
    target.snapshot().assert_empty_at_baseline(&baseline);

    let TestMapping::ReadWrite(mut mapping) = map_with_mode(&mapper, 2, 7, true, false).unwrap() else { unreachable!() };
    mapping.as_bytes_mut().copy_from_slice(b"ABCDE");
    let witness = ViewWitness::new(mapping.as_bytes(), 2);
    let (release, occupied) = occupy_only_worker();
    assert!(block_on(future::poll_once(mapping.flush())).is_none(), "worker 未释放前真实刷新不可能完成");
    drop(mapping);
    let retained = target.snapshot();
    witness.assert_live(&retained);
    assert_content_read_conflicts(&reader);
    release.send(()).unwrap();
    block_on(occupied);
    wait_for_content_read(&reader, b"01ABCDE789");
    let released = target.snapshot();
    witness.assert_released(&released);
    released.assert_empty_at_baseline(&baseline);
    println!("MMR-A05 before={baseline:?} cancelled_and_handle_drop={retained:?} completed={released:?}");
}

/// MMR-A04：owned/borrowed 两种真实来源取消后，后台尚在排队时仍必须保活
/// 映射；目标实际收到精确追加内容后才归还来源，测试侧不复制来源、不补做写入。
#[test]
fn test_cancelled_append_keeps_owned_and_borrowed_mmap_sources_until_real_completion() {
    let test_name = "test_cancelled_append_keeps_owned_and_borrowed_mmap_sources_until_real_completion";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("cancelled-buffer-source");
    let destination = temporary.path().join("cancelled-buffer-target");
    let (mapper, target) = prepare_file(&source);
    std::fs::write(&destination, b"prefix|").unwrap();
    let namespace = LocalFileNamespace::new();
    let reader = block_on(namespace.open(&source, FileAccessMode::Read)).unwrap();
    let mut writer = block_on(namespace.open(&destination, FileAccessMode::Append)).unwrap();
    for _ in 0..8 { drop(map_read_only(&mapper, 2, 7)); }
    let baseline = target.snapshot();
    for (borrowed, expected) in [(false, &b"prefix|23456"[..]), (true, &b"prefix|2345623456"[..])] {
        let mut mapping = Some(map_read_only(&mapper, 2, 7));
        let witness = ViewWitness::new(mapping.as_ref().unwrap().as_bytes(), 2);
        let (release, occupied) = occupy_only_worker();
        if borrowed {
            assert!(block_on(future::poll_once(writer.append(mapping.as_ref().unwrap()))).is_none());
            drop(mapping.take());
        } else {
            assert!(block_on(future::poll_once(writer.append(mapping.take().unwrap()))).is_none());
        }
        let retained = target.snapshot();
        witness.assert_live(&retained);
        assert_content_read_conflicts(&reader);
        release.send(()).unwrap();
        block_on(occupied);
        assert_eq!(block_on(writer.byte_len()).unwrap(), expected.len() as u64);
        wait_for_content_read(&reader, b"0123456789");
        assert_eq!(std::fs::read(&destination).unwrap(), expected);
        let released = target.snapshot();
        witness.assert_released(&released);
        released.assert_empty_at_baseline(&baseline);
        println!("MMR-A04-cancel borrowed={borrowed} before={baseline:?} retained={retained:?} completed={released:?}");
    }
}

/// MMR-A06：无轮询无副作用；元信息完成后将原生建图实际排队，再取消调用方。
/// 被取消的生产完成者仍保护范围，随后自行清理无人接收的结果；测试不补做建图/Drop。
#[test]
fn test_cancelled_mapping_creation_cleans_undelivered_result_and_native_resources() {
    use std::future::Future;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, Waker};

    let test_name = "test_cancelled_mapping_creation_cleans_undelivered_result_and_native_resources";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("cancelled-creation-source");
    std::fs::write(&path, b"0123456789").unwrap();
    let namespace = LocalFileNamespace::new();
    let mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap();
    let reader = block_on(namespace.open(&path, FileAccessMode::Read)).unwrap();
    let target = platform::Target::new(&path);
    for writable in [false, true] {
        for _ in 0..8 { drop(map_with_mode(&mapper, 2, 7, writable, false).unwrap()); }
        let baseline = target.snapshot();
        let create = || async {
            let range = MmapRange::new(2, 7).unwrap();
            // SAFETY: 独占夹具，没有库外修改；只控制实际执行池，不伪造产品状态。
            if writable {
                unsafe { mapper.map_read_write_uncoordinated(range).await }.map(TestMapping::ReadWrite)
            } else {
                unsafe { mapper.map_read_only_uncoordinated(range).await }.map(TestMapping::ReadOnly)
            }
        };
        drop(create());
        target.snapshot().assert_empty_at_baseline(&baseline);

        let (release_metadata, metadata_worker) = occupy_only_worker();
        let wake = std::sync::Arc::new(ObservedWake(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(create());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        release_metadata.send(()).unwrap();
        block_on(metadata_worker);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !wake.0.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "真实元信息完成必须唤醒公开 Future");
            std::thread::sleep(Duration::from_millis(1));
        }

        // 元信息结果已经就绪；重新占用真实 worker，再轮询公开 Future，使建图
        // 完成者已经接管文件/预留而原生操作仍排队。保活由生产完成者独立承担。
        let (release_mapping, mapping_worker) = occupy_only_worker();
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        drop(future);
        assert_content_read_conflicts(&reader);
        assert!(target.snapshot().views.is_empty(), "排队阶段原生图尚未建立");
        release_mapping.send(()).unwrap();
        block_on(mapping_worker);
        wait_for_content_read(&reader, b"0123456789");
        let released = target.snapshot();
        released.assert_empty_at_baseline(&baseline);
        let replacement = map_with_mode(&mapper, 2, 7, writable, false).unwrap();
        assert_eq!(replacement.as_bytes(), b"23456");
        drop(replacement);
        target.snapshot().assert_empty_at_baseline(&baseline);
        println!("MMR-A06-cancel writable={writable} before={baseline:?} undelivered_result_released={released:?}");
    }
}

/// MMR-A06：两权限的角色/授权/EOF/极大长度错误，及真实相交范围冲突，均有
/// 精确分类与 OS 初终证据；有效 guard 不能因另一请求失败而被提前释放。
#[test]
fn test_rejected_mapping_requests_leave_no_native_view_or_file_resource_residue() {
    let test_name = "test_rejected_mapping_requests_leave_no_native_view_or_file_resource_residue";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("rejection-source");
    let (ordinary, target) = prepare_file(&path);
    drop(ordinary);
    let namespace = LocalFileNamespace::new();
    for access in [FileAccessMode::Read, FileAccessMode::Append, FileAccessMode::Overwrite, FileAccessMode::Truncate] {
        let file = block_on(namespace.open(&path, access)).unwrap();
        let baseline = target.snapshot();
        for writable in [false, true] {
            let error = map_with_mode(&file, 2, 7, writable, false).unwrap_err();
            assert_eq!(error.current_context(), &ErrorKind::InvalidState);
            target.snapshot().assert_empty_at_baseline(&baseline);
        }
    }
    let readonly = block_on(namespace.open(&path, FileAccessMode::ReadMmap)).unwrap();
    let baseline = target.snapshot();
    let error = map_with_mode(&readonly, 2, 7, true, false).unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);
    target.snapshot().assert_empty_at_baseline(&baseline);
    drop(readonly);
    let mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap();
    for _ in 0..8 { drop(map_with_mode(&mapper, 2, 7, true, false).unwrap()); }
    let baseline = target.snapshot();
    assert!(MmapRange::new(3, 3).is_err());
    assert!(MmapRange::new(7, 2).is_err());
    for writable in [false, true] {
        let error = map_with_mode(&mapper, 2, 7, writable, true).unwrap_err();
        assert_eq!(error.current_context(), &ErrorKind::InvalidState);
        target.snapshot().assert_empty_at_baseline(&baseline);
        for (start, end) in [(9, 11), (0, isize::MAX as u64 + 1), (u64::MAX - 1, u64::MAX)] {
            let error = map_with_mode(&mapper, start, end, writable, false).unwrap_err();
            assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
            target.snapshot().assert_empty_at_baseline(&baseline);
        }
        let mapping = map_with_mode(&mapper, 2, 7, writable, false).unwrap();
        let witness = ViewWitness::new(mapping.as_bytes(), 2);
        for conflicting_permission in [false, true] {
            let error = map_with_mode(&mapper, 6, 8, conflicting_permission, false).unwrap_err();
            assert_eq!(error.current_context(), &ErrorKind::Conflict);
            witness.assert_live(&target.snapshot());
            assert_eq!(mapping.as_bytes(), b"23456");
        }
        drop(mapping);
        target.snapshot().assert_empty_at_baseline(&baseline);
    }
    assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");
    println!("MMR-A06-errors normal_release_baseline={baseline:?} final={:?}", target.snapshot());
}

/// MMR-A06：进程容量显式为一，真实目录作业已经获准排队占满容量；元信息
/// 已完成的两权限建图由生产设施精确拒绝，恢复文件/范围且没有 OS 视图残留。
#[test]
fn test_mapping_capacity_rejection_returns_file_and_leaves_no_native_resources() {
    use std::future::Future;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll, Waker};

    let test_name = "test_mapping_capacity_rejection_returns_file_and_leaves_no_native_resources";
    if !enter_isolated_scenario(test_name) { return; }
    pi_async_fs::set_local_blocking_capacity(std::num::NonZeroUsize::new(1).unwrap()).unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("capacity-source");
    std::fs::write(&path, b"0123456789").unwrap();
    let namespace = LocalFileNamespace::new();
    let mapper = block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap();
    let target = platform::Target::new(&path);
    for writable in [false, true] {
        for _ in 0..8 { drop(map_with_mode(&mapper, 2, 7, writable, false).unwrap()); }
        let baseline = target.snapshot();
        let (release_metadata, metadata_worker) = occupy_only_worker();
        let wake = std::sync::Arc::new(ObservedWake(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(wake.clone());
        let mut context = Context::from_waker(&waker);
        let mut creation = Box::pin(async {
            let range = MmapRange::new(2, 7).unwrap();
            // SAFETY: 独占夹具，真实容量/执行池调节不修改文件、身份或租约内容。
            if writable {
                unsafe { mapper.map_read_write_uncoordinated(range).await }.map(TestMapping::ReadWrite)
            } else {
                unsafe { mapper.map_read_only_uncoordinated(range).await }.map(TestMapping::ReadOnly)
            }
        });
        assert!(matches!(creation.as_mut().poll(&mut context), Poll::Pending));
        release_metadata.send(()).unwrap();
        block_on(metadata_worker);
        let deadline = Instant::now() + Duration::from_secs(10);
        while !wake.0.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "真实元信息查询必须完成");
            std::thread::sleep(Duration::from_millis(1));
        }

        let (release_work, occupied) = occupy_only_worker();
        let mut directory = Box::pin(namespace.read_dir(temporary.path().to_path_buf()));
        assert!(block_on(future::poll_once(directory.as_mut())).is_none(), "真实目录任务必须获准排队");
        let error = block_on(creation).expect_err("容量已经由真实生产作业占满");
        assert_eq!(error.current_context(), &ErrorKind::ResourceExhausted);
        let rejected = target.snapshot();
        rejected.assert_empty_at_baseline(&baseline);
        release_work.send(()).unwrap();
        block_on(occupied);
        drop(block_on(directory).expect("恢复 worker 后真实目录作业必须成功"));
        let replacement = map_with_mode(&mapper, 2, 7, writable, false)
            .expect("容量拒绝必须已经恢复文件槽位和范围");
        assert_eq!(replacement.as_bytes(), b"23456");
        drop(replacement);
        target.snapshot().assert_empty_at_baseline(&baseline);
        println!("MMR-A06-capacity writable={writable} before={baseline:?} rejected={rejected:?}");
    }
}

/// MMR-A07/A08：普通和合作式两个域、两权限，共四个组合各 64 次真实建立/
/// 触达/释放；每次原生完整区间已消失且目标 FD/进程附带资源严格回预热基线。
/// 协调域正常 Drop 后相同范围重新取得，不清空/伪造协议或调用 OS 解映射。
#[test]
fn test_repeated_mmap_drop_returns_native_and_coordination_resources_without_accumulation() {
    let test_name = "test_repeated_mmap_drop_returns_native_and_coordination_resources_without_accumulation";
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let namespace = LocalFileNamespace::new();
    for coordinated in [false, true] {
        let path = temporary.path().join(if coordinated { "repeat-coordinated" } else { "repeat-ordinary" });
        std::fs::write(&path, b"0123456789").unwrap();
        // 独立目标身份在授权建立前获取并关闭观察句柄，不旁路已建立协调域。
        let target = platform::Target::new(&path);
        let authority = if coordinated {
            // SAFETY: 夹具独占；此后产品访问全部通过同一协议的授权资源。
            Some(unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap())
        } else { None };
        let file = match &authority {
            Some(authority) => block_on(namespace.open_with_cross_process_authority(authority, FileAccessMode::ReadWriteMmap)).unwrap(),
            None => block_on(namespace.open(&path, FileAccessMode::ReadWriteMmap)).unwrap(),
        };
        for writable in [false, true] {
            for _ in 0..8 { drop(map_with_mode(&file, 2, 7, writable, coordinated).unwrap()); }
            let baseline = target.snapshot();
            assert!(baseline.views.is_empty());
            for iteration in 0..64 {
                let mut mapping = map_with_mode(&file, 2, 7, writable, coordinated).unwrap();
                assert_eq!(mapping.as_bytes(), b"23456");
                if let TestMapping::ReadWrite(mapping) = &mut mapping {
                    mapping.as_bytes_mut()[0] = b'W';
                    assert_eq!(mapping.as_bytes()[0], b'W');
                    mapping.as_bytes_mut()[0] = b'2';
                    block_on(mapping.flush()).unwrap();
                }
                let witness = ViewWitness::new(mapping.as_bytes(), 2);
                let live = target.snapshot();
                witness.assert_live(&live);
                drop(mapping);
                let released = target.snapshot();
                witness.assert_released(&released);
                released.assert_empty_at_baseline(&baseline);
                if iteration == 0 || iteration == 63 {
                    println!("MMR-A08 coordinated={coordinated} writable={writable} iteration={iteration} before={baseline:?} live={live:?} after={released:?}");
                }
            }
            println!("MMR-A08-summary coordinated={coordinated} writable={writable} exact_iterations=64 final={:?}", target.snapshot());
        }
    }
}

/// MMR-A07：真实对端进程持有映射时，同范围明确冲突；对端正常 Drop 后，
/// 它的文件资源和进程仍存活，OS 视图/句柄已归还，当前进程即可取得同一范围。
/// 两权限都执行此闭环；只有完成上述证据后才让对端进程退出。
#[test]
fn test_coordinated_mmap_normal_drop_returns_range_before_peer_process_exits() {
    let test_name = "test_coordinated_mmap_normal_drop_returns_range_before_peer_process_exits";
    if let Ok(permission) = std::env::var(PEER_PERMISSION) {
        assert!(matches!(permission.as_str(), "read" | "write"));
        run_coordinated_release_peer(permission == "write");
        return;
    }
    if !enter_isolated_scenario(test_name) { return; }
    let temporary = tempfile::tempdir().unwrap();
    let namespace = LocalFileNamespace::new();
    for writable in [false, true] {
        let path = temporary.path().join(if writable { "peer-write" } else { "peer-read" });
        std::fs::write(&path, b"0123456789").unwrap();
        let target = platform::Target::new(&path);
        let mut peer = PeerProcess::spawn(test_name, &path, writable);
        peer.expect_marker("MMR-PEER-OBSERVATION-READY");
        // SAFETY: 独占夹具，两进程原生观察句柄均已关闭；此后只使用相同合作协议。
        let authority = unsafe { block_on(namespace.establish_cross_process_authority(&path)) }.unwrap();
        let file = block_on(namespace.open_with_cross_process_authority(&authority, FileAccessMode::ReadWriteMmap)).unwrap();
        for _ in 0..8 { drop(map_with_mode(&file, 2, 7, writable, true).unwrap()); }
        let baseline = target.snapshot();
        peer.send("map");
        peer.expect_marker("MMR-PEER-MAPPING-HELD");
        let error = map_with_mode(&file, 2, 7, writable, true)
            .expect_err("另一个真实进程的活动范围必须精确冲突");
        assert_eq!(error.current_context(), &ErrorKind::Conflict);
        assert!(peer.child.process.try_wait().unwrap().is_none());
        peer.send("drop");
        peer.expect_marker("MMR-PEER-NORMAL-DROP-COMPLETE");
        assert!(peer.child.process.try_wait().unwrap().is_none(), "正常 Drop 证据必须发生在进程退出前");
        let replacement = map_with_mode(&file, 2, 7, writable, true)
            .expect("对端仍存活时范围必须已正常归还");
        assert_eq!(replacement.as_bytes(), if writable { &b"ABCDE"[..] } else { &b"23456"[..] });
        let witness = ViewWitness::new(replacement.as_bytes(), 2);
        witness.assert_live(&target.snapshot());
        drop(replacement);
        let released = target.snapshot();
        witness.assert_released(&released);
        released.assert_empty_at_baseline(&baseline);
        println!("MMR-A07 peer_still_alive=true writable={writable} parent_before={baseline:?} parent_after={released:?}");
        peer.finish();
    }
}
