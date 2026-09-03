// 文件内存映射的透明拥有型句柄。
//
// 本模块只公开两种能力不同的资源外壳：[`ReadMmapHandle`] 共享一个不可变
// 映射实例，支持低成本克隆和并发读取；[`ReadWriteMmapHandle`] 独占一个
// 可写映射实例，既不可克隆也不可通过共享引用跨线程并发访问。两者都隐藏
// 底层 `memmap2` 对象、页对齐扩大范围、原始地址、文件句柄和进程内协调
// 租约。
//
// 同一受管文件的既有映射访问可以与严格尾部追加并存：映射只能访问建立时
// 冻结的逻辑范围，追加只能从调用时真实文件尾开始，二者不构成重叠文件
// 字节或统一事务。为了阻止映射内存被反向用作同文件的普通写入源，内部
// 范围租约还必须登记精确公开视图的虚拟地址区间；append 和 overwrite adapter
// 在副作用前检查实际后端 buffer 地址，并拒绝与目标文件任一活动映射视图
// 相交的输入。
// 该检查也覆盖从 [`ReadMmapHandle::as_bytes`] 派生的普通切片和子切片，不能
// 只依赖公开 buffer 的 Rust 类型。
//
// 全文件覆盖与目标文件任何活动映射互斥，因而同文件的只读映射句柄或
// 其派生视图不能作为覆盖源。不同文件的 [`ReadMmapHandle`] 可以作为追加或
// 全文件覆盖源；这一能力只避免为脱离语义额外复制整个映射，不承诺底层
// 文件路径是完全的操作系统级零拷贝传输。
//
// 映射与追加并存不等于两条操作系统访问路径具有内容快照、跨路径可见性、
// 原子提交或持久化顺序。已有映射永远不会因追加自动扩大；需要读取新增
// 范围时必须在 append 完成后另行建立新的不相交映射。网络文件、库外句柄
// 和其它进程仍位于当前协调保证之外。
//
// 映射生命周期不提供显式 `close()`：句柄离开作用域后由字段析构解除映射
// 并释放租约；需要观察失败的持久化动作必须在 Drop 前显式完成。公开句柄
// 只暴露已经冻结的范围、字节视图和刷新能力。

use core::cell::Cell;
use core::fmt;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::{Context, Poll};
use std::sync::Arc;

use pi_result::{InteropResultExt, RawResult};

use crate::bounded_blocking;
use crate::{BufferFailure, DetachableWriteBuffer, MmapRange};

// 公开刷新 Future 被取消后，同步刷新仍可能已经在线程池中执行。该包装在
// 正常路径转发结果，在未完成即析构时只分离内部任务，使任务继续持有映射
// 核心，直到底层不再访问虚拟地址。
struct MmapDetachOnDrop<T> {
    task: Option<async_global_executor::Task<T>>,
}

impl<T> MmapDetachOnDrop<T> {
    fn new(task: async_global_executor::Task<T>) -> Self {
        Self { task: Some(task) }
    }
}

impl<T> Unpin for MmapDetachOnDrop<T> {}

impl<T> Future for MmapDetachOnDrop<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        let result = Pin::new(
            this.task.as_mut().expect("mmap task must exist while polling"),
        )
        .poll(context);
        if result.is_ready() {
            this.task.take();
        }
        result
    }
}

impl<T> Drop for MmapDetachOnDrop<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

// 映射状态的最后一个拥有者释放时执行一次协调登记清理。该字段必须排在
// 原生映射字段之后，使 Rust 的字段析构顺序先解除虚拟地址映射，再开放
// 相同文件范围给后续操作。
struct MappingCleanup {
    release: Option<Box<dyn FnOnce() + Send + Sync + 'static>>,
}

impl MappingCleanup {
    fn new(release: Box<dyn FnOnce() + Send + Sync + 'static>) -> Self {
        Self {
            release: Some(release),
        }
    }
}

impl Drop for MappingCleanup {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

// 一个只读逻辑映射实例的共享内部所有权。
//
// 这里共同持有底层只读映射、范围租约清理动作及平台资源。
// 把租约与映射放进同一个引用计数状态，可以确保只读句柄的所有克隆都只
// 指向一个映射和一项协调登记。
struct ReadMmapState {
    range: MmapRange,
    mapping: memmap2::Mmap,
    cleanup: MappingCleanup,
}

// 可并发共享的只读文件内存映射句柄。
//
// # 克隆与“单一映射”
//
// 克隆本句柄只增加共享状态的原子引用计数，不重新调用操作系统建立映射，
// 不复制映射内容，也不创建第二项范围租约。因此，同一范围仍只有一个逻辑
// 映射实例；多个 Rust 句柄值只是它的共享访问凭证。该操作是 O(1) 且零
// 数据复制，但并非字面上的零 CPU 成本，因为线程安全引用计数需要一次
// 原子操作。
//
// 丢弃任意一个非末尾克隆只释放该凭证。只有最后一个克隆被丢弃时，内部
// 最后一个共享内部状态才解除底层虚拟地址映射、释放平台资源，并在最后释放
// 范围租约。操作系统页缓存和脏页的物理回收时机不属于该保证。
//
// # 线程能力
//
// 本类型通过 `Arc` 及内部只读状态自动获得 `Send + Sync`，不使用手写
// `unsafe impl`。调用方可以把不同克隆发送到不同线程并发读取同一映射，
// 但这不会放宽跨进程外部修改、截断或替换文件的安全边界。
//
// # 不透明边界
//
// 本类型没有公开构造器，也不实现 `Default`、`Copy`、判等、排序或哈希。
// 资源句柄代表活动生命周期而不是稳定值；若需要诊断，应使用其后续明确
// 提供的范围和格式化接口，不能依赖内存地址或后端句柄比较身份。
/// 可并发共享的只读文件内存映射句柄。
///
/// 本类型没有公开构造器。它表示一个已经获准的非空逻辑范围，可低成本克隆、
/// `Send + Sync`，但不实现 `Copy`。所有克隆共享同一逻辑映射；最后一个克隆
/// 被丢弃时自动释放相关资源。句柄只提供受生命周期约束的只读字节视图，不
/// 公开裸地址或可与句柄分离的访问能力。
pub struct ReadMmapHandle {
    state: Arc<ReadMmapState>,
}

impl ReadMmapHandle {
    pub(crate) fn from_mapping(
        range: MmapRange,
        mapping: memmap2::Mmap,
        release: Box<dyn FnOnce() + Send + Sync + 'static>,
    ) -> Self {
        debug_assert_eq!(range.len(), mapping.len() as u64);
        Self {
            state: Arc::new(ReadMmapState {
                range,
                mapping,
                cleanup: MappingCleanup::new(release),
            }),
        }
    }

    // 返回本映射实例覆盖的原始文件逻辑范围。
    //
    // 所有克隆返回同一范围的共享借用。该值不包含操作系统页或分配粒度
    // 对齐所产生的隐藏前缀，也不会因映射建立后的合法尾部追加而扩大。
    // 返回引用的生命周期绑定到本次句柄借用，调用方不能从中取得或释放
    // 内部范围租约。
    #[must_use]
    /// 返回本映射覆盖的原始文件逻辑范围。
    ///
    /// 该范围不会因映射建立后的文件尾部追加而扩大。
    pub fn range(&self) -> &MmapRange {
        &self.state.range
    }

    // 返回当前进程可通过本句柄读取的逻辑字节数。
    //
    // 建立映射成功已经证明 [`MmapRange::len`] 可以转换为 `usize` 并由一个
    // Rust 切片表达，因此本方法与 [`Self::as_bytes`] 返回切片的长度始终
    // 相等。公开映射范围非空，所以结果大于零。
    #[must_use]
    /// 返回可通过本句柄读取的逻辑字节数。
    ///
    /// 映射范围保证非空，因此结果始终大于零，并等于 [`Self::as_bytes`] 的
    /// 切片长度。
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.state.mapping.len()
    }

    // 借用完整逻辑映射的零复制只读字节视图。
    //
    // 返回切片从公开 [`Self::range`] 的 `start` 对应字节开始，长度恰好为
    // [`Self::len`]；后端为了页对齐而额外映射的字节绝不进入该视图。切片
    // 生命周期不超过本次 `&self` 借用，因而只要任意视图存活，最后一个
    // 句柄克隆就不能被安全 Rust 丢弃或解除映射。
    //
    // 该方法只建立借用，不复制字节、不分配、不执行系统调用，也不推进任何
    // 文件游标。它不提供内容快照或跨进程一致性保证：其它进程修改、替换或
    // 截断底层文件仍落在公开声明的外部协调边界之外。
    #[must_use]
    /// 借用完整逻辑映射的零复制只读字节视图。
    ///
    /// 返回切片严格对应 [`Self::range`]，其生命周期不超过本次句柄借用。
    /// 本方法不复制字节、不推进游标，也不把映射内容提升为文件快照。
    pub fn as_bytes(&self) -> &[u8] {
        &self.state.mapping
    }
}

impl AsRef<[u8]> for ReadMmapHandle {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

// owned 只读映射句柄可以整体移动为普通写入操作的独立载体。
//
// 该实现沿用已冻结的 [`DetachableWriteBuffer`] 行为：脱离不增加引用计数、不复制映射
// 字节，也不建立新的映射或范围租约；普通错误恢复则把同一个句柄值移回
// 调用方。
impl DetachableWriteBuffer for ReadMmapHandle {
    type Detached = ReadMmapHandle;
    type Recovery = ();

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        Ok((self, ()))
    }

    fn recover_from_detached(
        detached: Self::Detached,
        _recovery: Self::Recovery,
    ) -> Self {
        detached
    }
}

// 借用的只读映射句柄通过一次 `Arc` 引用计数增量保持映射独立存活。
//
// 临时载体与原引用共享同一个逻辑映射及范围租约，不复制字节。普通错误
// 恢复会先丢弃临时句柄，再返还调用时的同一个引用；写入成功则只丢弃临时
// 句柄，调用方持有的原句柄继续有效。
impl<'a> DetachableWriteBuffer for &'a ReadMmapHandle {
    type Detached = ReadMmapHandle;
    type Recovery = &'a ReadMmapHandle;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        let detached = ReadMmapHandle {
            state: Arc::clone(&self.state),
        };

        Ok((detached, self))
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

impl Clone for ReadMmapHandle {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl fmt::Debug for ReadMmapHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadMmapHandle")
            .field("range", self.range())
            .field("len", &self.len())
            .finish()
    }
}

impl fmt::Display for ReadMmapHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "read-only mmap {}", self.range())
    }
}

// 一个可写逻辑映射实例的独占内部所有权。
//
// 这里共同持有底层可写映射、范围租约清理动作及平台资源。字段均必须能够
// 安全移动到其它线程，但不得自行提供共享可变
// 访问。
struct ReadWriteMmapState {
    range: MmapRange,
    mapping: memmap2::MmapRaw,
    cleanup: MappingCleanup,
}

// 可移动但不可共享的读写文件内存映射句柄。
//
// # 单写所有权
//
// 本类型不实现 `Clone`。私有的 `PhantomData<Cell<()>>` 在不占用运行时
// 空间的前提下阻止类型自动获得 `Sync`，因此不能通过 `&ReadWriteMmapHandle`
// 在多个线程中并发使用。`Cell<()>` 本身可以移动，故只要实际映射状态为
// `Send`，整个句柄便自动获得 `Send`，可以把完整所有权转移到另一线程。
// 这里不需要也不允许仅为满足签名而手写 `unsafe impl Send`。
//
// # 释放顺序
//
// 丢弃句柄必须先停止访问并解除完整底层虚拟地址映射，再释放平台映射和
// 文件资源，最后释放范围租约。`Drop` 没有错误通道且不负责持久化刷新；
// 需要观察刷新失败的调用方必须使用后续单独冻结的显式接口。
//
// # 不透明边界
//
// 本类型没有公开构造器，也不实现 `Default`、`Copy`、判等、排序或哈希。
// 它不会公开底层映射对象、裸指针、可分离 guard，或绕开范围租约的
// `DerefMut`/`AsMut` 接口。
/// 可移动但不可共享的读写文件内存映射句柄。
///
/// 本类型没有公开构造器，不实现 `Clone`、`Copy` 或 `Sync`，但可以整体跨线程
/// 移动。它表示一个已经获准的非空逻辑范围；所有读取、修改和刷新都必须通过
/// 本句柄。丢弃句柄会自动释放映射资源，但不会隐式刷新未持久化修改。
pub struct ReadWriteMmapHandle {
    state: Arc<ReadWriteMmapState>,
    not_sync: PhantomData<Cell<()>>,
}

impl ReadWriteMmapHandle {
    pub(crate) fn from_mapping(
        range: MmapRange,
        mapping: memmap2::MmapRaw,
        release: Box<dyn FnOnce() + Send + Sync + 'static>,
    ) -> Self {
        debug_assert_eq!(range.len(), mapping.len() as u64);
        Self {
            state: Arc::new(ReadWriteMmapState {
                range,
                mapping,
                cleanup: MappingCleanup::new(release),
            }),
            not_sync: PhantomData,
        }
    }

    // 返回本映射实例覆盖的原始文件逻辑范围。
    //
    // 该范围不包含后端为了满足页或分配粒度对齐而映射的隐藏字节，也不会
    // 因映射建立后的合法尾部追加而扩大。返回引用不能脱离句柄生命周期，
    // 也不授予调用方直接操作内部范围租约的能力。
    #[must_use]
    /// 返回本映射覆盖的原始文件逻辑范围。
    ///
    /// 该范围不会因映射建立后的文件尾部追加而扩大。
    pub fn range(&self) -> &MmapRange {
        &self.state.range
    }

    // 返回当前进程可通过本句柄访问的逻辑字节数。
    //
    // 建立映射成功已经证明 [`MmapRange::len`] 能转换为 `usize` 并由单个
    // Rust 切片表达；结果始终大于零，并分别等于 [`Self::as_bytes`] 与
    // [`Self::as_bytes_mut`] 返回视图的长度。
    #[must_use]
    /// 返回可通过本句柄访问的逻辑字节数。
    ///
    /// 结果始终大于零，并等于只读和可写完整视图的切片长度。
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.state.mapping.len()
    }

    // 借用完整逻辑映射的零复制只读字节视图。
    //
    // 返回切片严格受本次共享借用约束，不复制、不分配且不推进文件游标。
    // 本类型刻意不实现 `AsRef<[u8]>` 或 [`DetachableWriteBuffer`]，因此这项
    // 显式读取能力不会使可写映射句柄进入普通写入源的泛型能力集合。
    #[must_use]
    /// 借用完整逻辑映射的零复制只读字节视图。
    ///
    /// 返回切片严格对应 [`Self::range`]，且不使本句柄成为普通写入源。
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: 建图时已验证非空长度和文件快照，映射核心在返回借用期间由
        // 句柄 Arc 保持存活；公开协调合同阻止进程内截断和相交访问。共享
        // 借用不能与同一句柄的可写借用同时存在。
        unsafe {
            core::slice::from_raw_parts(
                self.state.mapping.as_ptr(),
                self.state.mapping.len(),
            )
        }
    }

    // 独占借用完整逻辑映射的零复制可写字节视图。
    //
    // 视图起点与长度严格对应 [`Self::range`]，绝不包含后端页对齐产生的
    // 隐藏范围。独占借用使同一句柄在视图存活期间不能再次读取、写入、刷新
    // 或被丢弃；返回引用也不能比本次 `&mut self` 借用活得更久。
    //
    // 修改切片可能把对应映射页标记为脏页，但本方法本身不刷新、不承诺持久
    // 化，也不建立事务或回滚点。不同进程对底层文件的写入、替换或截断仍
    // 位于本库协调边界之外。
    #[must_use]
    /// 独占借用完整逻辑映射的零复制可写字节视图。
    ///
    /// 返回切片严格对应 [`Self::range`]。修改会改变映射内容，但本方法本身
    /// 不刷新、不建立事务，也不提供回滚保证。
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: 读写句柄不可克隆且不实现 Sync，本方法又要求独占借用，
        // 因而公开安全接口同时最多产生一个可写切片。内部 Arc 克隆只允许
        // 调用 MmapRaw 的同步刷新，不构造 Rust 字节引用；刷新与操作系统对
        // 同一映射的写回可以安全重叠，但取消后不建立内容快照或顺序保证。
        unsafe {
            core::slice::from_raw_parts_mut(
                self.state.mapping.as_mut_ptr(),
                self.state.mapping.len(),
            )
        }
    }

    // 同步完成地刷新本句柄整个公开逻辑映射范围内的脏页。
    //
    // 本方法只属于可写映射句柄。只读映射不能通过安全接口产生脏页，因此
    // [`ReadMmapHandle`] 不提供含义模糊的同名方法。刷新目标固定为
    // [`Self::range`] 表达的完整逻辑范围；调用方不需要再次传入文件范围或
    // 句柄内偏移。本版不另设局部刷新接口，避免把文件坐标、映射内坐标和
    // 后端页对齐坐标混成一个参数。
    //
    // # 同步完成语义
    //
    // 公开接口保持运行时无关的异步形式，但内置本地 adapter 必须在线程池
    // 执行 `memmap2` 的同步完成型刷新。实现应把公开逻辑范围换算成底层对齐
    // 映射内的相对区间，并使用同步 `flush_range` 或能够证明等价的同步路径；
    // 不能把只发起后台写回便返回的异步提示冒充完成。
    //
    // 只有底层同步刷新调用成功返回后，本 Future 才能返回 `Ok(())`。成功
    // 表示操作系统已经按当前平台的映射同步合同处理该逻辑范围内的脏页；
    // 它不扩大映射范围，也不使映射后的合法尾部追加进入本句柄的可访问
    // 视图。页粒度向外取整、共享页和平台文件缓冲屏障可能顺带处理范围外的
    // 脏数据，因此本接口承诺“至少覆盖完整逻辑范围”，不承诺只影响这些
    // 字节。
    //
    // 成功不提供掉电、操作系统崩溃、控制器失信、介质或硬件损坏后的绝对
    // 存续保证，也不代替目录同步、文件命名空间提交或其它文件资源的显式
    // 持久性协议。用户进程在成功返回后崩溃时，本次调用不再依赖句柄随后
    // 执行隐式刷新；系统级失败仍属于前述明确排除范围。
    //
    // # 错误与部分效果
    //
    // 所有可报告失败通过 [`pi_result::Error`] 返回，不得 panic。错误不撤销
    // 调用前对映射字节的修改，句柄及其逻辑视图仍保持有效；操作系统也可能
    // 已经写回部分页，但本接口不能报告精确的已刷新字节范围。调用方可以在
    // 修复错误原因后再次刷新，却不能把失败解释成“磁盘内容一定完全未变”。
    //
    // # 独占借用、并发与追加
    //
    // `&mut self` 保证同一句柄不能同时产生可写视图、执行其它句柄操作或
    // 发起多个刷新。不同文件或同一文件中已经获准的不相交映射拥有各自的
    // 句柄，可以在进程内协调规则允许时独立刷新；操作系统仍可能在页缓存、
    // 文件缓冲或设备层串行这些请求，本接口不承诺物理并行。
    //
    // 本次刷新可以与同文件严格尾部追加并存：映射只覆盖建图时冻结的旧
    // 范围，追加只从当时文件尾开始。某些平台的文件级缓冲屏障可能顺带推进
    // 并发追加数据的写回，但调用方不得借此推导追加已经获得本方法的成功、
    // 持久性或顺序保证；追加仍只服从自己的完成合同。
    //
    // # 取消、Drop 与资源寿命
    //
    // 丢弃 Future 表示调用方停止等待，不保证已经撤销底层同步刷新。若任务
    // 尚未提交，实现可以无刷新副作用地取消；一旦同步调用已经排队或开始，
    // 内部完成所有者必须继续持有底层映射、文件资源、映射 guard 和范围
    // 租约，直到刷新真正返回，随后才记录结果并释放操作状态。
    //
    // 调用方取消后没有异步错误交付通道。此时即使公开句柄也被立即丢弃，
    // [`Drop`] 也只能释放公开所有权；在途完成所有者必须延迟最终解映射，
    // 直到系统不再访问映射地址。没有在途操作时，句柄 Drop 按正常顺序解除
    // 映射并释放资源。本类型不提供显式 `close()`，Drop 也绝不隐式启动一次
    // 新刷新，因为析构没有可靠的异步等待和错误报告通道。
    //
    // # 幂等性、副作用与成本
    //
    // 在两次调用之间没有新写入且外部文件状态不变时，重复成功刷新不会再次
    // 改变逻辑文件字节，因此在内容效果上可重复；但每次调用仍可能执行系统
    // 调用、占用阻塞线程、更新内部状态并独立失败，所以它不是纯函数，也不
    // 保证结果幂等或固定延迟。
    //
    // 时间成本通常随需要处理的映射页数和存储设备状态变化，线程池排队、
    // 缺页、写回、文件系统和设备缓存都可能增加尾延迟。方法不分配与映射
    // 长度相等的用户态复制 buffer，也不复制完整文件内容；这不等于零系统
    // 开销、零页缓存活动或内核原生异步 I/O。
    //
    // # 线程与异步运行时
    //
    // 返回 Future 为 `Send`，可以在对本句柄的独占借用期内跨工作线程迁移，
    // 但不要求 `'static`，也不绑定 Tokio、async-std、Monoio 等宿主运行时。
    // 同步刷新由内部阻塞任务池承载，因而不会直接占用调用方执行器的 poll
    // 线程；它仍会占用一个阻塞 worker，并受线程池容量与调度影响。
    /// 等待本句柄完整逻辑范围内的脏页完成同步刷新。
    ///
    /// 成功至少覆盖整个 [`Self::range`]，但可能连带刷新其它共享状态。错误
    /// 不撤销已经修改的字节，且部分页可能已经写回；调用方可以在句柄仍有效
    /// 时重试。取消后已开始的刷新可能继续完成。成功不保证掉电、系统崩溃、
    /// 硬件缓存失信或介质损坏场景，也不代替命名空间持久化。
    pub fn flush(
        &mut self,
    ) -> impl Future<Output = pi_result::Result<()>> + Send + '_ {
        let state = Arc::clone(&self.state);
        async move {
            let task = async_global_executor::spawn(async move {
                bounded_blocking::unblock(move || state.mapping.flush())
                    .await?
                    .into_classified_error()
            });
            MmapDetachOnDrop::new(task).await
        }
    }
}

// 允许读写映射句柄直接作为固定随机读取或固定顺序读取的目标缓冲区。
//
// `as_mut()` 的权威视图与 [`ReadWriteMmapHandle::as_bytes_mut`] 完全相同。
// 固定读取仍必须使用 [`crate::ReadTargetRegion`] 约束本次可覆盖窗口；普通
// 错误或取消前已经提交的前缀可能同时成为目标映射文件的脏页，接口不承诺
// 回滚。该实现不赋予增长能力，也不使本类型成为普通写入源。
impl AsMut<[u8]> for ReadWriteMmapHandle {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

impl fmt::Debug for ReadWriteMmapHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReadWriteMmapHandle")
            .field("range", self.range())
            .field("len", &self.len())
            .finish()
    }
}

impl fmt::Display for ReadWriteMmapHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "read-write mmap {}", self.range())
    }
}
