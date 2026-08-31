# pi_async_fs

`pi_async_fs` 是一个面向本地文件系统与远端对象存储的、异步运行时无关的
Rust 文件 API 合同库。它把目录命名空间（namespace）、已打开文件 I/O、
缓冲区所有权、内存映射（MMAP）和进程内/跨进程协调的语义放在统一的公开
类型系统中，同时不把具体异步运行时、后端或操作系统资源类型暴露给调用方。

## 当前状态：第一批次 API 声明

第一批次已经冻结并声明公共 API、关联类型、泛型约束和所需辅助类型，但尚未
提供本地或远端适配实现。除项目设计者明确批准的常用
`DetachableWriteBuffer` 实现外，函数、方法、格式化、比较和资源析构等函数体
仍使用 `todo!()` 占位；当前版本用于编译期接口评审，调用占位接口会 panic，
不能用于真实文件操作。

当前声明使用关联类型默认值，因此需要 nightly Rust；项目不在 `Cargo.toml`
固定 `rust-version`。相应语言特性稳定后可以在不改变合同语义的前提下支持
stable Rust。

开发分为三个批次：

1. 声明并审查公共 trait、辅助类型和完整公开合同；
2. 实现并验收本地文件系统适配层，包括 Windows、Linux 与 Linux `tmpfs`；
3. 在同一合同下实现并验收远端文件能力，不把对象存储伪装成完整 POSIX
   文件系统。

## 公共能力边界

- `FileNamespace` 负责定位符、元信息、可用空间、目录枚举与递归遍历、创建、
  安全/非协调删除、严格不替换改名、原子 `copy_new`、文件打开和跨进程授权。
- `FileIo` 只操作已经打开且绑定到稳定文件对象的不可克隆资源，提供随机读取、
  顺序读取、增长读取、严格尾部追加、全文件覆盖、缩短型截断、普通文件刷新
  和只读/读写 MMAP。首版不提供顺序写或任意偏移普通写。
- `DetachableWriteBuffer` 描述写入源在后端确有需要时如何脱离调用方生命周期，
  并保证普通写入错误可以恢复调用时的同一个原始缓冲区。内置实现覆盖
  `Vec<u8>`、`Box<[u8]>`、`Arc<[u8]>`、`Cow<[u8]>`、`bytes::Bytes`、
  `bytes::BytesMut` 及常用只读借用。
- `GrowableReadBuffer` 把“保留已有内容并在逻辑尾部增长”的语义与普通
  `bytes::BufMut` 区分开；固定读取则使用 `AsMut<[u8]>` 和
  `ReadTargetRegion`。
- `ReadMmapHandle` 是可低成本克隆且可共享的只读映射凭证；
  `ReadWriteMmapHandle` 可跨线程移动，但不可克隆且不实现 `Sync`。所有映射
  内容只能通过透明句柄访问。

所有异步方法返回标准 `Future + Send + 'a` 或借用等价形式，不绑定具体异步
运行时。它允许 Future 在有效借用期内跨工作线程迁移，但不会自动把借用变成
`'static`；要求顶层任务为 `Send + 'static` 的运行时应让 `async move` 任务先
拥有文件资源和拥有所有权的缓冲区，再在任务内部调用借用式 API。

## 概念性使用示例

以下示例展示冻结后的调用形状。第一批次没有具体文件系统实现，故使用
`ignore`，不能
在当前版本执行。

```rust,ignore
use pi_async_fs::{CopyTargetEvidence, FileNamespace};

async fn copy_without_replacing<N>(
    namespace: &N,
    source: &N::Locator,
    destination: &N::Locator,
) -> pi_result::Result<u64>
where
    N: FileNamespace,
{
    match namespace.copy_new(source, destination).await {
        Ok(outcome) => Ok(outcome.content_byte_len()),
        Err(failure) => {
            // PublicationUnknown 表示目标可能已经完整发布，不能盲目重试。
            if matches!(
                failure.target_evidence(),
                CopyTargetEvidence::PublicationUnknown { .. }
            ) {
                // 业务恢复流程应重新查询并核验目标。
            }
            Err(failure.into_parts().0)
        }
    }
}
```

```rust,ignore
use pi_async_fs::{FileIo, ReadTargetRegion};

async fn read_header<F: FileIo>(file: &F) -> pi_result::Result<[u8; 16]> {
    let mut header = [0_u8; 16];
    let target = ReadTargetRegion::try_from(0..header.len()).unwrap();
    file.read_exact_at(0, &mut header, target).await?;
    Ok(header)
}
```

严格追加和全文件覆盖都接收实现 `DetachableWriteBuffer` 的拥有型值或借用
值；成功不返还缓冲区。严格追加普通失败通过 `BufferFailure<B>` 返还原始
`B`，全文件覆盖普通失败通过 `OverwriteFailure<B>` 返还原始 `B` 和目标状态
证据。读取失败只返回统一错误，不返还读取目标。

## 并发、安全与持久性要点

- 同一进程内，不同路径或独立句柄只要表示同一底层文件，就必须服从同一组
  并发规则；不能用定位符文本是否相等代替对象身份判断。
- 任意两个 MMAP 的公开逻辑范围采用半开区间 `[start, end)`，只要相交就禁止
  同时建立。活动映射与普通读取、覆盖、截断、普通文件刷新和安全删除互斥；
  严格尾部追加是唯一可与既有映射长期并存的普通内容 I/O。
- 默认 `rename` 不与内容操作建立额外互斥。平台允许时，既有句柄和映射继续
  绑定原文件对象；平台或文件系统拒绝占用中的改名时，直接返回明确错误。
- 跨进程协调是合作式协议。只有显式建立并绑定
  `CrossProcessFileAuthority` 的资源获得该合同；仅设置协调根目录不会让普通
  操作自动获得跨进程安全语义。
- MMAP 的未协调创建入口是 `unsafe`，因为库无法约束其它进程和旁路原生句柄；
  `unsafe` 不会关闭进程内身份、范围和冲突检查。
- 普通文件 `flush` 与读写映射句柄的 `flush` 是不同屏障。成功也不承诺掉电、
  操作系统崩溃、失信控制器、介质损坏或硬件故障后的绝对持久性。
- `copy_new` 只复制主逻辑内容，并保证最终目标只会以完整内容出现且不会替换
  既有对象；它不复制 ACL、时间、扩展属性、备用数据流或后端专有元信息，
  也不提供跨进程协调式重载。

## 使用禁忌与已知限制

- 当前第一批次不得调用任何 `todo!()` 占位接口，也不得把“可以编译”理解为
  已有可用文件系统实现。
- 不要用“先 `try_exists`、再创建/改名”的检查后操作模拟原子不替换语义。
- 不要把同文件的活动映射或其派生切片作为该文件追加/覆盖的输入；实现必须在
  文件副作用前明确拒绝。
- 不要在 `CopyTargetEvidence::PublicationUnknown`、提交状态 `Unknown` 或
  `TransferProgress::AtLeast` 后盲目重试。
- 不要从 `Drop` 推导刷新成功；MMAP 句柄析构只负责释放映射相关资源。
- 本批次不提供 `FileWatch`、`remove_dir_all`、`replace`、顺序写和任意偏移写。
  `remove_dir` 只删除空目录。
- 普通资源只保证同一进程、共享同一库协调状态的操作互斥；其它进程、另一份
  不共享协调状态的库实例和绕过本库的原生句柄位于保证边界之外。
- 跨线程构建必须保证最终依赖图没有为 `pi_atom` 使用的同一份 `pi_share`
  启用 `rc` 特性；该组合不在本库支持范围内，可能使安全 Rust 中的并发
  `Atom` 克隆或释放形成数据竞争。依赖、特性或锁定版本变化后必须重新审计
  这一条件。

## 第一批次验证

第一批次只验证声明、文档和 lint，不编写或执行功能测试：

```text
cargo check
cargo clippy --all-targets --all-features
cargo doc --no-deps
```

第二批次开始后将进入连续的 TDD、实现、审查与真实本地文件系统集成测试循环；
专项测试、压力测试和基准将在相应实现闭环后独立补齐并验收。

## 许可证

MIT
