// 常用写入缓冲区的真实按需脱离实现。
//
// 本模块使常用标准库与 `bytes` 载体可以可靠、安全、高效地实现
// [`DetachableWriteBuffer`]。它不执行文件 I/O，也不包含 namespace、watch
// 或具体后端行为。
//
// 实现只使用三种透明策略：
//
// - owned allocation 或句柄直接移动，零字节复制；
// - `Arc<[u8]>` 与 `Bytes` 的共享引用执行 O(1) 句柄克隆；
// - 其它借用视图先做可失败容量申请，再复制到独立 `Vec<u8>`。

use std::borrow::Cow;
use std::collections::TryReserveError;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use pi_result::error_stack::Report;
use pi_result::{ErrorKind, RawResult};

use crate::transfer::{BufferFailure, TransferProgress};
use crate::write_buffer::DetachableWriteBuffer;

// 内置借用缓冲区无法为脱离副本取得所需内存。
//
// 该叶子错误保留分配请求和标准库根因，随后由 [`allocation_failure`] 收敛
// 为公共 [`ErrorKind::ResourceExhausted`]。它不包含或格式化原始文件内容。
#[derive(Debug, pi_result::thiserror::Error)]
enum WriteBufferDetachError {
    // 为完整连续副本申请容量失败。
    #[error("failed to reserve {requested_bytes} bytes for a detached write buffer")]
    Allocation {
        // 本次尝试复制的权威视图长度。
        requested_bytes: usize,
        // 标准库返回的容量或分配根因。
        #[source]
        source: TryReserveError,
    },
}

// 为借用写入源创建一个完整的可失败 `Vec<u8>` 副本。
//
// 先使用 `try_reserve_exact` 取得至少 `bytes.len()` 的容量，成功后才复制。
// 因而可预期的容量溢出或分配失败能够作为值返回，而不是经由 `to_vec()` 的
// 不可恢复分配路径表达。分配器允许实际提供更大容量，但有效长度严格等于
// 输入长度；空输入不申请容量。
fn try_copy_bytes(bytes: &[u8]) -> RawResult<Vec<u8>, TryReserveError> {
    let mut detached = Vec::new();
    detached.try_reserve_exact(bytes.len())?;
    detached.extend_from_slice(bytes);
    Ok(detached)
}

// 把脱离分配失败与仍然完整的原始缓冲区组合成公共恢复载体。
//
// 此路径发生在任何文件 I/O 之前，所以进度必然为精确零。局部 thiserror
// frame 保存根因，外层 context 使用稳定的公共资源耗尽分类。
fn allocation_failure<B>(
    buffer: B,
    requested_bytes: usize,
    source: TryReserveError,
) -> BufferFailure<B> {
    let error = Report::new(WriteBufferDetachError::Allocation {
        requested_bytes,
        source,
    })
    .change_context(ErrorKind::ResourceExhausted);

    BufferFailure {
        error,
        buffer,
        progress: TransferProgress::Exact { bytes: 0 },
    }
}

// 复制一个共享借用的连续视图，同时把原引用保存为恢复令牌。
//
// `original` 与 `bytes` 必须描述同一个写入源；各 impl 在调用点显式建立该
// 关系。成功时返回独立 `Vec`，失败时返还同一个原引用且不产生文件副作用。
fn detach_copied<'a, T>(
    original: &'a T,
    bytes: &[u8],
) -> RawResult<(Vec<u8>, &'a T), BufferFailure<&'a T>>
where
    T: ?Sized,
{
    match try_copy_bytes(bytes) {
        Ok(detached) => Ok((detached, original)),
        Err(source) => Err(allocation_failure(original, bytes.len(), source)),
    }
}

// `Vec<u8>` 直接移动原 allocation；脱离与错误恢复都不复制字节。
impl DetachableWriteBuffer for Vec<u8> {
    type Detached = Vec<u8>;
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

// `Box<[u8]>` 直接移动原 Box；脱离与错误恢复都不复制字节。
impl DetachableWriteBuffer for Box<[u8]> {
    type Detached = Box<[u8]>;
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

// owned `Arc<[u8]>` 直接移动原句柄，不增加引用计数且不复制字节。
impl DetachableWriteBuffer for Arc<[u8]> {
    type Detached = Arc<[u8]>;
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

// owned `Cow<[u8]>` 按当前变体选择移动或受检复制，并在错误时恢复原变体。
//
// `Owned(Vec)` 直接成为脱离载体，`Recovery = None`；`Borrowed(&[u8])` 复制
// 到独立 `Vec`，并用 `Some(original)` 保存原引用。普通错误恢复不会把
// Borrowed 输入悄悄升级为内容相等的 Owned 值。
impl<'a> DetachableWriteBuffer for Cow<'a, [u8]> {
    type Detached = Vec<u8>;
    type Recovery = Option<&'a [u8]>;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        match self {
            Cow::Owned(buffer) => Ok((buffer, None)),
            Cow::Borrowed(buffer) => match try_copy_bytes(buffer) {
                Ok(detached) => Ok((detached, Some(buffer))),
                Err(source) => Err(allocation_failure(
                    Cow::Borrowed(buffer),
                    buffer.len(),
                    source,
                )),
            },
        }
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        match recovery {
            Some(original) => {
                drop(detached);
                Cow::Borrowed(original)
            }
            None => Cow::Owned(detached),
        }
    }
}

// owned [`Bytes`] 直接移动原共享存储句柄，不增加引用计数或复制字节。
impl DetachableWriteBuffer for Bytes {
    type Detached = Bytes;
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

// owned [`BytesMut`] 直接移动原 allocation；写入源合同不会修改其中字节。
impl DetachableWriteBuffer for BytesMut {
    type Detached = BytesMut;
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

// 借用切片通过受检分配复制到 `Vec`；原切片引用作为恢复令牌保留。
impl<'a> DetachableWriteBuffer for &'a [u8] {
    type Detached = Vec<u8>;
    type Recovery = &'a [u8];

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        detach_copied(self, self)
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 `Vec<u8>` 复制其当前完整逻辑内容，并保留同一个 `&Vec` 供错误恢复。
impl<'a> DetachableWriteBuffer for &'a Vec<u8> {
    type Detached = Vec<u8>;
    type Recovery = &'a Vec<u8>;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        detach_copied(self, self.as_slice())
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 `Box<[u8]>` 复制其完整切片，并保留同一个 `&Box` 供错误恢复。
#[allow(clippy::borrowed_box)]
impl<'a> DetachableWriteBuffer for &'a Box<[u8]> {
    type Detached = Vec<u8>;
    type Recovery = &'a Box<[u8]>;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        detach_copied(self, self.as_ref())
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 `Arc<[u8]>` 通过一次 O(1) 引用计数克隆共享存储，不复制字节。
//
// 恢复时丢弃临时 Arc，使强引用计数回到脱离前的状态，再返还同一个原引用。
impl<'a> DetachableWriteBuffer for &'a Arc<[u8]> {
    type Detached = Arc<[u8]>;
    type Recovery = &'a Arc<[u8]>;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        Ok((Arc::clone(self), self))
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 `Cow` 无法移动其 Owned 分支，因此统一复制当前权威视图并保留原引用。
impl<'a, 'b> DetachableWriteBuffer for &'a Cow<'b, [u8]> {
    type Detached = Vec<u8>;
    type Recovery = &'a Cow<'b, [u8]>;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        detach_copied(self, self.as_ref())
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 [`Bytes`] 通过 O(1) 句柄克隆共享同一当前视图，不复制字节。
//
// 恢复时丢弃临时句柄以恢复引用计数，并返还同一个原引用。
impl<'a> DetachableWriteBuffer for &'a Bytes {
    type Detached = Bytes;
    type Recovery = &'a Bytes;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        Ok((Bytes::clone(self), self))
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}

// 借用 [`BytesMut`] 不能在不消费原值的情况下共享其可变 allocation，因此
// 通过受检分配复制当前已初始化视图，并保留同一个原引用供错误恢复。
impl<'a> DetachableWriteBuffer for &'a BytesMut {
    type Detached = Vec<u8>;
    type Recovery = &'a BytesMut;

    fn try_detach(self) -> RawResult<(Self::Detached, Self::Recovery), BufferFailure<Self>> {
        detach_copied(self, self.as_ref())
    }

    fn recover_from_detached(
        detached: Self::Detached,
        recovery: Self::Recovery,
    ) -> Self {
        drop(detached);
        recovery
    }
}
