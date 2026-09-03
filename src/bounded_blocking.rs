// 本地同步系统调用的进程级有界准入设施。
//
// 该设施承载 [`crate::LocalFileNamespace`] 直接提交、并且能够把容量许可交给
// 真实完成者持有的同步工作，例如建图、映射刷新、部分平台身份操作和目录枚举
// 步骤。它不包裹或冒充控制后端自行提交的异步管线。详细作用域与取消边界见
// `docs/BUG_ISSUES_CONTEXT.md#issue-b2h-def-010`。

use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use pi_result::{ClassifyErrorKind, InteropResultExt};

#[derive(Debug, pi_result::thiserror::Error)]
#[error(
    "bounded blocking facility is saturated at {max_in_flight} in-flight operations"
)]
struct BlockingFacilitySaturated {
    max_in_flight: usize,
}

impl ClassifyErrorKind for BlockingFacilitySaturated {
    fn classify_error_kind(&self) -> pi_result::ErrorKind {
        pi_result::ErrorKind::ResourceExhausted
    }
}

#[derive(Debug, pi_result::thiserror::Error)]
#[error(
    "local blocking capacity is already frozen at {configured}; requested {requested}"
)]
struct BlockingCapacityConflict {
    configured: usize,
    requested: usize,
}

impl ClassifyErrorKind for BlockingCapacityConflict {
    fn classify_error_kind(&self) -> pi_result::ErrorKind {
        pi_result::ErrorKind::Conflict
    }
}

struct BoundedBlockingFacility {
    permits: Arc<async_lock::Semaphore>,
    max_in_flight: usize,
}

const DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT: usize = 1000;
static BLOCKING_FACILITY: OnceLock<BoundedBlockingFacility> = OnceLock::new();

// 为进程级本地阻塞准入设施安装一次性容量配置。
//
// `get_or_init` 同时承担显式设置与首次默认使用之间的线性化。竞争失败方只比较
// 已发布数值；它不能替换设施，也不需要持有跨调用锁。这里刻意不修改底层共享
// worker 池，因为容量和 worker 数是两个独立资源维度，而且后者还服务进程中
// 本库无法拥有的其它调用方。
/// 设置当前进程中本地受控阻塞工作的最大在途容量。
///
/// `max_in_flight` 同时计算已经获准排队和正在执行的受控工作，而不是线程数、
/// 已打开文件数或全部异步任务数。容量饱和时，尚未提交的相关操作不会等待，
/// 而会返回 [`pi_result::ErrorKind::ResourceExhausted`]。
///
/// 本函数应在第一次需要该容量的本地文件操作之前调用。首次显式设置或首次使用
/// 会冻结当前进程配置；相同值可以幂等地重复设置，不同值返回
/// [`pi_result::ErrorKind::Conflict`]。配置不支持重置或运行中替换，并由所有
/// [`crate::LocalFileNamespace`] 句柄共享。
///
/// 未显式设置时，当前版本使用 `1000`。该默认值可以在后续版本调整；需要稳定
/// 部署配置的调用方应始终显式调用本函数。本函数不改变其它工作来源的并行度，
/// 也不保证限制不属于上述受控范围的工作。
///
/// 调用是同步的，不执行文件系统 I/O、不调用用户代码。首次成功可能建立少量
/// 进程级准入状态，因此不是纯函数；时间和额外空间复杂度均为 `O(1)`。
pub fn set_local_blocking_capacity(
    max_in_flight: NonZeroUsize,
) -> pi_result::Result<()> {
    let requested = max_in_flight.get();
    let configured = BLOCKING_FACILITY
        .get_or_init(|| BoundedBlockingFacility::new(requested))
        .max_in_flight;

    if configured == requested {
        Ok(())
    } else {
        Err(BlockingCapacityConflict {
            configured,
            requested,
        })
        .into_classified_error()
    }
}

impl BoundedBlockingFacility {
    fn new(max_in_flight: usize) -> Self {
        Self {
            permits: Arc::new(async_lock::Semaphore::new(max_in_flight)),
            max_in_flight,
        }
    }

    async fn run<F, T>(&self, operation: F) -> pi_result::Result<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = self.permits
            .try_acquire_arc()
            .ok_or(BlockingFacilitySaturated {
                max_in_flight: self.max_in_flight,
            })
            .into_classified_error()?;
        Ok(blocking::unblock(move || {
            // 许可必须由真实同步闭包拥有；外层 Future 在提交后被丢弃时，
            // `blocking` 仍会完成该闭包，容量也必须继续反映这项在途工作。
            let _permit = permit;
            operation()
        })
        .await)
    }

    async fn run_with_input<I, F, T>(
        &self,
        input: I,
        operation: F,
    ) -> core::result::Result<T, (pi_result::Error, I)>
    where
        I: Send + 'static,
        F: FnOnce(I) -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = match self.permits.try_acquire_arc() {
            Some(permit) => permit,
            None => {
                let error = core::result::Result::<(), _>::Err(
                    BlockingFacilitySaturated {
                        max_in_flight: self.max_in_flight,
                    },
                )
                .into_classified_error()
                .expect_err("an explicit saturation error cannot be success");
                return Err((error, input));
            }
        };
        Ok(blocking::unblock(move || {
            // 与 `run` 相同，许可必须跟随真实闭包，而不是调用方 Future。
            let _permit = permit;
            operation(input)
        })
        .await)
    }
}

// 非等待地准入一项拥有全部输入和输出的同步工作。
//
// 返回 Future 从未轮询时不会取得许可或提交闭包。容量已满时不会执行
// `operation`，而是返回 `ResourceExhausted`；成功提交后，许可由同步闭包
// 持有到真实完成，外层 Future 取消不会提前释放。
pub(crate) async fn unblock<F, T>(operation: F) -> pi_result::Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    BLOCKING_FACILITY
        .get_or_init(|| {
            BoundedBlockingFacility::new(
                DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT,
            )
        })
        .run(operation)
        .await
}

// 执行本身已经返回统一 `pi_result` 的同步工作，并把设施准入错误与操作错误
// 收敛到同一层。该函数只消除嵌套 `Result`，不改变操作错误的上下文或分类。
pub(crate) async fn unblock_result<F, T>(
    operation: F,
) -> pi_result::Result<T>
where
    F: FnOnce() -> pi_result::Result<T> + Send + 'static,
    T: Send + 'static,
{
    unblock(operation).await?
}

// 非等待地提交一项消费唯一拥有型输入的同步工作。准入失败时闭包绝不执行，
// 并把输入与设施错误一并返还，供调用方恢复临时移出 guard 的资源。
pub(crate) async fn unblock_with_input<I, F, T>(
    input: I,
    operation: F,
) -> core::result::Result<T, (pi_result::Error, I)>
where
    I: Send + 'static,
    F: FnOnce(I) -> T + Send + 'static,
    T: Send + 'static,
{
    BLOCKING_FACILITY
        .get_or_init(|| {
            BoundedBlockingFacility::new(
                DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT,
            )
        })
        .run_with_input(input, operation)
        .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use futures_lite::future::{self, block_on};
    use futures_lite::stream::StreamExt;

    use super::{
        BLOCKING_FACILITY, BoundedBlockingFacility,
        DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT,
    };
    use crate::{
        FileNamespace, LocalFileNamespace, WalkDepthLimit, WalkOptions,
    };

    // 验证生产设施对一个未饱和、无副作用的拥有型闭包执行恰好一次，并把闭包
    // 结果原样交还调用方。这是后续容量和取消语义的最小正向控制，不涉及真实
    // 文件系统、全局设施或并发资源竞争，可以与其它纯单元测试隔离运行。
    #[test]
    fn test_bounded_blocking_runs_one_admitted_operation() {
        let facility = BoundedBlockingFacility::new(1);
        let value = block_on(facility.run(|| 42_u64))
            .expect("未饱和设施应执行获准闭包");

        assert_eq!(value, 42);
    }

    // 验证容量一的生产设施在首个真实阻塞闭包仍在执行时，不等待、不提交第二
    // 个闭包，并把失败精确分类为资源耗尽。同步通道只控制测试时序；原子计数
    // 是独立负面控制，用来证明第二个闭包没有被测试脚手架或后台延迟执行。
    // 两次等待均有两秒截止；本测试占用一个阻塞 worker，必须串行运行。
    #[test]
    fn test_bounded_blocking_rejects_work_beyond_capacity_without_running_it() {
        let facility = Arc::new(BoundedBlockingFacility::new(1));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let first_facility = Arc::clone(&facility);
        let first = async_global_executor::spawn(async move {
            first_facility
                .run(move || {
                    started_sender.send(()).expect("应能报告闭包已开始");
                    release_receiver.recv().expect("应能收到释放信号");
                    1_u8
                })
                .await
        });
        started_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("首个阻塞闭包应在截止前开始");

        let executions = Arc::new(AtomicUsize::new(0));
        let second_executions = Arc::clone(&executions);
        let error = block_on(facility.run(move || {
            second_executions.fetch_add(1, Ordering::Relaxed);
            2_u8
        }))
        .expect_err("超过容量的工作必须立即失败");

        assert_eq!(
            error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        assert_eq!(executions.load(Ordering::Relaxed), 0);
        release_sender.send(()).expect("应能释放首个闭包");
        assert_eq!(block_on(first).expect("首个闭包应成功"), 1);
        assert_eq!(
            block_on(facility.run(|| 3_u8)).expect("释放许可后应能重新准入"),
            3,
        );
    }

    // 验证容量拒绝发生在拥有型输入被提交以前：设施必须把原输入完整返还，
    // 且绝不能执行消费输入的闭包。该语义用于建图等先从资源 guard 中暂时
    // 取出唯一文件句柄的路径，否则瞬时饱和会意外关闭仍应可用的公开资源。
    // 同步通道只建立确定的饱和时序；本测试占用一个阻塞 worker，须串行。
    #[test]
    fn test_bounded_blocking_rejection_returns_owned_input() {
        let facility = Arc::new(BoundedBlockingFacility::new(1));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let first_facility = Arc::clone(&facility);
        let first = async_global_executor::spawn(async move {
            first_facility
                .run(move || {
                    started_sender.send(()).expect("应能报告闭包已开始");
                    release_receiver.recv().expect("应能收到释放信号");
                })
                .await
        });
        started_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("首个阻塞闭包应在截止前开始");

        let executions = Arc::new(AtomicUsize::new(0));
        let rejected_executions = Arc::clone(&executions);
        let input = vec![1_u8, 2, 3, 4];
        let (error, returned_input) = block_on(facility.run_with_input(
            input,
            move |owned_input| {
                rejected_executions.fetch_add(1, Ordering::Relaxed);
                owned_input.len()
            },
        ))
        .expect_err("超过容量的拥有型工作必须被拒绝并返还输入");

        assert_eq!(
            error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        assert_eq!(returned_input, vec![1_u8, 2, 3, 4]);
        assert_eq!(executions.load(Ordering::Relaxed), 0);
        release_sender.send(()).expect("应能释放首个闭包");
        block_on(first).expect("首个闭包应成功");
    }

    // 验证容量二准确允许两项真实在途工作，而第三项仍立即失败。两个开始信号
    // 证明容量不是错误地按“任务批次”或单一 worker 计算；第三项原子计数为零
    // 证明饱和分支没有延迟提交。测试占用两个阻塞 worker，必须串行运行。
    #[test]
    fn test_bounded_blocking_capacity_two_admits_exactly_two_operations() {
        let facility = Arc::new(BoundedBlockingFacility::new(2));
        let (started_sender, started_receiver) = mpsc::sync_channel(2);
        let (release_sender, release_receiver) = mpsc::channel();
        let release_receiver = Arc::new(std::sync::Mutex::new(release_receiver));
        let mut admitted = Vec::new();
        for index in 0_u8..2 {
            let facility = Arc::clone(&facility);
            let started_sender = started_sender.clone();
            let release_receiver = Arc::clone(&release_receiver);
            admitted.push(async_global_executor::spawn(async move {
                facility.run(move || {
                    started_sender.send(index).expect("应能报告闭包已开始");
                    release_receiver.lock()
                        .expect("释放通道锁不应中毒")
                        .recv()
                        .expect("应能收到释放信号");
                    index
                }).await
            }));
        }
        let mut started = [
            started_receiver.recv_timeout(Duration::from_secs(2))
                .expect("首项应在截止前开始"),
            started_receiver.recv_timeout(Duration::from_secs(2))
                .expect("第二项应在截止前开始"),
        ];
        started.sort_unstable();
        assert_eq!(started, [0, 1]);

        let executions = Arc::new(AtomicUsize::new(0));
        let rejected_executions = Arc::clone(&executions);
        let error = block_on(facility.run(move || {
            rejected_executions.fetch_add(1, Ordering::Relaxed);
        }))
        .expect_err("第三项必须超过容量二");
        assert_eq!(
            error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        assert_eq!(executions.load(Ordering::Relaxed), 0);

        release_sender.send(()).expect("应能释放首项");
        release_sender.send(()).expect("应能释放第二项");
        for task in admitted {
            block_on(task).expect("获准闭包应成功");
        }
    }

    // 验证调用方取消已经提交的阻塞工作后，容量许可仍由真实闭包持有，直到
    // 闭包报告完成。若许可错误地跟随 Future 栈释放，第二个闭包会执行并使
    // 负面控制计数增加。同步通道分别证明首个闭包已开始和最终已结束；两次
    // 等待均有两秒截止。本测试占用一个阻塞 worker，必须串行运行。
    #[test]
    fn test_bounded_blocking_cancellation_does_not_release_running_work_permit() {
        let facility = Arc::new(BoundedBlockingFacility::new(1));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(0);
        let (finished_sender, finished_receiver) = mpsc::sync_channel(1);
        let mut first = Box::pin(facility.run(move || {
            started_sender.send(()).expect("应能报告闭包已开始");
            release_receiver.recv().expect("应能收到释放信号");
            finished_sender.send(()).expect("应能报告闭包已结束");
        }));
        block_on(async {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                assert!(
                    future::poll_once(first.as_mut()).await.is_none(),
                    "释放信号发出前闭包不能完成",
                );
                match started_receiver.try_recv() {
                    Ok(()) => break,
                    Err(mpsc::TryRecvError::Empty) => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "首个阻塞闭包应在截止前开始",
                        );
                        future::yield_now().await;
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        panic!("闭包开始前发送端不应断开")
                    }
                }
            }
        });
        drop(first);

        let executions = Arc::new(AtomicUsize::new(0));
        let second_executions = Arc::clone(&executions);
        let second = block_on(facility.run(move || {
            second_executions.fetch_add(1, Ordering::Relaxed);
        }));
        release_sender.send(()).expect("应能释放首个闭包");
        finished_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("取消等待后首个闭包仍应在截止前真实结束");

        let error = second.expect_err("运行中工作必须继续占用唯一许可");
        assert_eq!(
            error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        assert_eq!(executions.load(Ordering::Relaxed), 0);
    }

    // 验证公开目录创建与逐项推动全部服从同一生产准入容量。测试只直接占用真实
    // 生产许可来构造确定的饱和状态；目录结果和错误仍完全通过公开 namespace/
    // stream 接缝观察，没有在测试侧实现目录读取。该测试会短暂占满进程级容量，
    // 必须按项目门禁使用 `--test-threads=1` 串行执行。
    #[test]
    fn test_directory_stream_steps_observe_global_blocking_capacity() {
        let directory = tempfile::tempdir().expect("应能创建真实临时目录");
        let child = directory.path().join("child");
        std::fs::create_dir(&child).expect("应能创建直接子目录");
        std::fs::write(child.join("nested"), b"nested")
            .expect("应能创建递归后代");
        let root = directory.path().to_path_buf();
        let namespace = LocalFileNamespace::new();
        let facility = BLOCKING_FACILITY.get_or_init(|| {
            BoundedBlockingFacility::new(
                DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT,
            )
        });

        let mut permits =
            Vec::with_capacity(DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT);
        for _ in 0..DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT {
            permits.push(
                facility.permits
                    .try_acquire_arc()
                    .expect("串行测试应能占用全部生产许可"),
            );
        }

        let read_dir_error = match block_on(namespace.read_dir(root.clone())) {
            Ok(_) => panic!("饱和时不得建立浅层目录流"),
            Err(error) => error,
        };
        assert_eq!(
            read_dir_error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        let walk_error = match block_on(namespace.walk(
            root.clone(),
            WalkOptions::new(WalkDepthLimit::Unlimited),
        )) {
            Ok(_) => panic!("饱和时不得建立递归目录流"),
            Err(error) => error,
        };
        assert_eq!(
            walk_error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );

        drop(permits);
        let mut shallow = block_on(namespace.read_dir(root.clone()))
            .expect("释放容量后应能建立浅层目录流");
        let mut recursive = block_on(namespace.walk(
            root,
            WalkOptions::new(WalkDepthLimit::Unlimited),
        ))
        .expect("释放容量后应能建立递归目录流");

        let mut permits =
            Vec::with_capacity(DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT);
        for _ in 0..DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT {
            permits.push(
                facility.permits
                    .try_acquire_arc()
                    .expect("串行测试应能再次占用全部生产许可"),
            );
        }
        let shallow_error = block_on(shallow.next())
            .expect("饱和应作为浅层流项目返回")
            .expect_err("饱和的逐项读取不得成功");
        assert_eq!(
            shallow_error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        assert!(
            block_on(shallow.next()).is_none(),
            "浅层流在首个迭代错误后必须终止",
        );
        let recursive_error = block_on(recursive.next())
            .expect("饱和应作为递归流项目返回")
            .expect_err("饱和的递归逐项读取不得成功");
        assert_eq!(
            recursive_error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );

        drop(permits);
        let child_entry = block_on(recursive.next())
            .expect("容量恢复后递归流应保留当前目录状态")
            .expect("容量恢复后应产生直接子目录");
        assert_eq!(child_entry.entry.locator, child);

        let mut permits =
            Vec::with_capacity(DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT);
        for _ in 0..DEFAULT_MAX_BLOCKING_WORK_IN_FLIGHT {
            permits.push(
                facility.permits
                    .try_acquire_arc()
                    .expect("串行测试应能第三次占用全部生产许可"),
            );
        }
        let descent_error = block_on(recursive.next())
            .expect("子目录打开饱和应作为递归流项目返回")
            .expect_err("饱和时不得进入子目录");
        assert_eq!(
            descent_error.current_context(),
            &pi_result::ErrorKind::ResourceExhausted,
        );
        drop(permits);
        assert!(
            block_on(recursive.next()).is_none(),
            "失败子树被报告并跳过后，遍历应正常结束",
        );
    }
}
