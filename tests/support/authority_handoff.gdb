# R01独立专项：只控制线程调度；不修改生产内存、指令或返回值。
# 用法：PI_ASYNC_FS_HANDOFF_DIR=<新的ext4目录> PI_ASYNC_FS_HANDOFF_INTERLEAVE=1
# timeout 45s gdb --batch -x tests/support/authority_handoff.gdb --args <测试二进制>
#   --exact test_coordinated_resource_handoff_keeps_removal_blocked_until_last_drop
#   --nocapture --test-threads=1
set pagination off
set confirm off
set non-stop on
set breakpoint pending on
set detach-on-fork on
set follow-fork-mode parent
set print thread-events off
python
import gdb
import os
import time

fixture = os.environ["PI_ASYNC_FS_HANDOFF_DIR"]
diagnostic_exit = None

def record_exit(event):
    global diagnostic_exit
    diagnostic_exit = getattr(event, "exit_code", None)

gdb.events.exited.connect(record_exit)

class LifecycleInterleave(gdb.Breakpoint):
    def __init__(self):
        # 源码位置敏感性显式保留：找首次状态锁块之后的等价观察点。
        # 修复前后均只读requires_lock；不能为了转绿跳过这个暂停或改断言。
        with open("src/local.rs") as source:
            lines = source.readlines()
        method = next(i for i, line in enumerate(lines) if "async fn acquire_lifecycle_owner_count(" in line)
        start = next(i for i in range(method, len(lines)) if "let requires_lock = {" in lines[i])
        end = next(i for i in range(start, len(lines)) if lines[i].rstrip() == "        };")
        position = next(i for i in range(end + 1, len(lines)) if lines[i].strip()) + 1
        super().__init__("src/local.rs:" + str(position), internal=False)
        self.injected = False

    def stop(self):
        if self.injected or not os.path.exists(os.path.join(fixture, "armed")):
            return False
        try:
            requires_lock = bool(gdb.parse_and_eval("requires_lock"))
        except gdb.error as error:
            gdb.write("R01_DEBUGGER_LOCALS_ERROR=" + str(error) + "\n")
            return True
        if requires_lock:
            return False
        self.injected = True
        gdb.write("R01_DEBUGGER_PAUSED_B_AFTER_NONZERO_OBSERVATION\n")
        with open(os.path.join(fixture, "release-a"), "w") as signal:
            signal.write("release")
        deadline = time.monotonic() + 10.0
        while not os.path.exists(os.path.join(fixture, "released-a")):
            if time.monotonic() >= deadline:
                gdb.write("R01_DEBUGGER_A_RELEASE_TIMEOUT\n")
                return True
            time.sleep(0.001)
        with open(os.path.join(fixture, "injection-complete"), "w") as evidence:
            evidence.write("B paused without state mutation; A normally dropped")
        gdb.write("R01_DEBUGGER_A_NORMALLY_DROPPED_BEFORE_B_RESUMES\n")
        return False

probe = LifecycleInterleave()
end
run
python
if not probe.injected:
    raise gdb.GdbError("R01 required interleave was not observed; no product verdict")
if diagnostic_exit is None:
    raise gdb.GdbError("R01 inferior did not complete; no product verdict")
if diagnostic_exit != 0:
    raise gdb.GdbError("R01 regression failed; inferior exit=" + str(diagnostic_exit))
end
