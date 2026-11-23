"""切换编排器单元测试：用内存假适配器覆盖成功、超时、重启核对三条主线。"""
from __future__ import annotations

import os
import tempfile
import unittest

from app.state import (
    PHASE_NORMAL,
    PHASE_PROMOTED,
    PHASE_SWITCH_PENDING,
    RESULT_FAILED,
    RESULT_SUCCESS,
    RESULT_TIMEOUT,
    StateStore,
    SwitchError,
)
from app.switcher import Switcher, lsn_int


class ManualClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def time(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class FakeAdapter:
    """模拟一对主备，支持 LSN 追赶、暂停回放、提升和只读围栏。"""

    def __init__(self, clock: ManualClock) -> None:
        self.clock = clock
        self.recovery = {"A": False, "B": True}
        self.lsn = {"A": "0/1000", "B": "0/0"}          # 主的当前 LSN / 备已接收 LSN
        self.replayed = {"A": None, "B": "0/0"}
        self.read_only = {"A": False, "B": False}
        self.paused = False
        self.promote_calls = 0
        self.events: list[str] = []
        self.writers_busy = False

    def is_in_recovery(self, node: str) -> bool:
        return self.recovery[node]

    def is_fenced_readonly(self, node: str) -> bool:
        return (not self.recovery[node]) and self.read_only[node]

    def current_wal_lsn(self, node: str) -> str:
        return self.lsn[node]

    def replayed_wal_lsn(self, node: str) -> str:
        return self.replayed[node] or "0/0"

    def set_read_only(self, node: str, read_only: bool) -> None:
        self.read_only[node] = read_only
        self.events.append(f"readonly:{node}={'on' if read_only else 'off'}")

    def wait_for_wal_quiesce(self, node: str, deadline: float,
                             poll_interval: float):
        if self.writers_busy:
            return None
        return self.lsn[node]

    def promote(self, node: str) -> None:
        self.promote_calls += 1
        self.events.append(f"promote:{node}")
        # 与真实 pg_promote(wait=>false) 一致：角色在后续轮询时翻转。
        self._pending_promote = node

    def wait_until_primary(self, node: str, timeout_seconds: int) -> None:
        if getattr(self, "_pending_promote", None) == node:
            self.recovery[node] = False
            self._pending_promote = None
        if self.recovery[node]:
            raise SwitchError("提升未完成")

    def catch_up(self, lsn: str) -> None:
        """模拟备库收到并回放 WAL。"""
        self.lsn["B"] = lsn
        if not self.paused:
            self.replayed["B"] = lsn

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False
        self.replayed["B"] = self.lsn["B"]


class SwitcherTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "switch.json"))
        self.clock = ManualClock()
        self.adapter = FakeAdapter(self.clock)
        # 测试里不真睡：clock 由用例手工推进。
        self.switcher = Switcher(
            self.store, self.adapter,
            poll_interval=1.0, promote_wait_seconds=5,
            sleep=lambda _s: None, clock=self.clock.time,
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_lsn_int_ordering(self) -> None:
        self.assertEqual(lsn_int("0/0"), 0)
        self.assertLess(lsn_int("0/FFFF"), lsn_int("1/0"))
        self.assertEqual(lsn_int("1/10"), (1 << 32) + 16)

    def test_switch_success_after_replay_catches_up(self) -> None:
        # 受理：旧主置只读，记录目标 LSN 与截止时间。
        state = self.switcher.start_switch("B", timeout=20.0)
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)
        self.assertTrue(self.adapter.read_only["A"])
        self.assertEqual(state.target_lsn, "0/1000")

        # 第一轮：备库落后，且暂停回放，推进时间未超时 -> 继续等待。
        self.adapter.pause()
        state = self.switcher.run_once()
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)

        # 恢复回放并追上目标 LSN -> 提升并收尾。
        self.adapter.resume()
        self.adapter.catch_up("0/1000")
        state = self.switcher.run_once()
        self.assertEqual(self.adapter.promote_calls, 1)
        self.assertEqual(state.phase, PHASE_NORMAL)
        self.assertEqual(state.leader, "B")
        self.assertFalse(self.adapter.read_only["B"])
        self.assertEqual(state.history[-1].result, RESULT_SUCCESS)

    def test_switch_times_out_when_replay_stays_paused(self) -> None:
        self.switcher.start_switch("B", timeout=5.0)
        self.adapter.pause()

        # 轮询若干次，每次推进 2 秒，必然越过截止时间。
        state = None
        for _ in range(5):
            state = self.switcher.run_once()
            self.clock.advance(2.0)
            if state.phase == PHASE_NORMAL:
                break
        self.assertEqual(state.phase, PHASE_NORMAL)
        self.assertEqual(state.leader, "A")  # 旧主恢复写入
        self.assertFalse(self.adapter.read_only["A"])
        self.assertEqual(self.adapter.promote_calls, 0)  # 没追平绝不提升
        self.assertEqual(state.history[-1].result, RESULT_TIMEOUT)

        # 超时回滚后恢复回放，再次发起切换可以成功。
        self.adapter.resume()
        self.adapter.catch_up("0/1000")
        self.switcher.start_switch("B", timeout=20.0)
        state = self.switcher.run_once()
        self.assertEqual(state.leader, "B")
        self.assertEqual(state.history[-1].result, RESULT_SUCCESS)

    def test_inflight_writes_drain_before_final_lsn_locks(self) -> None:
        # 受理后旧主上还有已接单的写事务在执行。
        state = self.switcher.start_switch("B", timeout=20.0)
        self.adapter.writers_busy = True
        state = self.switcher.run_once()
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)
        self.assertFalse(state.lsn_finalized)

        # 写事务提交后旧主 LSN 前移到 0/2000，备库目前只回放了 0/1000。
        self.adapter.writers_busy = False
        self.adapter.lsn["A"] = "0/2000"
        self.adapter.replayed["B"] = "0/1000"
        state = self.switcher.run_once()
        self.assertTrue(state.lsn_finalized)
        self.assertEqual(state.target_lsn, "0/2000")  # 锁定的是收尾后的最终 LSN
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)  # 还没追平，继续等

        # 备库回放追平最终 LSN 后才允许提升。
        self.adapter.replayed["B"] = "0/2000"
        state = self.switcher.run_once()
        self.assertEqual(state.leader, "B")
        self.assertEqual(state.history[-1].target_lsn, "0/2000")
        self.assertEqual(state.history[-1].result, RESULT_SUCCESS)

    def test_target_no_longer_standby_finalizes_failure_and_restores_old_leader(self) -> None:
        self.switcher.start_switch("B", timeout=20.0)
        # 推进到目标节点时，B 已不是备库（异常拓扑）。
        self.adapter.recovery["B"] = False
        state = self.switcher.run_once()
        self.assertEqual(state.phase, PHASE_NORMAL)
        self.assertEqual(state.leader, "A")
        self.assertFalse(self.adapter.read_only["A"])  # 旧主围栏已解除
        self.assertEqual(state.history[-1].result, RESULT_FAILED)
        self.assertEqual(self.adapter.promote_calls, 0)  # 绝不能提升

    def test_reject_start_when_old_leader_is_standby(self) -> None:
        self.adapter.recovery["A"] = True
        with self.assertRaises(SwitchError):
            self.switcher.start_switch("B", timeout=5.0)

    def test_reconcile_after_crash_between_promote_and_target_update(self) -> None:
        # 场景：SWITCH_PENDING 期间备库实际已被提升，但 Web 进程崩溃，
        # 持久化状态还停在 SWITCH_PENDING。
        state = self.switcher.start_switch("B", timeout=20.0)
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)
        self.adapter.catch_up("0/1000")
        # 模拟提升已落库但进程在 mark_promoted 前退出。
        self.adapter.recovery["B"] = False

        new_switcher = Switcher(
            self.store, self.adapter,
            poll_interval=1.0, promote_wait_seconds=5,
            sleep=lambda _s: None, clock=self.clock.time,
        )
        fixed = new_switcher.reconcile_on_startup()
        self.assertEqual(fixed.phase, PHASE_NORMAL)
        self.assertEqual(fixed.leader, "B")  # 重启后只认真正的主库
        self.assertEqual(fixed.history[-1].result, RESULT_SUCCESS)

    def test_reconcile_rolls_back_when_promotion_never_happened(self) -> None:
        self.switcher.start_switch("B", timeout=5.0)
        # A 仍是唯一主库（提升未发生）。
        fixed = self.switcher.reconcile_on_startup()
        self.assertEqual(fixed.leader, "A")
        self.assertEqual(fixed.phase, PHASE_NORMAL)
        self.assertEqual(fixed.history[-1].result, RESULT_TIMEOUT)
        self.assertFalse(self.adapter.read_only["A"])

    def test_reconcile_corrects_stale_leader_in_normal_phase(self) -> None:
        # NORMAL 状态但实际主库已是 B（例如手工提升后重启）。
        self.adapter.recovery["A"] = True
        self.adapter.recovery["B"] = False
        fixed = self.switcher.reconcile_on_startup()
        self.assertEqual(fixed.leader, "B")
        self.assertEqual(fixed.phase, PHASE_NORMAL)

    def test_reconcile_refuses_split_brain(self) -> None:
        self.adapter.recovery["A"] = False
        self.adapter.recovery["B"] = False
        with self.assertRaises(SwitchError):
            self.switcher.reconcile_on_startup()

    def test_promoted_phase_resume_after_crash(self) -> None:
        state = self.switcher.start_switch("B", timeout=20.0)
        self.adapter.catch_up("0/1000")
        self.adapter.promote("B")
        self.store.mark_promoted(state)
        # 进程在 finalize_success 前退出；重启后 B 已是主库。
        self.adapter.recovery["B"] = False
        fixed = Switcher(
            self.store, self.adapter,
            sleep=lambda _s: None, clock=self.clock.time,
        ).reconcile_on_startup()
        self.assertEqual(fixed.leader, "B")
        self.assertEqual(fixed.phase, PHASE_NORMAL)
        self.assertEqual(fixed.history[-1].result, RESULT_SUCCESS)


if __name__ == "__main__":
    unittest.main()
