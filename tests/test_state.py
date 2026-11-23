"""状态机持久化与非法流转的单元测试（无需数据库）。"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from app.state import (
    PHASE_NORMAL,
    PHASE_PROMOTED,
    PHASE_ROLLBACK,
    PHASE_SWITCH_PENDING,
    RESULT_SUCCESS,
    RESULT_TIMEOUT,
    StateStore,
    SwitchError,
    SwitchState,
    atomic_write_json,
)


class StateStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "state", "switch.json")
        self.store = StateStore(self.path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_load_creates_default_file(self) -> None:
        state = self.store.load()
        self.assertEqual(state.phase, PHASE_NORMAL)
        self.assertEqual(state.leader, "A")
        self.assertTrue(os.path.exists(self.path))

    def test_full_success_lifecycle(self) -> None:
        state = self.store.begin_switch("B", "0/1000", timeout=10.0, now=100.0)
        self.assertEqual(state.phase, PHASE_SWITCH_PENDING)
        self.assertEqual(state.deadline, 110.0)
        self.assertEqual(state.current.old_leader, "A")

        self.store.mark_promoted(state)
        self.assertEqual(state.phase, PHASE_PROMOTED)
        self.store.finalize_success(state, now=105.0)

        again = self.store.load()
        self.assertEqual(again.phase, PHASE_NORMAL)
        self.assertEqual(again.leader, "B")
        self.assertEqual(again.current, None)
        self.assertEqual(len(again.history), 1)
        self.assertEqual(again.history[0].result, RESULT_SUCCESS)

    def test_timeout_rollback_lifecycle_keeps_leader(self) -> None:
        state = self.store.begin_switch("B", "0/1000", timeout=5.0, now=0.0)
        self.store.mark_rollback(state)
        self.assertEqual(state.phase, PHASE_ROLLBACK)
        self.store.finalize_timeout(state, "模拟回放超时", now=6.0)

        again = self.store.load()
        self.assertEqual(again.phase, PHASE_NORMAL)
        self.assertEqual(again.leader, "A")  # 旧主恢复写入
        self.assertEqual(again.history[0].result, RESULT_TIMEOUT)

    def test_reject_switch_while_switch_pending(self) -> None:
        self.store.begin_switch("B", "0/1", timeout=5.0)
        with self.assertRaises(SwitchError):
            self.store.begin_switch("A", "0/2", timeout=5.0)

    def test_reject_switch_to_current_leader(self) -> None:
        with self.assertRaises(SwitchError):
            self.store.begin_switch("A", "0/1", timeout=5.0)

    def test_illegal_phase_transitions_rejected(self) -> None:
        fresh = SwitchState()
        with self.assertRaises(SwitchError):
            self.store.mark_promoted(fresh)
        with self.assertRaises(SwitchError):
            self.store.mark_rollback(fresh)

    def test_round_trip_through_json(self) -> None:
        state = self.store.begin_switch("B", "0/ABCD", timeout=3.0, now=7.0)
        self.store.mark_promoted(state)
        reloaded = self.store.load()
        self.assertEqual(reloaded.phase, PHASE_PROMOTED)
        self.assertEqual(reloaded.target_lsn, "0/ABCD")
        self.assertEqual(reloaded.current.new_leader, "B")

    def test_lsn_finalized_flag_round_trip_and_resets_on_finalize(self) -> None:
        state = self.store.begin_switch("B", "0/100", timeout=3.0)
        self.assertFalse(state.lsn_finalized)
        self.store.update_target_lsn(state, "0/200")
        reloaded = self.store.load()
        self.assertTrue(reloaded.lsn_finalized)
        self.assertEqual(reloaded.target_lsn, "0/200")

        self.store.mark_promoted(reloaded)
        self.store.finalize_success(reloaded)
        clean = self.store.load()
        self.assertFalse(clean.lsn_finalized)
        self.assertIsNone(clean.target_lsn)


class AtomicWriteTest(unittest.TestCase):
    def test_atomic_write_is_fsynced_and_replaceable(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "f.json")
            atomic_write_json(path, {"x": 1})
            with open(path, encoding="utf-8") as fh:
                self.assertEqual(json.load(fh), {"x": 1})
            atomic_write_json(path, {"x": 2})
            with open(path, encoding="utf-8") as fh:
                self.assertEqual(json.load(fh), {"x": 2})
            # 不留临时文件
            self.assertEqual(sorted(os.listdir(d)), ["f.json"])


if __name__ == "__main__":
    unittest.main()
