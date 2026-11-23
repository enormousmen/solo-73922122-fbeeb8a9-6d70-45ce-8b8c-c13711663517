"""Web 层测试：FastAPI + 内存假适配器，验证 503、切换与重启核对的接口行为。"""
from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.config import NodeConfig, Settings
from app.main import create_app
from app.state import PHASE_NORMAL, PHASE_SWITCH_PENDING
from app.switcher import Switcher


class ManualClock:
    def __init__(self) -> None:
        self.t = 1000.0

    def time(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class WebFakeAdapter:
    def __init__(self, clock: ManualClock) -> None:
        self.clock = clock
        self.recovery = {"A": False, "B": True}
        self.lsn = {"A": "0/1000", "B": "0/0"}
        self.replayed = {"A": None, "B": "0/0"}
        self.read_only = {"A": False, "B": False}
        self.paused = False
        self.records: list[dict] = []
        self._next_id = 1
        self.promote_calls = 0

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

    def wait_for_wal_quiesce(self, node: str, deadline: float,
                             poll_interval: float) -> str:
        return self.lsn[node]

    def promote(self, node: str) -> None:
        self.promote_calls += 1
        self._pending = node

    def wait_until_primary(self, node: str, timeout_seconds: int) -> None:
        if getattr(self, "_pending", None) == node:
            self.recovery[node] = False
            self._pending = None
        if self.recovery[node]:
            raise RuntimeError("still standby")

    def pause_replay(self, node: str) -> None:
        if not self.recovery[node]:
            raise RuntimeError("not standby")
        self.paused = True

    def resume_replay(self, node: str) -> None:
        if not self.recovery[node]:
            raise RuntimeError("not standby")
        self.paused = False
        self.replayed["B"] = self.lsn["B"]

    def is_replay_paused(self, node: str) -> bool:
        return self.paused if self.recovery[node] else False

    def node_status(self, node: str) -> dict:
        return {
            "name": node, "reachable": True,
            "role": "standby" if self.recovery[node] else "primary",
            "replay_paused": self.paused if self.recovery[node] else None,
            "wal_lsn": self.lsn[node], "replay_lsn": self.replayed[node],
            "read_only": self.read_only[node] if not self.recovery[node] else None,
        }

    def ensure_schema(self) -> None:
        return None

    def insert_record(self, node: str, content: str) -> dict:
        row = {
            "id": self._next_id, "content": content,
            "written_by": f"node-{node}",
            "created_at": datetime.now(timezone.utc).isoformat(), "node": node,
        }
        self._next_id += 1
        self.records.append(row)
        return row

    def list_records(self, node: str, limit: int = 50) -> list[dict]:
        return list(reversed(self.records[-limit:]))

    def catch_up(self) -> None:
        self.lsn["B"] = self.lsn["A"]
        if not self.paused:
            self.replayed["B"] = self.lsn["A"]


class WebApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = ManualClock()
        self.adapter = WebFakeAdapter(self.clock)
        self.state_path = os.path.join(self.tmp.name, "switch.json")
        settings = Settings(
            node_a=NodeConfig("A", "ahost", 5432, "u", "p", "db", "admin", "apass"),
            node_b=NodeConfig("B", "bhost", 5432, "u", "p", "db", "admin", "apass"),
            state_file=self.state_path,
            switch_timeout=5.0,
            poll_interval=1.0,
            promote_wait_seconds=5,
            background_worker=False,  # 测试中手工驱动状态机，避免后台线程并发
        )
        self.app = create_app(settings, self.adapter)
        self.client = TestClient(self.app)
        self.client.__enter__()
        self.store = self.app.state.store
        self.switcher = self.app.state.switcher

    def tearDown(self) -> None:
        self.client.__exit__(None, None, None)
        self.tmp.cleanup()

    def _drive_until_normal(self, max_steps: int = 20, advance: float = 2.0) -> None:
        for _ in range(max_steps):
            state = self.store.load()
            if state.phase == PHASE_NORMAL:
                return
            self.switcher.run_once()
            self.clock.advance(advance)
        self.fail("切换一直没有回到 NORMAL")

    def test_index_page_renders(self) -> None:
        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        self.assertIn("主备单向切换", resp.text)

    def test_write_and_query_before_switch(self) -> None:
        resp = self.client.post("/api/records", json={"content": "hello-1"})
        self.assertEqual(resp.status_code, 201)
        self.assertEqual(resp.json()["node"], "A")

        resp = self.client.get("/api/records")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["count"], 1)

    def test_switch_accepts_then_writes_get_503_and_timeout_rolls_back(self) -> None:
        # 暂停回放，制造追不上的场景。
        r = self.client.post("/api/replay/pause", json={})
        self.assertEqual(r.status_code, 200)

        r = self.client.post("/api/switch", json={})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["accepted"])
        self.assertEqual(self.store.load().phase, PHASE_SWITCH_PENDING)

        # 切换受理后，新写入一律 503 + Retry-After。
        r = self.client.post("/api/records", json={"content": "blocked"})
        self.assertEqual(r.status_code, 503)
        self.assertEqual(r.headers.get("retry-after"), "5")
        self.assertEqual(r.json()["detail"]["error"], "SWITCH_IN_PROGRESS")

        # 驱动状态机直到超时回滚。
        self._drive_until_normal()
        state = self.store.load()
        self.assertEqual(state.leader, "A")
        self.assertEqual(state.history[-1].result, "TIMEOUT")

        # 回滚后写入恢复，仍走旧主 A。
        r = self.client.post("/api/records", json={"content": "after-rollback"})
        self.assertEqual(r.status_code, 201)
        self.assertEqual(r.json()["node"], "A")

    def test_resume_replay_then_switch_succeeds_and_writes_go_to_b(self) -> None:
        # 先在旧主写一条已确认记录。
        r = self.client.post("/api/records", json={"content": "committed-on-A"})
        self.assertEqual(r.status_code, 201)

        # 第一次切换因暂停回放超时。
        self.client.post("/api/replay/pause", json={})
        self.client.post("/api/switch", json={})
        self._drive_until_normal()
        self.assertEqual(self.store.load().history[-1].result, "TIMEOUT")

        # 恢复回放并重新切换：备库追平后提升。
        self.client.post("/api/replay/resume", json={})
        self.adapter.catch_up()
        r = self.client.post("/api/switch", json={})
        self.assertEqual(r.status_code, 200)
        self._drive_until_normal()

        state = self.store.load()
        self.assertEqual(state.leader, "B")
        self.assertEqual(state.history[-1].result, "SUCCESS")
        self.assertEqual(self.adapter.promote_calls, 1)

        # 新写入只走新主 B。
        r = self.client.post("/api/records", json={"content": "after-promotion"})
        self.assertEqual(r.status_code, 201)
        self.assertEqual(r.json()["node"], "B")

        # 已确认的旧记录在新主上同样查得到。
        r = self.client.get("/api/records?node=B")
        contents = {row["content"] for row in r.json()["records"]}
        self.assertIn("committed-on-A", contents)
        self.assertIn("after-promotion", contents)

    def test_duplicate_switch_is_rejected(self) -> None:
        self.client.post("/api/replay/pause", json={})
        self.assertEqual(self.client.post("/api/switch", json={}).status_code, 200)
        r = self.client.post("/api/switch", json={})
        self.assertEqual(r.status_code, 409)

    def test_restart_after_promotion_routes_writes_to_new_primary(self) -> None:
        # 完成一次成功切换到 B。
        self.adapter.catch_up()
        self.client.post("/api/switch", json={})
        self._drive_until_normal()
        self.assertEqual(self.store.load().leader, "B")

        # 模拟“提升后、更新写入目标前退出”：新进程、同一状态文件、同一数据库。
        second_app = create_app(
            Settings(
                node_a=NodeConfig("A", "ahost", 5432, "u", "p", "db", "admin", "apass"),
                node_b=NodeConfig("B", "bhost", 5432, "u", "p", "db", "admin", "apass"),
                state_file=self.state_path,
                switch_timeout=5.0, poll_interval=1.0, promote_wait_seconds=5,
                background_worker=False,
            ),
            self.adapter,
        )
        with TestClient(second_app) as client2:
            # 启动核对后 leader 仍为 B，写入只走 B。
            r = client2.post("/api/records", json={"content": "after-restart"})
            self.assertEqual(r.status_code, 201)
            self.assertEqual(r.json()["node"], "B")

    def test_status_reports_both_nodes(self) -> None:
        r = self.client.get("/api/status")
        self.assertEqual(r.status_code, 200)
        roles = {n["name"]: n["role"] for n in r.json()["nodes"]}
        self.assertEqual(roles, {"A": "primary", "B": "standby"})
        self.assertEqual(r.json()["cluster"]["phase"], PHASE_NORMAL)


if __name__ == "__main__":
    unittest.main()
