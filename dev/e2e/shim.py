"""本地沙箱 E2E 专用适配垫片（不进入 Docker 交付物）。

本机 seccomp 禁止 postmaster 拉起 walreceiver 等子进程，无法运行真正的
流复制备库。因此本地用「pg_receivewal + 门控归档 + 归档恢复备库」等价替代：

* WAL 的接收/落盘语义与流复制备库相同（完整段先 fsync 再可见）；
* 备库 startup 进程真实回放归档段，恢复到的位置即磁盘上已有的位置。

垫片只重写 B 的角色判定与落盘快照，使 :class:`PgClusterAdapter` 上层
（FastAPI 路由、核验判定、状态机）运行的全是交付代码。
B 的三种故障用 state/ 下的标记文件模拟：

* b_unreachable      模拟 B 不可达（连接失败）
* b_promoted         模拟 B 已提升（pg_is_in_recovery()=false）
* b_replay_paused    模拟回放暂停（归档门同时关闭，WAL 仍在磁盘）
"""
from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

import psycopg

from app.database import PgClusterAdapter

SEGMENT_SIZE_LSN = 1 << 24  # 16MB，对应 LSN 数值单位
SEGMENT_RE = re.compile(r"^[0-9A-F]{24}$")


class ArchiveStandbyAdapter(PgClusterAdapter):
    def __init__(self, settings) -> None:
        super().__init__(settings)
        base = os.environ["E2E_STANDBY_DIR"]
        self.archive_dir = os.path.join(base, "archive")
        self.state_dir = os.path.join(base, "state")
        self.gate_file = os.path.join(self.state_dir, "gate")
        self.unreachable_file = os.path.join(self.state_dir, "b_unreachable")
        self.promoted_file = os.path.join(self.state_dir, "b_promoted")
        self.paused_file = os.path.join(self.state_dir, "b_replay_paused")
        self.freeze_file = os.path.join(self.state_dir, "b_replay_freeze")

    # --- 标记文件 --- #
    def _unreachable(self) -> bool:
        return os.path.exists(self.unreachable_file)

    def _promoted(self) -> bool:
        return os.path.exists(self.promoted_file)

    def _paused(self) -> bool:
        return os.path.exists(self.paused_file)

    def _streaming(self) -> bool:
        try:
            with open(self.gate_file, "r", encoding="utf-8") as fh:
                return fh.read().strip() != "closed"
        except FileNotFoundError:
            return True

    def _archive_segments(self) -> list[str]:
        try:
            return sorted(
                n for n in os.listdir(self.archive_dir) if SEGMENT_RE.match(n)
            )
        except FileNotFoundError:
            return []

    def _receive_lsn(self) -> Optional[str]:
        """归档中最大完整段的段末 LSN（该位置之前的 WAL 均已在备库磁盘）。"""
        segments = self._archive_segments()
        if not segments:
            return None
        seg_no = int(segments[-1][8:], 16)
        end = (seg_no + 1) * SEGMENT_SIZE_LSN
        return f"{end >> 32:X}/{end & 0xFFFFFFFF:08X}"

    # --- 角色判定：B 不接受 SQL，全部由垫片根据标记回答 --- #
    def is_in_recovery(self, node: str) -> bool:
        if node != "B":
            return super().is_in_recovery(node)
        if self._unreachable():
            raise psycopg.OperationalError("E2E 模拟：备库 B 不可达")
        return not self._promoted()

    def standby_wal_snapshot(self, node: str) -> Optional[Dict[str, Any]]:
        if node != "B":
            return super().standby_wal_snapshot(node)
        if self._unreachable():
            return None
        if self._promoted():
            return {"in_recovery": False}
        receive_lsn = self._receive_lsn()
        if self._paused():
            # 暂停期间回放位置冻结在暂停时刻（驱动把当时的 LSN 写入文件）。
            try:
                with open(self.freeze_file, "r", encoding="utf-8") as fh:
                    replay_lsn = fh.read().strip()
            except FileNotFoundError:
                replay_lsn = receive_lsn
        else:
            replay_lsn = receive_lsn
        return {
            "in_recovery": True,
            "receive_lsn": receive_lsn,
            "replay_lsn": replay_lsn,
            "replay_paused": self._paused(),
            "streaming": self._streaming(),
        }
