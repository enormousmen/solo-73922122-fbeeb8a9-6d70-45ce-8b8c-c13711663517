"""切换状态机与原子持久化（纯标准库，可离线单测）。

状态流转::

    NORMAL --(开始切换)--> SWITCH_PENDING
    SWITCH_PENDING --(备库追上并提升成功)--> PROMOTED
    SWITCH_PENDING --(回放等待超时)--> ROLLBACK -> NORMAL
    PROMOTED --(提升完成/启动核对后收尾)--> NORMAL（leader 已切换）

所有落盘操作走「写临时文件 + fsync + os.replace + fsync 目录」，
保证即使进程在任意时刻被 kill，重启后读到的也是完整状态。
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

PHASE_NORMAL = "NORMAL"
PHASE_SWITCH_PENDING = "SWITCH_PENDING"
PHASE_PROMOTED = "PROMOTED"
PHASE_ROLLBACK = "ROLLBACK"

ALL_PHASES = (PHASE_NORMAL, PHASE_SWITCH_PENDING, PHASE_PROMOTED, PHASE_ROLLBACK)

# 切换结果
RESULT_PENDING = "PENDING"
RESULT_SUCCESS = "SUCCESS"
RESULT_TIMEOUT = "TIMEOUT"
RESULT_FAILED = "FAILED"


class SwitchError(RuntimeError):
    """切换流程中的可预期错误。"""


@dataclass
class SwitchRecord:
    started_at: float
    finished_at: Optional[float] = None
    result: str = RESULT_PENDING
    old_leader: str = ""
    new_leader: str = ""
    target_lsn: Optional[str] = None
    message: str = ""


@dataclass
class SwitchState:
    phase: str = PHASE_NORMAL
    leader: str = "A"
    target: Optional[str] = None
    target_lsn: Optional[str] = None
    lsn_finalized: bool = False
    deadline: Optional[float] = None
    current: Optional[SwitchRecord] = None
    history: List[SwitchRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "phase": self.phase,
            "leader": self.leader,
            "target": self.target,
            "target_lsn": self.target_lsn,
            "lsn_finalized": self.lsn_finalized,
            "deadline": self.deadline,
            "current": asdict(self.current) if self.current else None,
            "history": [asdict(h) for h in self.history],
        }

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "SwitchState":
        current = SwitchRecord(**raw["current"]) if raw.get("current") else None
        history = [SwitchRecord(**h) for h in raw.get("history", [])]
        return cls(
            phase=raw.get("phase", PHASE_NORMAL),
            leader=raw.get("leader", "A"),
            target=raw.get("target"),
            target_lsn=raw.get("target_lsn"),
            lsn_finalized=raw.get("lsn_finalized", False),
            deadline=raw.get("deadline"),
            current=current,
            history=history,
        )

    def public_view(self, now: Optional[float] = None) -> Dict[str, Any]:
        """给 HTTP 接口用的只读视图。"""
        remaining = None
        if self.phase == PHASE_SWITCH_PENDING and self.deadline is not None:
            remaining = max(0.0, round(self.deadline - (now or time.time()), 1))
        return {
            "phase": self.phase,
            "leader": self.leader,
            "target": self.target,
            "target_lsn": self.target_lsn,
            "lsn_finalized": self.lsn_finalized,
            "deadline_remaining_seconds": remaining,
            "current_switch": asdict(self.current) if self.current else None,
            "history": [asdict(h) for h in self.history[-10:]],
        }


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".switch-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_dir(directory)
    except BaseException:
        try:
            os.unlink(tmp)
        except FileNotFoundError:
            pass
        raise


class StateStore:
    """SwitchState 的持久化门面，所有写操作都原子落盘。"""

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> SwitchState:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                return SwitchState.from_dict(json.load(fh))
        except FileNotFoundError:
            state = SwitchState()
            self.save(state)
            return state

    def save(self, state: SwitchState) -> None:
        atomic_write_json(self.path, state.to_dict())

    def begin_switch(self, target: str, target_lsn: str, timeout: float,
                     now: Optional[float] = None) -> SwitchState:
        """开始一次切换；状态机非法转换直接拒绝。"""
        state = self.load()
        if state.phase != PHASE_NORMAL:
            raise SwitchError(
                f"当前阶段为 {state.phase}，不能发起新的切换"
            )
        if target == state.leader:
            raise SwitchError(f"节点 {target} 已经是写入主库，无需切换")
        ts = now if now is not None else time.time()
        state.current = SwitchRecord(
            started_at=ts,
            old_leader=state.leader,
            new_leader=target,
            target_lsn=target_lsn,
        )
        state.phase = PHASE_SWITCH_PENDING
        state.target = target
        state.target_lsn = target_lsn
        state.lsn_finalized = False
        state.deadline = ts + timeout
        self.save(state)
        return state

    def mark_promoted(self, state: SwitchState) -> None:
        if state.phase != PHASE_SWITCH_PENDING:
            raise SwitchError("只有 SWITCH_PENDING 才能进入 PROMOTED")
        state.phase = PHASE_PROMOTED
        self.save(state)

    def update_target_lsn(self, state: SwitchState, lsn: str) -> None:
        """围栏旧主并等写事务收尾后，用最终 LSN 替换受理时的初值。"""
        if state.phase != PHASE_SWITCH_PENDING:
            raise SwitchError("只有 SWITCH_PENDING 才能更新目标 LSN")
        state.target_lsn = lsn
        state.lsn_finalized = True
        if state.current is not None:
            state.current.target_lsn = lsn
        self.save(state)

    def finalize_success(self, state: SwitchState, now: Optional[float] = None) -> None:
        if state.target is None or state.current is None:
            raise SwitchError("缺少切换目标，无法完成收尾")
        state.leader = state.target
        state.current.result = RESULT_SUCCESS
        state.current.finished_at = now if now is not None else time.time()
        state.current.message = "备库已回放完旧主全部已提交事务并完成提升"
        state.history.append(state.current)
        state.current = None
        state.phase = PHASE_NORMAL
        state.target = None
        state.target_lsn = None
        state.lsn_finalized = False
        state.deadline = None
        self.save(state)

    def mark_rollback(self, state: SwitchState) -> None:
        if state.phase != PHASE_SWITCH_PENDING:
            raise SwitchError("只有 SWITCH_PENDING 才能进入 ROLLBACK")
        state.phase = PHASE_ROLLBACK
        self.save(state)

    def finalize_timeout(self, state: SwitchState, message: str,
                         now: Optional[float] = None) -> None:
        if state.current is None:
            raise SwitchError("缺少切换记录，无法回滚收尾")
        state.current.result = RESULT_TIMEOUT
        state.current.finished_at = now if now is not None else time.time()
        state.current.message = message
        state.history.append(state.current)
        state.current = None
        state.phase = PHASE_NORMAL
        state.target = None
        state.target_lsn = None
        state.lsn_finalized = False
        state.deadline = None
        self.save(state)

    def finalize_failure(self, state: SwitchState, message: str,
                         now: Optional[float] = None) -> None:
        if state.current is not None:
            state.current.result = RESULT_FAILED
            state.current.finished_at = now if now is not None else time.time()
            state.current.message = message
            state.history.append(state.current)
            state.current = None
        state.phase = PHASE_NORMAL
        state.target = None
        state.target_lsn = None
        state.lsn_finalized = False
        state.deadline = None
        self.save(state)
