"""单向切换编排器。

编排器本身不 import psycopg，只依赖 :class:`ClusterAdapter` 协议，
因此可以用内存假适配器在没有数据库的环境里完整单测状态机。
真实适配器见 :mod:`app.database`。
"""
from __future__ import annotations

import logging
import time
from typing import Optional, Protocol

from .state import (
    PHASE_PROMOTED,
    PHASE_ROLLBACK,
    PHASE_SWITCH_PENDING,
    RESULT_FAILED,
    StateStore,
    SwitchError,
    SwitchState,
)

logger = logging.getLogger(__name__)


class ClusterAdapter(Protocol):
    """数据库集群需要提供的原子操作。"""

    def is_in_recovery(self, node: str) -> bool: ...

    def is_fenced_readonly(self, node: str) -> bool:
        """非 recovery 节点是否被 default_transaction_read_only 围栏。"""

    def current_wal_lsn(self, node: str) -> str: ...

    def replayed_wal_lsn(self, node: str) -> str: ...

    def set_read_only(self, node: str, read_only: bool) -> None:
        """对非 recovery 节点设置 default_transaction_read_only 并 reload。"""

    def wait_for_wal_quiesce(self, node: str, deadline: float,
                             poll_interval: float) -> str:
        """等旧主上所有写事务收尾，返回收尾后的最终 WAL LSN。"""

    def promote(self, node: str) -> None: ...

    def wait_until_primary(self, node: str, timeout_seconds: int) -> None:
        """轮询 pg_is_in_recovery()=f，超时抛异常。"""


def lsn_int(lsn: str) -> int:
    """把 '0/16B7A48' 形式的 LSN 转成可比较的整数。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)


class Switcher:
    def __init__(
        self,
        store: StateStore,
        adapter: ClusterAdapter,
        poll_interval: float = 1.0,
        promote_wait_seconds: int = 30,
        sleep=time.sleep,
        clock=time.time,
    ) -> None:
        self.store = store
        self.adapter = adapter
        self.poll_interval = poll_interval
        self.promote_wait_seconds = promote_wait_seconds
        self._sleep = sleep
        self._clock = clock

    # ------------------------------------------------------------------ #
    # 发起切换
    # ------------------------------------------------------------------ #
    def start_switch(self, target: str, timeout: float) -> SwitchState:
        state = self.store.load()
        old_leader = state.leader
        # 1. 先确认旧主当前可写，并拿到当前 WAL 位置作为初始追赶目标。
        if self.adapter.is_in_recovery(old_leader):
            raise SwitchError(f"旧主 {old_leader} 当前处于 recovery，无法发起切换")
        # 目标可达但不是备库才硬拒绝；目标暂时不可达不阻断受理，
        # 后续提升前仍有强校验，追不上会在超时后回滚。
        try:
            if not self.adapter.is_in_recovery(target):
                raise SwitchError(f"目标 {target} 当前不是备库，无法提升（可能已切换过）")
        except SwitchError:
            raise
        except Exception as exc:
            logger.warning("切换受理时目标 %s 暂不可达，继续受理并在推进阶段重试：%r", target, exc)
        initial_lsn = self.adapter.current_wal_lsn(old_leader)
        # 2. 先落盘 SWITCH_PENDING（含初始 LSN、截止时间），再拒绝新写入。
        state = self.store.begin_switch(target, initial_lsn, timeout, now=self._clock())
        # 3. 围栏旧主：只拦截新事务，已接单的写事务继续执行直至提交。
        self.adapter.set_read_only(old_leader, True)
        logger.info(
            "切换已受理：旧主 %s 置为只读，目标 %s，初始 LSN=%s，超时=%.1fs",
            old_leader, target, initial_lsn, timeout,
        )
        return state

    # ------------------------------------------------------------------ #
    # 后台 / 重启续跑：推进一次状态机
    # ------------------------------------------------------------------ #
    def run_once(self) -> Optional[SwitchState]:
        state = self.store.load()
        if state.phase == PHASE_SWITCH_PENDING:
            return self._advance_pending(state)
        if state.phase == PHASE_ROLLBACK:
            return self._finish_rollback(state)
        if state.phase == PHASE_PROMOTED:
            return self._finish_promotion(state)
        return None

    def _advance_pending(self, state: SwitchState) -> SwitchState:
        assert state.target is not None and state.target_lsn is not None
        target = state.target
        old_leader = state.current.old_leader if state.current else state.leader
        try:
            # 阶段一：等旧主已接单的写事务全部收尾，锁定最终目标 LSN。
            if not state.lsn_finalized:
                final_lsn = self.adapter.wait_for_wal_quiesce(
                    old_leader, state.deadline or 0, self.poll_interval
                )
                if final_lsn is None:
                    if self._clock() >= (state.deadline or 0):
                        return self._timeout_rollback(state)
                    logger.info("旧主仍有写事务在收尾，下轮继续等待")
                    self._sleep(self.poll_interval)
                    return state
                self.store.update_target_lsn(state, final_lsn)
                logger.info("旧主写事务已全部收尾，最终目标 LSN=%s", final_lsn)

            # 阶段二：备库必须仍处于 recovery，且回放位置追平最终目标 LSN。
            lsn = state.target_lsn
            if not self.adapter.is_in_recovery(target):
                # 不可恢复的拓扑异常：解除旧主围栏并失败收尾，避免状态机卡死。
                self.adapter.set_read_only(old_leader, False)
                self.store.finalize_failure(
                    state, f"目标节点 {target} 已不是备库，状态异常，已终止切换并恢复旧主写入",
                    now=self._clock(),
                )
                return state
            replayed = self.adapter.replayed_wal_lsn(target)
            if lsn_int(replayed) >= lsn_int(lsn):
                logger.info("备库 %s 已回放至 %s（目标 %s），开始提升", target, replayed, lsn)
                # 提升前状态已持久化；崩溃后重启由 reconcile 确认提升结果。
                self.adapter.promote(target)
                self.store.mark_promoted(state)
                return self._finish_promotion(state)
            if self._clock() >= (state.deadline or 0):
                return self._timeout_rollback(state)
            logger.info("等待备库回放：replayed=%s 目标=%s", replayed, lsn)
            self._sleep(self.poll_interval)
            # 睡眠后再判一次超时，保证暂停回放时不会无限等。
            if self._clock() >= (state.deadline or 0):
                return self._timeout_rollback(state)
            return state
        except SwitchError:
            raise
        except Exception as exc:  # 数据库通信类故障：超时则回滚，否则留给下一轮
            if self._clock() >= (state.deadline or 0):
                return self._timeout_rollback(state)
            logger.warning("切换推进出现临时故障，下轮重试：%r", exc)
            self._sleep(self.poll_interval)
            return state

    def _timeout_rollback(self, state: SwitchState) -> SwitchState:
        assert state.current is not None
        self.store.mark_rollback(state)
        # 恢复旧主写入，新写入立即重新可用。
        self.adapter.set_read_only(state.current.old_leader, False)
        self.store.finalize_timeout(
            state,
            "等待备库回放超时，已恢复旧主写入；恢复回放后可重新发起切换",
            now=self._clock(),
        )
        logger.warning("切换超时回滚完成，旧主 %s 恢复写入", state.leader)
        return state

    def _finish_rollback(self, state: SwitchState) -> SwitchState:
        # 崩溃发生在 mark_rollback 与 finalize 之间时的续跑路径。
        assert state.current is not None
        self.adapter.set_read_only(state.current.old_leader, False)
        self.store.finalize_timeout(
            state, "回滚续跑：已恢复旧主写入", now=self._clock()
        )
        return state

    def _finish_promotion(self, state: SwitchState) -> SwitchState:
        assert state.target is not None
        new_leader = state.target
        self.adapter.wait_until_primary(new_leader, self.promote_wait_seconds)
        # 双保险：新主可写。
        self.adapter.set_read_only(new_leader, False)
        self.store.finalize_success(state, now=self._clock())
        logger.info("切换完成，新主为 %s", new_leader)
        return state

    # ------------------------------------------------------------------ #
    # 启动核对：即使 Web 服务在提升后、更新写入目标前退出，
    # 重启后也以实际数据库角色为准，只写真正的主库。
    # ------------------------------------------------------------------ #
    def reconcile_on_startup(self) -> SwitchState:
        """启动核对：以数据库实际角色 + 只读围栏为准。

        备库提升后旧主在物理上仍是 ``pg_is_in_recovery()=f``，
        但已被 ``default_transaction_read_only`` 围栏挡住，属于“已下台旧主”
        而非脑裂。真正的脑裂是两个节点都可写。
        """
        state = self.store.load()
        roles: dict[str, dict[str, bool]] = {}
        for node in ("A", "B"):
            try:
                in_recovery = self.adapter.is_in_recovery(node)
                fenced = (not in_recovery) and self.adapter.is_fenced_readonly(node)
                roles[node] = {"in_recovery": in_recovery, "fenced": fenced}
            except Exception as exc:
                logger.warning("启动核对：节点 %s 不可达：%r", node, exc)

        def writable_primaries() -> list[str]:
            return [
                n for n, r in roles.items()
                if not r["in_recovery"] and not r["fenced"]
            ]

        if not roles:
            logger.warning("启动核对：两个节点均不可达，维持状态 phase=%s", state.phase)
            return state

        if state.phase in (PHASE_SWITCH_PENDING, PHASE_ROLLBACK, PHASE_PROMOTED):
            target = state.target
            old = state.current.old_leader if state.current else state.leader
            target_role = roles.get(target)
            old_role = roles.get(old)
            if target_role is not None and not target_role["in_recovery"]:
                # 目标已是主库。旧主若仍可写则是真脑裂，拒绝自动选择。
                if old_role is not None and not old_role["in_recovery"] and not old_role["fenced"]:
                    raise SwitchError(
                        f"目标 {target} 与旧主 {old} 同时可写，疑似脑裂，拒绝自动选择"
                    )
                logger.info("启动核对：目标 %s 已是主库，续完切换收尾", target)
                self.adapter.set_read_only(target, False)
                if state.phase != PHASE_PROMOTED:
                    self.store.mark_promoted(state)
                self.store.finalize_success(state, now=self._clock())
                return state
            if old_role is not None and not old_role["in_recovery"]:
                # 提升未发生，旧主仍在：按超时回滚恢复写入。
                logger.info("启动核对：旧主 %s 仍是主库，按超时回滚", old)
                self.adapter.set_read_only(old, False)
                if state.phase == PHASE_SWITCH_PENDING:
                    self.store.mark_rollback(state)
                self.store.finalize_timeout(
                    state, "服务重启后核对：备库未完成提升，恢复旧主写入",
                    now=self._clock(),
                )
                return state
            # 两个可达节点都在 recovery 或目标不可达：维持现状等待。
            logger.warning("启动核对：暂无明确主库，维持状态 phase=%s", state.phase)
            return state

        # NORMAL（或残留）阶段：只允许一个可写主库。
        primaries = writable_primaries()
        if len(primaries) > 1:
            raise SwitchError(f"出现两个可写主库 {primaries}，拒绝自动选择，请人工介入")
        if len(primaries) == 0:
            logger.warning("启动核对：没有可写主库，维持状态 phase=%s", state.phase)
            return state
        actual_leader = primaries[0]

        if state.leader != actual_leader:
            logger.info("启动核对：写入目标由 %s 纠正为 %s", state.leader, actual_leader)
            state.leader = actual_leader
            state.phase = "NORMAL"
            state.target = state.target_lsn = state.deadline = None
            if state.current is not None:
                state.current.result = RESULT_FAILED
                state.current.finished_at = self._clock()
                state.current.message = "启动核对发现状态残留，已按实际主库角色纠正"
                state.history.append(state.current)
                state.current = None
            self.store.save(state)
        else:
            self.adapter.set_read_only(actual_leader, False)
        return state
