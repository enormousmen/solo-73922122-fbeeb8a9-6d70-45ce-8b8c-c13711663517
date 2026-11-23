"""PostgreSQL 适配器与示例记录表的数据访问。

连接一律短连接（每次操作新建），切换期拓扑变化时不会持有失效长连接。
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import psycopg

from .config import NodeConfig, Settings
from .state import SwitchError

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS demo_records (
    id          BIGSERIAL PRIMARY KEY,
    content     TEXT NOT NULL,
    written_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


class PgClusterAdapter:
    """实现 switcher.ClusterAdapter 协议的真实 PostgreSQL 操作集合。"""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._nodes: Dict[str, NodeConfig] = {
            settings.node_a.name: settings.node_a,
            settings.node_b.name: settings.node_b,
        }

    def _dsn(self, node: str, admin: bool = False) -> str:
        try:
            return self._nodes[node].dsn(admin=admin)
        except KeyError as exc:
            raise SwitchError(f"未知节点 {node}") from exc

    def _connect(self, node: str, admin: bool = False) -> psycopg.Connection:
        return psycopg.connect(self._dsn(node, admin=admin), autocommit=True)

    # --- ClusterAdapter 协议实现 -------------------------------------- #
    def is_in_recovery(self, node: str) -> bool:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            return bool(cur.fetchone()[0])

    def is_fenced_readonly(self, node: str) -> bool:
        """非 recovery 节点是否被 default_transaction_read_only 围栏。"""
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT pg_is_in_recovery(), current_setting('default_transaction_read_only')"
            )
            in_recovery, setting = cur.fetchone()
            return (not in_recovery) and setting == "on"

    def current_wal_lsn(self, node: str) -> str:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            return str(cur.fetchone()[0])

    def replayed_wal_lsn(self, node: str) -> str:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_last_wal_replay_lsn()::text")
            value = cur.fetchone()[0]
            if value is None:
                # 备库刚启动、尚未回放任何 WAL 时返回 NULL，视作 0/0。
                return "0/0"
            return str(value)

    def set_read_only(self, node: str, read_only: bool) -> None:
        # ALTER SYSTEM 需超级用户，走 admin 连接。
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            if cur.fetchone()[0]:
                logger.info("节点 %s 是备库，无需设置只读", node)
                return
            # ALTER SYSTEM SET 不支持绑定参数，用白名单字面量杜绝注入。
            value = "on" if read_only else "off"
            cur.execute(
                "ALTER SYSTEM SET default_transaction_read_only = " + value
            )
            cur.execute("SELECT pg_reload_conf()")
            logger.info("节点 %s default_transaction_read_only=%s 并已 reload",
                        node, "on" if read_only else "off")

    def wait_for_wal_quiesce(self, node: str, deadline: float,
                             poll_interval: float) -> Optional[str]:
        """旧主围栏后，等已接单的写事务全部收尾。

        判定标准：没有其他客户端后端处于 active 且在事务中。
        若仍有写事务在执行返回 None（由编排器下轮再查）；
        收尾后返回当前最终 WAL LSN。
        """
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_current_wal_lsn()::text,
                       EXISTS (
                           SELECT 1 FROM pg_stat_activity
                           WHERE pid <> pg_backend_pid()
                             AND backend_type = 'client backend'
                             AND state = 'active'
                             AND xact_start IS NOT NULL
                       )
                """
            )
            lsn, busy = cur.fetchone()
            if busy:
                logger.info("旧主 %s 仍有写事务在收尾，暂不锁定最终 LSN", node)
                return None
            return str(lsn)

    def promote(self, node: str) -> None:
        # pg_promote() 仅超级用户可执行，走 admin 连接。
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            if not cur.fetchone()[0]:
                logger.info("节点 %s 已经是主库，无需 promote", node)
                return
            cur.execute("SELECT pg_promote(wait => false)")
            logger.info("已对节点 %s 发起 pg_promote()", node)

    def wait_until_primary(self, node: str, timeout_seconds: int) -> None:
        deadline = time.time() + timeout_seconds
        last_error: Optional[Exception] = None
        while time.time() < deadline:
            try:
                if not self.is_in_recovery(node):
                    return
            except Exception as exc:  # 提升瞬间连接可能被重置，重试即可
                last_error = exc
            time.sleep(0.5)
        raise SwitchError(
            f"节点 {node} 在 {timeout_seconds}s 内未完成提升"
            + (f"：{last_error!r}" if last_error else "")
        )

    # --- 演示用：暂停/恢复备库 WAL 回放 -------------------------------- #
    def pause_replay(self, node: str) -> None:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            if not cur.fetchone()[0]:
                raise SwitchError(f"节点 {node} 不是备库，不能暂停回放")
            cur.execute("SELECT pg_wal_replay_pause()")
            logger.warning("节点 %s 的 WAL 回放已暂停", node)

    def resume_replay(self, node: str) -> None:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery()")
            if not cur.fetchone()[0]:
                raise SwitchError(f"节点 {node} 不是备库，不能恢复回放")
            cur.execute("SELECT pg_wal_replay_resume()")
            logger.info("节点 %s 的 WAL 回放已恢复", node)

    def is_replay_paused(self, node: str) -> Optional[bool]:
        with self._connect(node, admin=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_is_in_recovery(), pg_is_wal_replay_paused()")
            in_recovery, paused = cur.fetchone()
            return bool(paused) if in_recovery else None

    # --- 集群状态 ------------------------------------------------------ #
    def node_status(self, node: str) -> Dict[str, Any]:
        cfg = self._nodes[node]
        status: Dict[str, Any] = {
            "name": node,
            "host": cfg.host,
            "port": cfg.port,
            "reachable": False,
            "role": "unknown",
            "replay_paused": None,
            "wal_lsn": None,
            "replay_lsn": None,
        }
        try:
            with self._connect(node, admin=True) as conn, conn.cursor() as cur:
                status["reachable"] = True
                cur.execute("SELECT pg_is_in_recovery()")
                in_recovery = bool(cur.fetchone()[0])
                status["role"] = "standby" if in_recovery else "primary"
                if in_recovery:
                    cur.execute(
                        "SELECT pg_last_wal_replay_lsn()::text, "
                        "pg_is_wal_replay_paused()"
                    )
                    replay_lsn, paused = cur.fetchone()
                    status["replay_lsn"] = replay_lsn
                    status["replay_paused"] = bool(paused)
                    cur.execute("SELECT pg_last_wal_receive_lsn()::text")
                    status["wal_lsn"] = cur.fetchone()[0]
                else:
                    cur.execute("SELECT pg_current_wal_lsn()::text")
                    status["wal_lsn"] = cur.fetchone()[0]
                    cur.execute(
                        "SELECT setting FROM pg_settings "
                        "WHERE name='default_transaction_read_only'"
                    )
                    status["read_only"] = cur.fetchone()[0] == "on"
        except Exception as exc:
            status["error"] = str(exc).strip()
        return status

    def ensure_schema(self) -> None:
        # 建表需要建表权限：由初始化脚本 GRANT 给 app 角色；DDL 经流复制到备库。
        leader = self.settings.node_a.name  # 仅启动阶段调用，初始主固定为 A
        with self._connect(leader) as conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)

    # --- 示例记录读写 -------------------------------------------------- #
    def insert_record(self, node: str, content: str) -> Dict[str, Any]:
        with self._connect(node) as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO demo_records (content, written_by) "
                "VALUES (%s, %s) RETURNING id, content, written_by, created_at",
                (content, f"node-{node}"),
            )
            row = cur.fetchone()
        return {
            "id": row[0],
            "content": row[1],
            "written_by": row[2],
            "created_at": row[3].isoformat(),
            "node": node,
        }

    def list_records(self, node: str, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect(node) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, content, written_by, created_at "
                "FROM demo_records ORDER BY id DESC LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "content": r[1],
                "written_by": r[2],
                "created_at": r[3].isoformat(),
                "node": node,
            }
            for r in rows
        ]
