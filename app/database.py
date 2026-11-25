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

# 检修记录核验单：单号主键，wal_lsn 为提交后锁定的绑定 WAL 位置（pg_lsn 可直接比较）。
# 绑定行只在创建时写一次，之后没有任何 UPDATE/DELETE 路径，保证不可变性。
CREATE_MAINTENANCE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS maintenance_records (
    ticket_no   TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    wal_lsn     PG_LSN NOT NULL,
    wal_segment TEXT NOT NULL,
    written_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""
CREATE_MAINTENANCE_SEQ_SQL = (
    "CREATE SEQUENCE IF NOT EXISTS maintenance_ticket_seq"
)


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
        # DDL 与授权统一走管理员连接：容器内表由初始化脚本以超级用户创建，
        # 只有属主/超级用户能 GRANT；本地与容器行为因此保持一致。
        # DDL 经流复制物理复制到备库。
        leader = self.settings.node_a.name  # 仅启动阶段调用，初始主固定为 A
        app_user = self._nodes[leader].user
        with self._connect(leader, admin=True) as conn, conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            cur.execute(CREATE_MAINTENANCE_SEQ_SQL)
            cur.execute(CREATE_MAINTENANCE_TABLE_SQL)
            cur.execute(
                "GRANT SELECT, INSERT ON demo_records TO " + app_user
            )
            cur.execute(
                "GRANT ALL PRIVILEGES ON SEQUENCE demo_records_id_seq TO " + app_user
            )
            # maintenance_records 的 UPDATE 仅用于录入时一次性回写绑定 LSN，
            # 不存在任何按单号改写既有行的接口路径。
            cur.execute(
                "GRANT SELECT, INSERT, UPDATE ON maintenance_records TO " + app_user
            )
            # 序列权限没有 USAGE, ALL 的写法：ALL PRIVILEGES 等价
            # USAGE/SELECT/UPDATE 全授予，nextval() 需要 USAGE。
            cur.execute(
                "GRANT ALL PRIVILEGES ON SEQUENCE maintenance_ticket_seq TO "
                + app_user
            )

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

    # --- 检修记录核验单 ------------------------------------------------ #
    def create_maintenance_ticket(self, node: str, content: str) -> Dict[str, Any]:
        """在主库录入检修记录并返回核验单号与绑定的 WAL 位置。

        分两个事务，严格保证绑定 LSN 覆盖本次提交：

        1. 事务一：插入检修记录（含取号），提交即持久化；
        2. 事务二：提交后读取 ``pg_current_wal_lsn()``。该函数返回事务日志
           写入的当前位置，必然不早于事务一提交记录（含 commit WAL 记录）
           的位置，因此绑定 LSN 一定覆盖本次提交，并一次性回写该行。

        回写只发生在本次录入流程内；没有任何按单号改写或删除既有行的
        接口，因此后续录入与网页重启都不会改变已返回单号的绑定位置。
        """
        with self._connect(node) as conn, conn.cursor() as cur:
            # 事务一：取号 + 插入 + 提交
            cur.execute("SELECT nextval('maintenance_ticket_seq')")
            seq = int(cur.fetchone()[0])
            ticket_no = "MH" + time.strftime("%Y%m%d") + f"{seq:06d}"
            cur.execute(
                "INSERT INTO maintenance_records "
                "(ticket_no, content, wal_lsn, wal_segment, written_by) "
                "VALUES (%s, %s, '0/0'::pg_lsn, %s, %s)",
                (ticket_no, content, "", f"node-{node}"),
            )
            conn.commit()

            # 事务二：提交完成后的当前 WAL 位置，回写绑定（本事务自身的提交
            # 在该位置之后，不影响“绑定位置覆盖检修记录提交”的保证）。
            cur.execute("SELECT pg_current_wal_lsn()::text")
            bound_lsn = str(cur.fetchone()[0])
            # 段文件名直接问数据库，保证时间线/段大小与实例一致。
            cur.execute("SELECT pg_walfile_name(%s::pg_lsn)", (bound_lsn,))
            segment = str(cur.fetchone()[0])
            cur.execute(
                "UPDATE maintenance_records SET wal_lsn = %s::pg_lsn, "
                "wal_segment = %s WHERE ticket_no = %s",
                (bound_lsn, segment, ticket_no),
            )
            conn.commit()

            cur.execute(
                "SELECT ticket_no, content, wal_lsn::text, wal_segment, "
                "written_by, created_at FROM maintenance_records WHERE ticket_no = %s",
                (ticket_no,),
            )
            row = cur.fetchone()
        return {
            "ticket_no": row[0],
            "content": row[1],
            "wal_lsn": row[2],
            "wal_segment": row[3],
            "written_by": row[4],
            "created_at": row[5].isoformat(),
            "node": node,
        }

    def get_maintenance_ticket(self, node: str, ticket_no: str) -> Optional[Dict[str, Any]]:
        """从指定节点（主库，绑定关系的权威来源）读取核验单绑定信息。"""
        with self._connect(node) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT ticket_no, content, wal_lsn::text, wal_segment, "
                "written_by, created_at FROM maintenance_records WHERE ticket_no = %s",
                (ticket_no,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return {
            "ticket_no": row[0],
            "content": row[1],
            "wal_lsn": row[2],
            "wal_segment": row[3],
            "written_by": row[4],
            "created_at": row[5].isoformat(),
            "node": node,
        }

    def standby_wal_snapshot(self, node: str) -> Optional[Dict[str, Any]]:
        """读取备库 B 的落盘/回放快照；连接失败返回 None（即 B 不可达）。

        核验依据是 ``pg_last_wal_receive_lsn()``：备库通过流复制接收到的
        WAL 已同步（fsync）到磁盘的最后位置——即使回放暂停或已与主库断流，
        该位置之前的 WAL 都已落盘，不会回退。
        """
        try:
            with self._connect(node, admin=True) as conn, conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery()")
                in_recovery = bool(cur.fetchone()[0])
                if not in_recovery:
                    return {"in_recovery": False}
                cur.execute(
                    "SELECT pg_last_wal_receive_lsn()::text, "
                    "pg_last_wal_replay_lsn()::text, "
                    "pg_is_wal_replay_paused(), "
                    # 断流后 walreceiver 进程仍在重试，视图行还在，
                    # 只有 status='streaming' 才表示复制连接真正连通。
                    "COALESCE((SELECT status = 'streaming' "
                    "          FROM pg_stat_wal_receiver), false)"
                )
                receive_lsn, replay_lsn, paused, receiver_up = cur.fetchone()
                return {
                    "in_recovery": True,
                    "receive_lsn": str(receive_lsn) if receive_lsn is not None else None,
                    "replay_lsn": str(replay_lsn) if replay_lsn is not None else None,
                    "replay_paused": bool(paused),
                    "streaming": bool(receiver_up),
                }
        except Exception as exc:  # 连接拒绝/超时/主机不可达均归为不可达
            logger.warning("读取备库 %s 快照失败，视为不可达：%r", node, exc)
            return None
