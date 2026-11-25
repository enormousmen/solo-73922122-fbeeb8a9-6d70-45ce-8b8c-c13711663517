"""检修记录核验单：核验结果判定的纯逻辑（不 import psycopg，可离线单测）。

核验语义（对应需求）：

* 单号在主库提交成功后返回，绑定的 WAL 位置（LSN）已覆盖本次提交，
  绑定关系写入主库并经流复制复制到备库，后续录入与网页重启均不改变。
* 凭单号核验时，只要备库 B **仍是备库、可达、且已把该 LSN 落盘**
  （``pg_last_wal_receive_lsn``：已接收并 fsync 到磁盘的 WAL 位置），
  即使回放被暂停或与主库断流（已收到的部分不动），也给出确认。
* 三种异常分别给出原因：进度不足（BEHIND）、B 不可达（UNREACHABLE）、
  B 已提升（PROMOTED）。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional


class VerifyStatus(str, Enum):
    CONFIRMED = "CONFIRMED"          # B 仍是备库且绑定 LSN 已落盘
    BEHIND = "BEHIND"                # B 可达且是备库，但落盘进度不足
    UNREACHABLE = "UNREACHABLE"      # 无法连接 B
    PROMOTED = "PROMOTED"            # B 已提升为主库，不再是备库
    TICKET_NOT_FOUND = "TICKET_NOT_FOUND"  # 主库查无此单号


def lsn_int(lsn: str) -> int:
    """把 '0/16B7A48' 形式的 LSN 转成可比较的整数。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)


def wal_segment_name(lsn: str, timeline: int = 1, segment_size: int = 16 * 1024 * 1024) -> str:
    """由 LSN 推算所在 WAL 段文件名（默认时间线 1、段大小 16MB，与本演示一致）。

    纯展示用途，便于值班员在备库 ``pg_wal`` 目录中核对对应文件。
    """
    segment = lsn_int(lsn) // segment_size
    return f"{timeline:08X}{segment:016X}"


# 各状态面向值班员的中文说明
STATUS_MESSAGES: Dict[VerifyStatus, str] = {
    VerifyStatus.CONFIRMED: "备库 B 已将该核验单绑定的 WAL 位置落盘，核验通过",
    VerifyStatus.BEHIND: "备库 B 可达且仍为备库，但 WAL 落盘进度尚未覆盖该核验单绑定的位置，请稍后重试",
    VerifyStatus.UNREACHABLE: "备库 B 当前不可达，无法核验落盘情况，请检查 B 与网络后重试",
    VerifyStatus.PROMOTED: "节点 B 已提升为主库，不再是备库，按规则不能再以备库身份出具落盘确认",
    VerifyStatus.TICKET_NOT_FOUND: "主库上查不到该核验单号，请确认单号是否输入正确",
}


@dataclass
class VerifyResult:
    status: VerifyStatus
    ticket_no: Optional[str]
    bound_lsn: Optional[str] = None
    standby_receive_lsn: Optional[str] = None
    standby_replay_lsn: Optional[str] = None
    replay_paused: Optional[bool] = None
    streaming: Optional[bool] = None
    message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "confirmed": self.status is VerifyStatus.CONFIRMED,
            "ticket_no": self.ticket_no,
            "bound_lsn": self.bound_lsn,
            "standby_receive_lsn": self.standby_receive_lsn,
            "standby_replay_lsn": self.standby_replay_lsn,
            "replay_paused": self.replay_paused,
            "streaming": self.streaming,
            "message": self.message or STATUS_MESSAGES[self.status],
        }


def judge_verification(
    ticket_no: str,
    bound_lsn: Optional[str],
    standby_snapshot: Optional[Dict[str, Any]],
) -> VerifyResult:
    """根据主库绑定信息与备库快照判定核验结果。

    ``standby_snapshot`` 由数据库适配器取自 B，字段：

    * ``in_recovery``        B 是否仍处于 recovery（False 即已提升）
    * ``receive_lsn``        ``pg_last_wal_receive_lsn()``（已 fsync 落盘）
    * ``replay_lsn``         ``pg_last_wal_replay_lsn()``
    * ``replay_paused``      ``pg_is_wal_replay_paused()``
    * ``streaming``          WAL 接收进程是否在连（``pg_stat_wal_receiver``）

    快照为 ``None`` 表示 B 不可达。
    """
    if bound_lsn is None:
        return VerifyResult(
            status=VerifyStatus.TICKET_NOT_FOUND,
            ticket_no=ticket_no,
        )
    if standby_snapshot is None:
        return VerifyResult(
            status=VerifyStatus.UNREACHABLE,
            ticket_no=ticket_no,
            bound_lsn=bound_lsn,
        )
    if not standby_snapshot.get("in_recovery", False):
        return VerifyResult(
            status=VerifyStatus.PROMOTED,
            ticket_no=ticket_no,
            bound_lsn=bound_lsn,
        )

    receive_lsn = standby_snapshot.get("receive_lsn")
    replay_lsn = standby_snapshot.get("replay_lsn")
    paused = standby_snapshot.get("replay_paused")
    streaming = standby_snapshot.get("streaming")

    # receive_lsn 为 NULL：备库刚启动、一个 WAL 字节都还没收到，必然进度不足。
    covered = receive_lsn is not None and lsn_int(receive_lsn) >= lsn_int(bound_lsn)
    status = VerifyStatus.CONFIRMED if covered else VerifyStatus.BEHIND
    message = STATUS_MESSAGES[status]
    if covered and paused:
        message += "（当前回放已暂停：确认依据是 WAL 已落盘，不要求回放完成）"
    if covered and streaming is False:
        message += "（当前与主库断流：确认只依据此前已落盘的 WAL，不受影响）"

    return VerifyResult(
        status=status,
        ticket_no=ticket_no,
        bound_lsn=bound_lsn,
        standby_receive_lsn=receive_lsn,
        standby_replay_lsn=replay_lsn,
        replay_paused=bool(paused) if paused is not None else None,
        streaming=bool(streaming) if streaming is not None else None,
        message=message,
    )
