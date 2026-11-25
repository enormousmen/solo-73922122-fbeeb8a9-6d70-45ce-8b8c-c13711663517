#!/usr/bin/env python3
"""端到端自测：真实 PostgreSQL 主库 + 门控归档恢复备库，跑全部核验场景。

用法（先确保 dev/local_cluster.sh up 与 dev/e2e/e2e_cluster.sh 已启动）：
    python3 -P dev/e2e/e2e_run.py

覆盖场景（对应需求逐条）：
  S1 正常录入 -> 备库落盘确认；绑定 LSN 覆盖本次提交；后续录入不改旧单
  S2 暂停回放仍可确认；恢复回放后继续可查
  S3 断流期间录入（A 正常返回单号）；恢复后凭原单号确认
  S4 B 不可达：明确原因
  S5 B 已提升：明确原因
  S6 未知单号：404
  S7 网页“重启”（新应用实例 + 同一状态/数据库）：原单号确认不变
  S8 真实 HTTP（uvicorn 起服务）冒烟
"""
from __future__ import annotations

import os
import sys
import time

# 允许以 `python3 -P dev/e2e/e2e_run.py` 从仓库根运行
sys.path.insert(0, os.getcwd())

import psycopg  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.config import NodeConfig, Settings  # noqa: E402
from app.main import create_app  # noqa: E402
from dev.e2e.shim import ArchiveStandbyAdapter  # noqa: E402

PRIMARY_PORT = 55452
B = "/workspace/pg-e2e/b"
STATE_DIR = os.path.join(B, "state")
ARCHIVE = os.path.join(B, "archive")
STATE_FILE = os.path.join(B, "state", "web-switch.json")

PASS = "[PASS]"
FAIL = "[FAIL]"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"  {PASS if cond else FAIL} {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def settings() -> Settings:
    common = dict(user="app", password="apppass", dbname="appdb",
                  admin_user="postgres", admin_password="adminpass")
    return Settings(
        node_a=NodeConfig("A", "127.0.0.1", PRIMARY_PORT, **common),
        node_b=NodeConfig("B", "127.0.0.1", 55455, **common),
        state_file=STATE_FILE,
        switch_timeout=20.0, poll_interval=1.0,
        promote_wait_seconds=30, background_worker=False,
    )


def make_client() -> tuple[TestClient, ArchiveStandbyAdapter]:
    cfg = settings()
    adapter = ArchiveStandbyAdapter(cfg)
    app = create_app(cfg, adapter)
    client = TestClient(app)
    client.__enter__()
    return client, adapter


# --- 主库直接操作 --- #
def primary_dsn() -> str:
    return (f"host=127.0.0.1 port={PRIMARY_PORT} user=postgres "
            "password=adminpass dbname=appdb connect_timeout=5")


def switch_wal() -> None:
    with psycopg.connect(primary_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_switch_wal()")


def current_lsn() -> str:
    with psycopg.connect(primary_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_current_wal_lsn()::text")
        return str(cur.fetchone()[0])


def segments_covering(lsn: str) -> list[str]:
    with psycopg.connect(primary_dsn(), autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT pg_walfile_name(%s::pg_lsn)", (lsn,))
        return [str(cur.fetchone()[0])]


def wait_archive(segments: list[str], timeout: float = 30.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if all(os.path.exists(os.path.join(ARCHIVE, s)) for s in segments):
            return True
        time.sleep(0.5)
    return False


# --- 标记文件控制 --- #
def mark(name: str, on: bool) -> None:
    path = os.path.join(STATE_DIR, name)
    if on:
        open(path, "w").close()
    else:
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def set_gate(open_: bool) -> None:
    with open(os.path.join(STATE_DIR, "gate"), "w", encoding="utf-8") as fh:
        fh.write("open" if open_ else "closed")


def create_ticket(client: TestClient, content: str) -> dict:
    r = client.post("/api/maintenance", json={"content": content})
    assert r.status_code == 201, r.text
    switch_wal()  # 让绑定 LSN 所在段尽快完整，供归档管道搬运
    return r.json()


def verify(client: TestClient, ticket_no: str) -> dict:
    r = client.post("/api/maintenance/verify", json={"ticket_no": ticket_no})
    assert r.status_code == 200, r.text
    return r.json()


def main() -> int:
    os.environ["E2E_STANDBY_DIR"] = B
    # 初始状态：门开、无故障标记
    set_gate(True)
    for m in ("b_unreachable", "b_promoted", "b_replay_paused"):
        mark(m, False)

    client, adapter = make_client()

    print("S1 正常录入、绑定 LSN 覆盖提交、备库落盘确认、后续录入不改旧单")
    t1 = create_ticket(client, "1号机组例行检修")
    bound1 = t1["wal_lsn"]
    print(f"    单号={t1['ticket_no']} 绑定 LSN={bound1} 段={t1['wal_segment']}")
    check("单号格式 MH+日期+序号", t1["ticket_no"].startswith("MH"))
    # 覆盖性：绑定 LSN 不晚于主库当前 WAL 位置（提交 WAL 已在其之前）
    from app.verification import lsn_int
    check("绑定 LSN 数值不超过当前 LSN（覆盖本次提交）",
          lsn_int(bound1) <= lsn_int(current_lsn()),
          f"bound={bound1} current={current_lsn()}")
    # 进度不足先可复现（此时新段可能还没归档完）
    seg = segments_covering(bound1)
    if not wait_archive(seg):
        check("绑定段归档到备库", False, f"缺少 {seg}")
    else:
        check("绑定段归档到备库", True)
    v = verify(client, t1["ticket_no"])
    check("归档后核验 CONFIRMED", v["status"] == "CONFIRMED", v.get("message", ""))
    check("备库落盘位置覆盖绑定 LSN",
          v["standby_receive_lsn"] is not None
          and lsn_int(v["standby_receive_lsn"]) >= lsn_int(bound1))
    # 真实落盘证据：备库归档目录里确有该 WAL 段文件
    check("备库磁盘存在该 WAL 段", os.path.exists(os.path.join(ARCHIVE, seg[0])))
    # 后续录入不改变旧单
    t2 = create_ticket(client, "2号机组消缺")
    detail = client.get(f"/api/maintenance/{t1['ticket_no']}").json()
    check("后续录入不改变旧单绑定 LSN", detail["wal_lsn"] == bound1)
    check("新单号不同于旧单号", t2["ticket_no"] != t1["ticket_no"])

    print("S2 暂停回放：已落盘仍确认，且页面能看出回放暂停")
    # 记录暂停时刻的落盘位置作为冻结回放点
    snap = adapter.standby_wal_snapshot("B")
    with open(os.path.join(STATE_DIR, "b_replay_freeze"), "w", encoding="utf-8") as fh:
        fh.write(snap["receive_lsn"])
    mark("b_replay_paused", True)
    v = verify(client, t1["ticket_no"])
    check("暂停回放仍 CONFIRMED", v["status"] == "CONFIRMED", v.get("message", ""))
    check("返回 replay_paused=true", v["replay_paused"] is True)
    check("提示语说明回放暂停不影响确认", "回放已暂停" in v["message"])
    # 暂停期间主库继续写入（模拟真实回放暂停时主库仍前进）
    t3 = create_ticket(client, "暂停期间的检修")
    v3 = verify(client, t3["ticket_no"])
    check("暂停期间新单进度不足 BEHIND", v3["status"] == "BEHIND", v3.get("message", ""))
    # 恢复回放
    mark("b_replay_paused", False)
    assert wait_archive(segments_covering(t3["wal_lsn"]))
    v3 = verify(client, t3["ticket_no"])
    check("恢复后新单 CONFIRMED", v3["status"] == "CONFIRMED")
    v = verify(client, t1["ticket_no"])
    check("恢复后旧单仍 CONFIRMED", v["status"] == "CONFIRMED")

    print("S3 断流：断流期间 A 可录入并返回单号；恢复后凭原单号确认")
    set_gate(False)  # 关归档门 = WAL 流不到备库（断流）
    t4 = create_ticket(client, "断流期间录入的检修")
    print(f"    断流期间单号={t4['ticket_no']} LSN={t4['wal_lsn']}")
    check("断流时 A 正常返回单号", t4["ticket_no"].startswith("MH"))
    # 等两秒确认门关闭期间该段确实进不了归档
    time.sleep(2.0)
    blocked_seg = segments_covering(t4["wal_lsn"])[0]
    check("断流期间新 WAL 段未到备库", not os.path.exists(os.path.join(ARCHIVE, blocked_seg)))
    v4 = verify(client, t4["ticket_no"])
    check("断流期间核验为 BEHIND 且 streaming=false",
          v4["status"] == "BEHIND" and v4["streaming"] is False, v4.get("message", ""))
    set_gate(True)  # 恢复流
    check("恢复后原单号 CONFIRMED",
          wait_archive([blocked_seg]) and verify(client, t4["ticket_no"])["status"] == "CONFIRMED")
    check("恢复后 streaming=true", verify(client, t4["ticket_no"])["streaming"] is True)

    print("S4 B 不可达：明确原因")
    mark("b_unreachable", True)
    v = verify(client, t1["ticket_no"])
    check("不可达返回 UNREACHABLE", v["status"] == "UNREACHABLE", v.get("message", ""))
    check("说明文字含不可达", "不可达" in v["message"])
    mark("b_unreachable", False)
    check("恢复后重新 CONFIRMED", verify(client, t1["ticket_no"])["status"] == "CONFIRMED")

    print("S5 B 已提升：明确原因")
    mark("b_promoted", True)
    v = verify(client, t1["ticket_no"])
    check("已提升返回 PROMOTED", v["status"] == "PROMOTED", v.get("message", ""))
    check("说明文字含已提升", "提升" in v["message"])
    mark("b_promoted", False)

    print("S6 未知单号")
    r = client.post("/api/maintenance/verify", json={"ticket_no": "MH-NOPE"})
    check("未知单号 HTTP 404", r.status_code == 404)
    check("错误码 TICKET_NOT_FOUND", r.json()["detail"]["error"] == "TICKET_NOT_FOUND")

    print("S7 网页重启：新应用实例、同一数据库与状态目录，原单号确认不变")
    client.__exit__(None, None, None)
    client2, _ = make_client()
    v = verify(client2, t1["ticket_no"])
    check("重启后原单号仍 CONFIRMED 且绑定 LSN 不变",
          v["status"] == "CONFIRMED" and v["bound_lsn"] == bound1)
    # 再录一单验证重启后写入链路正常
    t5 = create_ticket(client2, "重启后的检修")
    check("重启后可继续录入", t5["ticket_no"].startswith("MH"))
    assert wait_archive(segments_covering(t5["wal_lsn"]))
    check("重启后新单可确认", verify(client2, t5["ticket_no"])["status"] == "CONFIRMED")

    print("S8 真实 HTTP 冒烟（uvicorn 起服务，请求后自行关闭）")
    import threading
    import uvicorn
    from app.main import create_app as _create

    cfg = settings()
    http_app = _create(cfg, ArchiveStandbyAdapter(cfg))
    # 用内核分配的空闲端口，避免固定端口被前次运行残留占用（沙箱内无法 kill）。
    import socket as _socket
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        http_port = probe.getsockname()[1]
    config = uvicorn.Config(http_app, host="127.0.0.1", port=http_port,
                            log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    import urllib.request
    import json as _json
    ok = False
    body = ""
    last_err = ""
    for _ in range(40):
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{http_port}/api/maintenance/verify",
                data=_json.dumps({"ticket_no": t1["ticket_no"]}).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=3) as resp:
                body = resp.read().decode()
                ok = resp.status == 200 and '"CONFIRMED"' in body
                break
        except Exception as exc:
            last_err = repr(exc)
            time.sleep(0.5)
    check("HTTP 核验接口返回 CONFIRMED", ok, (body[:200] or last_err))
    server.should_exit = True
    thread.join(timeout=10)

    print()
    if failures:
        print(f"{FAIL} 失败 {len(failures)} 项：{failures}")
        return 1
    print(f"{PASS} 全部 E2E 场景通过")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
