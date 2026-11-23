"""FastAPI 入口：状态/记录接口、切换受理、回放控制和演示页面。"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import Settings
from .database import PgClusterAdapter
from .state import PHASE_NORMAL, StateStore, SwitchError
from .switcher import Switcher

logger = logging.getLogger("switch-demo")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


class RecordIn(BaseModel):
    content: str = Field(min_length=1, max_length=500)


class SwitchIn(BaseModel):
    target: Optional[str] = None
    timeout: Optional[float] = Field(default=None, ge=1.0, le=600.0)


class ReplayIn(BaseModel):
    node: Optional[str] = None


def _make_lifespan(settings: Settings, store: StateStore,
                   adapter: PgClusterAdapter, switcher: Switcher,
                   wake: threading.Event):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 1. 以数据库实际角色为准核对状态（覆盖“提升后、更新目标前进程退出”）。
        try:
            switcher.reconcile_on_startup()
        except Exception as exc:
            logger.exception("启动核对失败：%r", exc)
        # 2. 等主库可达后建表（DDL 经流复制到备库）。
        for attempt in range(60):
            try:
                adapter.ensure_schema()
                break
            except Exception as exc:
                logger.info("等待主库就绪以建表（%s/60）：%r", attempt + 1, exc)
                wake.wait(2.0)

        def worker() -> None:
            # 用 Event 等待而不是 time.sleep：切换受理、回放恢复后可立即唤醒推进。
            while True:
                try:
                    state = store.load()
                    if state.phase != PHASE_NORMAL:
                        switcher.run_once()
                except Exception as exc:
                    logger.exception("切换工作线程出错：%r", exc)
                wake.wait(timeout=max(0.5, settings.poll_interval))
                wake.clear()

        if settings.background_worker:
            thread = threading.Thread(target=worker, name="switch-worker", daemon=True)
            thread.start()
        logger.info("应用启动完成，当前状态：%s", store.load().public_view())
        yield

    return lifespan


def create_app(
    settings: Optional[Settings] = None,
    adapter: Optional[PgClusterAdapter] = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    store = StateStore(settings.state_file)
    adapter = adapter or PgClusterAdapter(settings)
    switcher = Switcher(
        store,
        adapter,
        poll_interval=settings.poll_interval,
        promote_wait_seconds=settings.promote_wait_seconds,
    )

    wake = threading.Event()
    app = FastAPI(title="PostgreSQL 主备切换演示", lifespan=_make_lifespan(settings, store, adapter, switcher, wake))
    app.state.settings = settings
    app.state.store = store
    app.state.adapter = adapter
    app.state.switcher = switcher
    switch_lock = threading.Lock()

    # ---------------- 页面 ---------------- #
    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        html_path = Path(__file__).parent / "static" / "index.html"
        return HTMLResponse(html_path.read_text(encoding="utf-8"))

    # ---------------- 状态 ---------------- #
    @app.get("/api/status")
    def status() -> dict:
        state = store.load()
        nodes = [adapter.node_status(name) for name in ("A", "B")]
        return {
            "cluster": state.public_view(),
            "nodes": nodes,
        }

    # ---------------- 示例记录 ---------------- #
    @app.post("/api/records", status_code=201)
    def create_record(payload: RecordIn) -> dict:
        state = store.load()
        if state.phase != PHASE_NORMAL:
            # 切换受理后新写入一律提示稍后重试；旧主也已被置只读做双保险。
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "SWITCH_IN_PROGRESS",
                    "message": "主备切换进行中，写入已暂停，请稍后重试",
                    "phase": state.phase,
                },
                headers={"Retry-After": "5"},
            )
        try:
            return adapter.insert_record(state.leader, payload.content)
        except Exception as exc:
            # SQLSTATE 25006 = read_only_sql_transaction，数据库层围栏生效。
            sqlstate = getattr(exc, "sqlstate", None)
            text = str(exc).strip()
            if sqlstate == "25006" or "read-only" in text or "read only" in text:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "LEADER_READ_ONLY",
                        "message": "当前主库处于只读（切换中），请稍后重试",
                    },
                    headers={"Retry-After": "5"},
                )
            logger.exception("写入失败")
            raise HTTPException(status_code=502, detail={"error": "DB_ERROR", "message": text})

    @app.get("/api/records")
    def get_records(node: Optional[str] = None, limit: int = 50) -> dict:
        state = store.load()
        target = node or state.leader
        if target not in ("A", "B"):
            raise HTTPException(400, detail={"error": "BAD_NODE", "message": "node 只能是 A 或 B"})
        try:
            rows = adapter.list_records(target, limit=min(max(1, limit), 200))
        except Exception as exc:
            raise HTTPException(502, detail={"error": "DB_ERROR", "message": str(exc).strip()})
        return {"node": target, "count": len(rows), "records": rows}

    # ---------------- 单向切换 ---------------- #
    @app.post("/api/switch")
    def start_switch(payload: SwitchIn) -> dict:
        with switch_lock:
            state = store.load()
            if state.phase != PHASE_NORMAL:
                raise HTTPException(
                    409,
                    detail={
                        "error": "SWITCH_IN_PROGRESS",
                        "message": f"已有切换进行中（{state.phase}）",
                    },
                )
            target = payload.target or ("B" if state.leader == "A" else "A")
            if target not in ("A", "B"):
                raise HTTPException(400, detail={"error": "BAD_NODE", "message": "target 只能是 A 或 B"})
            timeout = payload.timeout or settings.switch_timeout
            try:
                new_state = switcher.start_switch(target, timeout)
            except SwitchError as exc:
                raise HTTPException(409, detail={"error": "SWITCH_REJECTED", "message": str(exc)})
            wake.set()
            return {
                "accepted": True,
                "message": "切换已受理：新写入请稍后重试，旧主已置只读，等待备库回放后提升",
                "switch": new_state.public_view(),
            }

    # ---------------- 回放暂停/恢复（演示用） ---------------- #
    @app.post("/api/replay/pause")
    def pause_replay(payload: ReplayIn) -> dict:
        node = payload.node or "B"
        try:
            adapter.pause_replay(node)
        except SwitchError as exc:
            raise HTTPException(409, detail={"error": "NOT_STANDBY", "message": str(exc)})
        wake.set()
        return {"node": node, "replay_paused": True}

    @app.post("/api/replay/resume")
    def resume_replay(payload: ReplayIn) -> dict:
        node = payload.node or "B"
        try:
            adapter.resume_replay(node)
        except SwitchError as exc:
            raise HTTPException(409, detail={"error": "NOT_STANDBY", "message": str(exc)})
        wake.set()
        return {"node": node, "replay_paused": False}

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("WEB_PORT", "8000")),
    )
