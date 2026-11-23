"""环境变量配置。仅依赖标准库，避免引入额外设置框架。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NodeConfig:
    name: str
    host: str
    port: int
    user: str
    password: str
    dbname: str
    admin_user: str
    admin_password: str

    def dsn(self, admin: bool = False) -> str:
        user = self.admin_user if admin else self.user
        password = self.admin_password if admin else self.password
        return (
            f"host={self.host} port={self.port} user={user} "
            f"password={password} dbname={self.dbname} "
            "connect_timeout=5 application_name=switch-demo"
        )


@dataclass(frozen=True)
class Settings:
    node_a: NodeConfig
    node_b: NodeConfig
    state_file: str
    switch_timeout: float
    poll_interval: float
    promote_wait_seconds: int
    background_worker: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        common_user = os.getenv("PG_APP_USER", "app")
        common_password = os.getenv("PG_APP_PASSWORD", "apppass")
        common_db = os.getenv("PG_APP_DB", "appdb")
        admin_user = os.getenv("PG_ADMIN_USER", "postgres")
        admin_password = os.getenv("PG_ADMIN_PASSWORD", "postgres-pass")

        def make_node(name: str, host_env: str, default_host: str) -> NodeConfig:
            return NodeConfig(
                name=name,
                host=os.getenv(host_env, default_host),
                port=int(os.getenv(f"PG_{name}_PORT", "5432")),
                user=os.getenv(f"PG_{name}_USER", common_user),
                password=os.getenv(f"PG_{name}_PASSWORD", common_password),
                dbname=os.getenv(f"PG_{name}_DB", common_db),
                admin_user=os.getenv(f"PG_{name}_ADMIN_USER", admin_user),
                admin_password=os.getenv(f"PG_{name}_ADMIN_PASSWORD", admin_password),
            )

        node_a = make_node("A", "PG_A_HOST", "primary")
        node_b = make_node("B", "PG_B_HOST", "standby")
        return cls(
            node_a=node_a,
            node_b=node_b,
            state_file=os.getenv("STATE_FILE", "/data/state/switch.json"),
            switch_timeout=float(os.getenv("SWITCH_TIMEOUT", "20")),
            poll_interval=float(os.getenv("POLL_INTERVAL", "1")),
            promote_wait_seconds=int(os.getenv("PROMOTE_WAIT_SECONDS", "30")),
            background_worker=os.getenv("ENABLE_BACKGROUND_WORKER", "1") not in ("0", "false", "False"),
        )
