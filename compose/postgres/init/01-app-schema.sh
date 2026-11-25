#!/bin/bash
# 仅在主库数据卷首次初始化时由官方 postgres 镜像执行一次。
# 创建：应用最小权限角色、流复制角色、演示表与授权。
set -euo pipefail

: "${PG_APP_USER:?missing PG_APP_USER}"
: "${PG_APP_PASSWORD:?missing PG_APP_PASSWORD}"
: "${PG_REPL_USER:?missing PG_REPL_USER}"
: "${PG_REPL_PASSWORD:?missing PG_REPL_PASSWORD}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
-- 流复制角色：仅授予 REPLICATION 登录权限，用于持续 WAL 流复制。
-- 注意：PG15 的 pg_basebackup 检查点信号要求超级用户，因此基础备份由
-- standby 入口脚本用超级用户发起，本角色不持有检查点/信号类权限。
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PG_REPL_USER}') THEN
    CREATE ROLE ${PG_REPL_USER} REPLICATION LOGIN PASSWORD '${PG_REPL_PASSWORD}';
  END IF;
END
\$\$;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
-- 应用角色：只对演示库/演示表有权限，不能做集群管理操作
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PG_APP_USER}') THEN
    CREATE ROLE ${PG_APP_USER} LOGIN PASSWORD '${PG_APP_PASSWORD}';
  END IF;
END
\$\$;

GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${PG_APP_USER};
GRANT USAGE, CREATE ON SCHEMA public TO ${PG_APP_USER};

CREATE TABLE IF NOT EXISTS demo_records (
    id          BIGSERIAL PRIMARY KEY,
    content     TEXT NOT NULL,
    written_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT ON demo_records TO ${PG_APP_USER};
GRANT ALL PRIVILEGES ON SEQUENCE demo_records_id_seq TO ${PG_APP_USER};

-- 检修记录核验单：单号主键，wal_lsn 为提交后锁定、覆盖本次提交的 WAL 位置。
-- 该表只 INSERT 不 UPDATE/DELETE，绑定关系随流复制物理复制到备库。
CREATE SEQUENCE IF NOT EXISTS maintenance_ticket_seq;
CREATE TABLE IF NOT EXISTS maintenance_records (
    ticket_no   TEXT PRIMARY KEY,
    content     TEXT NOT NULL,
    wal_lsn     PG_LSN NOT NULL,
    wal_segment TEXT NOT NULL,
    written_by  TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT SELECT, INSERT, UPDATE ON maintenance_records TO ${PG_APP_USER};
GRANT ALL PRIVILEGES ON SEQUENCE maintenance_ticket_seq TO ${PG_APP_USER};
SQL

echo "init: roles, demo_records and maintenance_records tables created"
