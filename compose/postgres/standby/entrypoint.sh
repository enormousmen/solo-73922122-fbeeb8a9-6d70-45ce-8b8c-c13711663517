#!/bin/bash
# 备库容器入口：数据卷为空时从主库做 pg_basebackup 建立物理备库，
# -R 会自动生成 standby.signal 与 primary_conninfo，随后以流复制方式启动。
set -euo pipefail

DATA_DIR=/var/lib/postgresql/data
: "${PRIMARY_HOST:?missing PRIMARY_HOST}"
: "${PG_REPL_USER:?missing PG_REPL_USER}"
: "${PG_REPL_PASSWORD:?missing PG_REPL_PASSWORD}"
: "${PG_ADMIN_USER:?missing PG_ADMIN_USER}"
: "${PG_ADMIN_PASSWORD:?missing PG_ADMIN_PASSWORD}"

mkdir -p "$DATA_DIR"
chown -R postgres:postgres /var/lib/postgresql
chmod 0700 "$DATA_DIR"

if [ ! -s "$DATA_DIR/PG_VERSION" ]; then
  echo "standby: data dir empty, pg_basebackup from ${PRIMARY_HOST} ..."
  # 主库可能还在就绪边缘，重试基础备份。
  ok=0
  # shellcheck disable=SC2034
  for attempt in $(seq 1 30); do
    if PGPASSWORD="$PG_ADMIN_PASSWORD" gosu postgres pg_basebackup \
         -h "$PRIMARY_HOST" -U "$PG_ADMIN_USER" \
         -D "$DATA_DIR" -Fp -Xs -P -R; then
      ok=1; break
    fi
    echo "standby: base backup attempt $attempt failed, retrying..."
    rm -rf "$DATA_DIR"/* "$DATA_DIR"/.[!.]* 2>/dev/null || true
    sleep 2
  done
  [ "$ok" = "1" ] || { echo "standby: pg_basebackup 多次失败" >&2; exit 1; }
  chmod 0700 "$DATA_DIR"
  # -R 生成的 primary_conninfo 用的是管理员，改写为专用流复制角色。
  sed -i "s|user=${PG_ADMIN_USER}|user=${PG_REPL_USER}|; s|password=${PG_ADMIN_PASSWORD}|password=${PG_REPL_PASSWORD}|" \
    "$DATA_DIR/postgresql.auto.conf"
  echo "standby: base backup finished; streaming as ${PG_REPL_USER}"
else
  echo "standby: existing data dir found, starting directly"
fi

# 提升后 standby.signal 会被 PostgreSQL 自动删除，容器重启即以主库身份运行。
exec gosu postgres postgres -D "$DATA_DIR" -c config_file="$DATA_DIR/postgresql.conf"
