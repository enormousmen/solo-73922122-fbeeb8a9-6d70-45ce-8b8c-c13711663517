#!/usr/bin/env bash
# 本地（无 Docker）用解压的 PostgreSQL 15 二进制拉起一对真实流复制主备。
# 仅用于开发机自测；交付物是 compose/ 下的 Docker Compose（容器内无信号限制）。
#
# 建备库两种方式，自动选择：
#   1) 优先 pg_basebackup（标准、推荐；需要 postgres 能向 checkpointer 发信号）
#   2) 信号被受限沙箱拦截时，退化为「物理复制槽 + 运行中文件拷贝 + 崩溃恢复」
#      —— PostgreSQL 靠 full_page_writes/WAL 重放把非一致快照恢复到一致点，
#         复制槽保证拷贝窗口内的 WAL 不会被主库回收。
set -euo pipefail

PGROOT="${PGROOT:-/workspace/.local-pg/pgroot}"
PGBIN="$PGROOT/usr/lib/postgresql/15/bin"
export LD_LIBRARY_PATH="$PGROOT/usr/lib/x86_64-linux-gnu"
export PATH="$PGBIN:$PATH"

BASE="${BASE:-/workspace/pg-local}"
PRIMARY_PORT="${PRIMARY_PORT:-55452}"
STANDBY_PORT="${STANDBY_PORT:-55453}"
SLOT_NAME="local_standby_slot"
ROLE_APP_PASS=apppass
ROLE_REPL_PASS=replpass
ROLE_ADMIN_PASS=adminpass

mkdir -p "$BASE"
chmod 700 "$BASE" 2>/dev/null || true

start_primary() {
  local datadir="$BASE/primary"
  rm -rf "$datadir"
  initdb -D "$datadir" -U postgres --auth=trust --no-locale --encoding=UTF8 >/dev/null
  cat >> "$datadir/postgresql.conf" <<EOF
port = $PRIMARY_PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$BASE/sock'
wal_level = replica
max_wal_senders = 10
hot_standby = on
EOF
  mkdir -p "$BASE/sock"
  pg_ctl -D "$datadir" -l "$BASE/primary.log" -w start
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 <<SQL
CREATE ROLE app LOGIN PASSWORD '$ROLE_APP_PASS';
CREATE ROLE repl REPLICATION LOGIN PASSWORD '$ROLE_REPL_PASS';
CREATE DATABASE appdb OWNER app;
SQL
  # 复刻 compose/postgres/init/01-app-schema.sh 的授权（建表交给应用启动时做）
  psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U postgres -d appdb -v ON_ERROR_STOP=1 <<SQL
GRANT USAGE, CREATE ON SCHEMA public TO app;
SQL
  echo "primary ready on 127.0.0.1:$PRIMARY_PORT"
}

start_standby() {
  local datadir="$BASE/standby"
  rm -rf "$datadir"
  if PGPASSWORD="$ROLE_REPL_PASS" pg_basebackup \
       -h 127.0.0.1 -p "$PRIMARY_PORT" -U repl -D "$datadir" -Fp -Xs -R 2>"$BASE/basebackup.err"; then
    echo "standby: built via pg_basebackup"
  else
    echo "standby: pg_basebackup 不可用（$(head -1 "$BASE/basebackup.err")），退化到文件拷贝+复制槽"
    # 1) 物理复制槽，保证拷贝窗口内 WAL 不被回收
    psql -h 127.0.0.1 -p "$PRIMARY_PORT" -U postgres -d postgres -tAc \
      "SELECT pg_create_physical_replication_slot('$SLOT_NAME')
       WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='$SLOT_NAME')" >/dev/null
    # 2) 运行中拷贝，删除 pid；无 backup_label，启动时自动崩溃恢复到一致点
    cp -a "$BASE/primary" "$datadir"
    rm -f "$datadir/postmaster.pid"
    touch "$datadir/standby.signal"
    cat > "$datadir/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=127.0.0.1 port=$PRIMARY_PORT user=repl password=$ROLE_REPL_PASS application_name=local-standby'
primary_slot_name = '$SLOT_NAME'
EOF
  fi
  cat >> "$datadir/postgresql.conf" <<EOF
port = $STANDBY_PORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$BASE/sock'
hot_standby = on
EOF
  pg_ctl -D "$datadir" -l "$BASE/standby.log" -w start
  echo "standby starting on 127.0.0.1:$STANDBY_PORT"
}

wait_standby_replaying() {
  for _ in $(seq 1 40); do
    local r
    r=$(psql -h 127.0.0.1 -p "$STANDBY_PORT" -U postgres -d appdb -tAc \
      "SELECT pg_is_in_recovery()" 2>/dev/null || echo "")
    [ "$r" = "t" ] && return 0
    sleep 1
  done
  echo "standby 未进入 recovery（受限沙箱可能拦截了 postmaster 拉起子进程的信号）" >&2
  return 1
}

stop_all() {
  for d in standby primary; do
    if [ -f "$BASE/$d/postmaster.pid" ]; then
      pg_ctl -D "$BASE/$d" -m fast -w stop || true
    fi
  done
}

case "${1:-up}" in
  up)
    start_primary
    start_standby
    wait_standby_replaying || true
    echo "cluster up (primary=$PRIMARY_PORT standby=$STANDBY_PORT)"
    ;;
  stop)
    stop_all
    echo "cluster stopped"
    ;;
  restart)
    "$0" stop
    "$0" up
    ;;
  *)
    echo "usage: $0 [up|stop|restart]" >&2
    exit 2
    ;;
esac
