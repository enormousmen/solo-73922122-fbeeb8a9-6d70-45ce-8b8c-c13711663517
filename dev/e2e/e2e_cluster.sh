#!/usr/bin/env bash
# 本地沙箱 E2E 集群（一次性启动；沙箱禁信号，进程随环境回收）。
# 拓扑：真实主库 55452（dev/local_cluster.sh 已建）
#       pg_receivewal -> staging -> 门控 watcher -> archive -> 备库 55455 归档恢复
set -euo pipefail

PGROOT="${PGROOT:-/workspace/.local-pg/pgroot}"
export LD_LIBRARY_PATH="$PGROOT/usr/lib/x86_64-linux-gnu"
P="$PGROOT/usr/lib/postgresql/15/bin"
PRIMARY_PORT=55452
B=/workspace/pg-e2e/b
SLOT=e2e_gate_slot

mkdir -p "$B/staging" "$B/archive" "$B/sock" "$B/state"
echo open > "$B/state/gate"
rm -f "$B/state/b_unreachable" "$B/state/b_promoted" \
      "$B/state/b_replay_paused" "$B/state/b_replay_freeze"

# 1) 流式接收 WAL 到 staging（独立进程，不依赖 postmaster 子进程）
nohup "$P/pg_receivewal" -h 127.0.0.1 -p "$PRIMARY_PORT" -U repl \
      -D "$B/staging" --slot="$SLOT" --no-loop \
      > "$B/receivewal.log" 2>&1 &
echo "pg_receivewal pid=$!"

# 2) 门控监视器：open 时把完整段原子搬进 archive
E2E_BASE="$B" nohup python3 /workspace/dev/e2e/gate_watcher.py \
      > "$B/watcher.log" 2>&1 &
echo "gate_watcher pid=$!"

# 3) 归档恢复备库（standby.signal + restore_command 已在数据目录配置）
nohup "$P/postgres" -D "$B/data" > "$B/standby.log" 2>&1 &
echo "standby postgres pid=$!"
echo "E2E 集群已启动（主库 55452 / 备库数据目录 $B/data）"
