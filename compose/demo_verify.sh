#!/usr/bin/env bash
# 检修单 WAL 落盘核验 · 端到端演示（需要先 docker compose up -d --build）：
#   场景一：暂停 B 的 WAL 回放，已落盘的核验单仍确认
#   场景二：断开 B 的流复制网络（断流）
#           - A 仍可录入并返回核验单号
#           - 断流前已落盘的旧单仍确认；断流期间的新单提示进度不足
#           - 恢复网络后凭原单号确认
#   附：B 不可达（断开业务网卡）时返回明确原因，再恢复
# 用法: ./demo_verify.sh [BASE_URL]
set -euo pipefail

BASE="${1:-http://localhost:8000}"
command -v curl >/dev/null || { echo "需要 curl"; exit 1; }
command -v jq >/dev/null || { echo "需要 jq"; exit 1; }
command -v docker >/dev/null || { echo "需要 docker（用于断流演示的网络断开/恢复）"; exit 1; }

api() {  # api METHOD PATH [JSON_BODY]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" -H 'Content-Type: application/json' \
      -d "$body" "$BASE$path"
  else
    curl -fsS -X "$method" "$BASE$path"
  fi
}
slog() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
wait_status() {  # wait_status TICKET STATUS [TIMEOUT]
  local ticket="$1" want="$2" timeout="${3:-30}" deadline=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local got
    got=$(api POST /api/maintenance/verify "{\"ticket_no\":\"$ticket\"}" | jq -r '.status')
    [ "$got" = "$want" ] && return 0
    sleep 1
  done
  echo "等待核验单 $ticket 状态 $want 超时，当前为 $got" >&2
  api POST /api/maintenance/verify "{\"ticket_no\":\"$ticket\"}" | jq . >&2
  exit 1
}
# 流复制专用网络（compose 项目名可能不同，按后缀匹配 repl-net）
REPL_NET="$(docker network ls --format '{{.Name}}' | grep -E '(^|_)repl-net$' | head -1 || true)"
DEMO_NET="$(docker network ls --format '{{.Name}}' | grep -E '(^|_)demo-net$' | head -1 || true)"
[ -n "$REPL_NET" ] || { echo "找不到 repl-net 网络，请先 docker compose up -d"; exit 1; }
[ -n "$DEMO_NET" ] || { echo "找不到 demo-net 网络，请先 docker compose up -d"; exit 1; }

slog "0. 集群初始状态"
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, nodes: [.nodes[] | {name, role, wal_lsn, replay_lsn}]}'

slog "1. 在主库 A 录入检修记录，拿到核验单号与绑定 WAL 位置"
T1=$(api POST /api/maintenance '{"content":"1号机组例行检修-演示"}')
echo "$T1" | jq '{ticket_no, wal_lsn, wal_segment, node, message}'
TICKET1=$(echo "$T1" | jq -r '.ticket_no')

slog "2. 凭单号核验：等待 B 落盘后 CONFIRMED"
wait_status "$TICKET1" CONFIRMED 30
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" \
  | jq '{status, confirmed, bound_lsn, standby_receive_lsn, replay_paused, streaming, message}'

slog "3. 暂停 B 的 WAL 回放，再核验同一单号：仍应 CONFIRMED"
api POST /api/replay/pause '{"node":"B"}' | jq .
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" \
  | jq '{status, confirmed, replay_paused, standby_receive_lsn, standby_replay_lsn, message}'
[ "$(api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" | jq -r '.status')" = "CONFIRMED" ] \
  || { echo "暂停回放后未确认，演示失败"; exit 1; }

slog "4. 断流：断开 B 的流复制网卡 $REPL_NET（B 的业务网卡保持连通）"
docker network disconnect "$REPL_NET" demo-standby
sleep 6  # 等 walreceiver 重试后在 pg_stat_wal_receiver 中消失

slog "4a. 断流期间 A 仍正常录入并返回新核验单号"
T2=$(api POST /api/maintenance '{"content":"断流期间录入的检修-演示"}')
echo "$T2" | jq '{ticket_no, wal_lsn, node, message}'
TICKET2=$(echo "$T2" | jq -r '.ticket_no')

slog "4b. 旧单（断流前已落盘）仍 CONFIRMED；新单进度不足 BEHIND 且 streaming=false"
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" \
  | jq '{ticket_no, status, streaming, message}'
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET2\"}" \
  | jq '{ticket_no, status, streaming, standby_receive_lsn, bound_lsn, message}'

slog "5. 恢复流复制网络，凭原单号 $TICKET2 核验：应转为 CONFIRMED"
docker network connect "$REPL_NET" demo-standby
wait_status "$TICKET2" CONFIRMED 40
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET2\"}" \
  | jq '{status, confirmed, streaming, standby_receive_lsn, message}'

slog "6. 恢复 B 的回放，两个单号持续可确认"
api POST /api/replay/resume '{"node":"B"}' | jq .
for t in "$TICKET1" "$TICKET2"; do
  api POST /api/maintenance/verify "{\"ticket_no\":\"$t\"}" \
    | jq '{ticket_no, status, replay_paused, message}'
done

slog "7. 附：断开 B 的业务网卡模拟不可达，核验应说明 UNREACHABLE；随后恢复"
docker network disconnect "$DEMO_NET" demo-standby
sleep 2
api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" \
  | jq '{status, message}'
docker network connect "$DEMO_NET" demo-standby
wait_status "$TICKET1" CONFIRMED 30
echo "恢复后核验：$(api POST /api/maintenance/verify "{\"ticket_no\":\"$TICKET1\"}" | jq -r '.status')"

slog "全部核验演示步骤完成"
