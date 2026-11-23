#!/usr/bin/env bash
# 端到端演示脚本：
#   1) 写入/查询示例记录
#   2) 暂停备库回放 -> 发起切换 -> 等待超时回滚（演示失败路径）
#   3) 恢复回放 -> 重新切换成功（演示成功路径）
#   4) 校验旧记录在新主可查、新写入只走新主
#   5) 重启 web 容器，校验重启后写入仍只走新主
# 用法: ./demo.sh [BASE_URL]
set -euo pipefail

BASE="${1:-http://localhost:8000}"
command -v curl >/dev/null || { echo "需要 curl"; exit 1; }
command -v jq >/dev/null || { echo "需要 jq"; exit 1; }

api() {  # api METHOD PATH [JSON_BODY]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" -H 'Content-Type: application/json' \
      -d "$body" "$BASE$path"
  else
    curl -fsS -X "$method" "$BASE$path"
  fi
}
wait_phase() {  # wait_phase PHASE [TIMEOUT]
  local want="$1" timeout="${2:-40}" deadline=$((SECONDS + timeout))
  while [ "$SECONDS" -lt "$deadline" ]; do
    local phase
    phase=$(api GET /api/status | jq -r '.cluster.phase')
    [ "$phase" = "$want" ] && return 0
    sleep 1
  done
  echo "等待阶段 $want 超时" >&2
  api GET /api/status | jq . >&2
  exit 1
}
slog() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

slog "0. 初始状态"
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, nodes: [.nodes[] | {name, role, replay_paused}]}'

slog "1. 在旧主 A 写入两条已确认记录"
api POST /api/records '{"content":"record-before-switch-1"}' | jq .
api POST /api/records '{"content":"record-before-switch-2"}' | jq .
api GET '/api/records?node=A' | jq '{node, count}'

slog "2. 暂停备库 B 的 WAL 回放（制造追不上）"
api POST /api/replay/pause '{"node":"B"}' | jq .

slog "3. 发起切换（20s 内回放追不上 -> 超时回滚）"
api POST /api/switch '{"target":"B","timeout":20}' | jq .
slog "3a. 切换受理后新写入应被拒绝（期望 HTTP 503）"
curl -sS -o /tmp/switch-503.json -w 'HTTP %{http_code}\n' \
  -X POST -H 'Content-Type: application/json' \
  -d '{"content":"should-be-rejected"}' "$BASE/api/records"
jq . /tmp/switch-503.json
wait_phase NORMAL 35
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, last: .cluster.history[-1].result}'

slog "4. 恢复回放，重新发起切换 -> 成功"
api POST /api/replay/resume '{"node":"B"}' | jq .
api POST /api/switch '{"target":"B","timeout":20}' | jq .
wait_phase NORMAL 40
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader, last: .cluster.history[-1]}'

slog "5. 校验：旧记录在新主 B 可查，新写入只走 B"
api GET '/api/records?node=B' | jq '{node, count, contents: [.records[].content]}'
for c in record-before-switch-1 record-before-switch-2; do
  api GET '/api/records?node=B' | jq -e --arg c "$c" '.records[] | select(.content==$c)' >/dev/null \
    && echo "OK: $c 在新主 B 可查"
done
api POST /api/records '{"content":"record-after-promotion"}' | jq .

slog "6. 重启网页服务，验证重启后写入仍只走新主 B"
docker compose restart web >/dev/null
sleep 3
api GET /api/status | jq '{phase: .cluster.phase, leader: .cluster.leader}'
api POST /api/records '{"content":"record-after-web-restart"}' | jq .
api GET /api/records | jq '{node, latest: .records[0].content}'

slog "全部演示步骤完成"
