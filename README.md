# PostgreSQL 主备单向切换 + 检修单 WAL 落盘核验演示平台

一个 FastAPI 网页，连接一对**真实运行流复制的 PostgreSQL 15 主备库**，支持：

- 录入 / 查询示例记录（写入只走当前主库）
- **检修记录核验单**：主库 A 录入返回核验单号，单号绑定覆盖本次提交的 WAL 位置，
  凭单号核验备库 B 是否已将该位置**落盘**（回放暂停、断流也能确认）
- 发起**单向**主备切换（旧主 → 备库提升）
- 暂停 / 恢复备库 WAL 回放，用于现场演示「回放超时回滚 → 恢复回放后切换成功」

## 快速开始

```bash
cd compose
cp .env.example .env          # 可改密码与超时参数
docker compose up -d --build
# 打开 http://localhost:8000
```

一键端到端演示（需要本机有 `curl`、`jq`、`docker compose`）：

```bash
./demo.sh                     # 主备切换全链路
./demo_verify.sh              # 检修单落盘核验：暂停回放确认、断流录入与恢复确认
```

`demo.sh` 会依次完成：写入记录 → 暂停备库回放 → 切换并观察 503 与超时回滚 →
恢复回放 → 重新切换成功 → 校验旧记录在新主可查、新写入只走新主 → 重启网页容器再校验。

`demo_verify.sh` 会依次完成：A 录入拿核验单号 → B 落盘确认 → **暂停 B 回放后仍确认** →
断开 B 的流复制网卡（断流）→ 断流期间 A 继续录入拿新单号 → 旧单仍确认、新单提示
进度不足 → 恢复网络后**凭原单号确认** → 断开业务网卡模拟 B 不可达并恢复。

## 检修单 WAL 落盘核验语义（与需求逐条对应）

1. **A 提交成功即返回核验单号，B 断流也能录入**：录入只写主库 A，流复制是异步的，
   B 的状态不影响 A 提交。
2. **单号绑定的 WAL 位置覆盖本次提交**：录入事务提交后再读取
   `pg_current_wal_lsn()` 作为绑定 LSN，该位置不早于本次提交的 commit WAL 记录；
   绑定行（`maintenance_records` 表）在录入流程内完成一次 LSN 回写后即不可变，
   没有任何按单号改写或删除既有行的接口，后续录入与网页重启都不改变它，
   且经物理流复制复制到 B。
3. **凭单号查询，B 仍是备库且已落盘即确认**：核验依据是 B 的
   `pg_last_wal_receive_lsn()`——备库已接收并 **fsync 落盘**的 WAL 位置，
   `receive_lsn >= 绑定 LSN` 即确认。
4. **暂停回放或断流也给出确认**：确认只看落盘不看回放。回放暂停
   （`pg_wal_replay_pause()`）只冻结回放，接收与落盘继续；断流后已落盘的 WAL
   不回退，两种情况下旧单都保持确认，接口同时返回 `replay_paused` / `streaming`
   状态并在提示语中说明。
5. **三种异常分别说明原因**：
   - `BEHIND`：B 可达且仍为备库，但落盘进度未覆盖绑定位置；
   - `UNREACHABLE`：无法连接 B；
   - `PROMOTED`：B 已提升为主库（`pg_is_in_recovery()=f`），不再以备库身份出具确认；
   - 另有 `TICKET_NOT_FOUND`（主库查无单号，HTTP 404）。
6. **断流演示方式**：compose 拆成 `demo-net`（网页↔A/B 的 SQL）与 `repl-net`
   （A↔B 的 WAL 流复制）两张网络；断开 B 在 `repl-net` 上的网卡即断流，
   而网页仍能经 `demo-net` 查询 B 的落盘实况。

## 切换语义（与需求逐条对应）

1. **切换受理后新写入提示稍后重试**：受理瞬间状态落盘为 `SWITCH_PENDING`，
   之后所有 `POST /api/records` 返回 `503 + Retry-After: 5`。
2. **已接单的写入在旧主收尾**：受理时通过 `ALTER SYSTEM SET
   default_transaction_read_only=on` + reload 围栏旧主，只拦截**新事务**，
   已在执行的写事务继续提交；后台等待旧主无活动写事务（`wait_for_wal_quiesce`）
   后才锁定**最终目标 LSN**，保证覆盖围栏前接单的最后一个事务。
3. **回放完旧主已提交事务才可提升**：轮询备库
   `pg_last_wal_replay_lsn() >= 最终目标 LSN` 后才调用 `pg_promote()`；
   未追平绝不提升。
4. **回放超时恢复旧主写入**：超过 `SWITCH_TIMEOUT` 未追平（例如回放被暂停），
   状态机进入 `ROLLBACK`，把旧主只读围栏解除并恢复写入，历史记录标记 `TIMEOUT`，
   恢复回放后可重新发起切换。
5. **提升后已确认记录在新主可查**：提升基于物理流复制，备库已回放旧主全部
   已提交 WAL，因此 `demo_records` 里所有已确认记录在新主完整可查。
6. **网页服务在提升后、更新写入目标前退出也安全**：所有状态经
   `write 临时文件 + fsync + os.replace + fsync 目录` 原子落盘到
   `/data/state/switch.json`；启动时 `reconcile_on_startup()` 以数据库**实际角色
   + 只读围栏**为准核对：目标已是主库则续完成功收尾，旧主仍是主库则按超时回滚，
   两个节点都可写（真脑裂）则拒绝自动选择。重启后的新写入只走真正的主库。

## 状态机

```
NORMAL ──发起切换──▶ SWITCH_PENDING ──回放追平+promote──▶ PROMOTED ──收尾──▶ NORMAL(leader=B)
                          │
                          └──超过 SWITCH_TIMEOUT──▶ ROLLBACK ──解除旧主只读──▶ NORMAL(leader=A)
```

## 角色与权限（最小权限）

| 角色 | 权限 | 用途 |
| --- | --- | --- |
| `app` | `demo_records` 的 SELECT/INSERT；`maintenance_records` 的 SELECT/INSERT/UPDATE（UPDATE 仅用于录入时一次性回写绑定 LSN，无改写既有单号的接口）及相关序列权限 | 网页记录与检修单读写 |
| `repl` | 仅 `REPLICATION` | 持续 WAL 流复制（`primary_conninfo`） |
| `postgres`（超级用户） | 全部 | 建库建角色、建表授权、`ALTER SYSTEM`、`pg_promote`、一次性 `pg_basebackup` |

> PG15 的 `pg_basebackup` 需要向 checkpointer 发信号，仅超级用户可做；
> 备库入口用超级用户完成一次性基础备份后，会把 `primary_conninfo` 改写为
> 最小权限的 `repl` 角色，持续复制不使用超级用户。

## 目录结构

```
compose/
├── docker-compose.yml          # primary + standby + web（demo-net/repl-net 双网络）
├── .env.example
├── demo.sh                     # 主备切换端到端演示脚本
├── demo_verify.sh              # 核验演示：暂停回放确认、断流录入/恢复确认
├── postgres/
│   ├── init/01-app-schema.sh   # 主库首启：建角色/表（含 maintenance_records）/授权
│   └── standby/entrypoint.sh   # 备库：pg_basebackup 建库后以流复制启动
└── web/
    ├── Dockerfile
    └── requirements.txt
app/                            # FastAPI 应用（与 compose 解耦，可独立运行）
├── config.py                   # 环境变量配置，app/admin 两套 DSN
├── state.py                    # 状态机 + 原子持久化（纯标准库，可离线单测）
├── switcher.py                 # 切换编排器（适配器协议，可注入假实现）
├── verification.py             # 核验判定纯逻辑（CONFIRMED/BEHIND/UNREACHABLE/PROMOTED）
├── database.py                 # 真实 PostgreSQL 适配器（psycopg 3）
├── main.py                     # HTTP 接口 + 演示页面 + 后台工作线程
└── static/index.html
tests/                          # unittest（状态机/编排器/Web 层/核验判定）
dev/local_cluster.sh            # 无 Docker 时用本机 postgres 二进制起主备的辅助脚本
dev/e2e/                        # 受限沙箱下的本地 E2E 基础设施（门控归档管道+垫片，仅自测用）
```

## 本地开发与自测

```bash
pip install -r compose/web/requirements.txt
PYTHONPATH=. python -m unittest discover -s tests -v
```

- `tests/test_state.py`：原子持久化、状态机非法流转、成功/超时生命周期
- `tests/test_switcher.py`：回放追平提升、暂停回放超时回滚、写事务收尾锁定最终
  LSN、提升后崩溃的启动核对、脑裂拒绝
- `tests/test_verification.py`：核验判定纯逻辑（落盘覆盖、暂停回放/断流仍确认、
  进度不足、不可达、已提升、单号不存在）
- `tests/test_web.py`：503 + Retry-After、切换受理/回滚/成功全链路、重启后写入路由、
  检修单录入与核验状态、重启后单号绑定不变

## 已知边界

- 这是**演示级**单向切换，不做自动故障检测与 fence-old-leader 的 STONITH；
  旧主在新主提升后仍可能被人手工恢复写入，启动核对会把「两个节点都可写」识别为
  脑裂并拒绝自动选择，需人工介入。
- 切换后旧主不会自动反向重建为新主的备库，再次切回需要先重建流复制。
- 写事务收尾判定基于 `pg_stat_activity` 中无其他活动事务后端；超长事务会推迟
  最终 LSN 锁定，但仍受 `SWITCH_TIMEOUT` 约束，超时即回滚。
