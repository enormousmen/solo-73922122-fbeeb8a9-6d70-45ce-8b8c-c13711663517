# 本地沙箱 E2E 设施（不是 Docker 交付物）

本目录仅用于**无 Docker 的受限沙箱**里对真实 PostgreSQL 主备做端到端自测，
不进入 `compose/` 交付物；交付路径仍是标准流复制（`standby/entrypoint.sh`
里的 `pg_basebackup -R`）。

## 为什么需要它

本沙箱的 seccomp 禁止 `kill` 系统调用，PostgreSQL postmaster 无法向
checkpointer / walreceiver 等子进程发信号，因此跑不起真正的流复制备库
（备库会卡在 "database system is starting up"）。但独立的
`pg_receivewal` 进程可以正常流式接收 WAL，归档恢复的 startup 进程也能
正常回放。

## 拓扑（与流复制备库语义等价的部分）

```
主库 55452 ──pg_receivewal──▶ b/staging（完整 WAL 段，已 fsync）
                                  │ gate_watcher 按门控原子搬运
                                  ▼
                               b/archive
                                  │ restore_command='cp archive/%f %p'
                                  ▼
                            备库 55455（standby.signal 归档恢复，真实回放）
```

* `pg_receivewal` 写完整段才去掉 `.partial`，与流复制备库「先落盘后可见」一致；
* 备库 startup 进程真实回放归档段，恢复到的位置就是磁盘上已有的 WAL 位置；
* `state/gate` 写 `closed` 即停止搬运（模拟断流/暂停），已进 archive 的段
  保留不动（模拟「已落盘 WAL 不回退」）；写 `open` 即恢复。

## 适配垫片 shim.py

备库在此沙箱不接受 SQL，`ArchiveStandbyAdapter` 只重写 **B 节点**的角色判定与
落盘快照（读归档目录实况 + state 标记文件），A 节点与全部上层逻辑（FastAPI
路由、`verification.py` 判定、状态机）运行的都是交付代码。

## 运行

```bash
# 1) 起主库（dev/local_cluster.sh，会自动退化到文件拷贝建备，忽略其备库失败）
bash dev/local_cluster.sh up
# 2) 起门控归档管道与归档恢复备库
bash dev/e2e/e2e_cluster.sh
# 3) 跑八个场景（-P 不把 cwd 加入 sys.path，依赖装在 user site）
python3 -P dev/e2e/e2e_run.py
```

覆盖：正常录入/覆盖性/落盘确认、暂停回放仍确认、断流录入与恢复后确认、
B 不可达、B 已提升、未知单号、网页重启、真实 HTTP（uvicorn）冒烟。
