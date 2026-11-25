#!/usr/bin/env python3
"""门控型 WAL 归档监视器（仅本地沙箱 E2E 用）。

pg_receivewal 把 WAL 流式写入 staging/（完整段去掉 .partial）。
本监视器把完整段从 staging 原子移动到 archive/（备库 restore_command
的来源），移动与否受 gate 文件内容控制：

* gate 内容为 open （默认）：完整段立即进 archive，备库即可恢复；
* gate 内容为 closed：段留在 staging，模拟「断流 / 暂停」——
  已进 archive 的 WAL 仍在备库磁盘上，不会回退。

不使用任何信号；通过文件开关控制，适配本机 seccomp 限制。
"""
from __future__ import annotations

import os
import shutil
import time

BASE = os.environ["E2E_BASE"]
STAGING = os.path.join(BASE, "staging")
ARCHIVE = os.path.join(BASE, "archive")
GATE = os.path.join(BASE, "state", "gate")
INTERVAL = 0.5


def gate_open() -> bool:
    try:
        with open(GATE, "r", encoding="utf-8") as fh:
            return fh.read().strip() != "closed"
    except FileNotFoundError:
        return True


def main() -> None:
    os.makedirs(STAGING, exist_ok=True)
    os.makedirs(ARCHIVE, exist_ok=True)
    while True:
        if gate_open():
            for name in sorted(os.listdir(STAGING)):
                # 只搬完整段与历史文件，.partial 说明还在写入。
                if name.endswith(".partial"):
                    continue
                src = os.path.join(STAGING, name)
                dst = os.path.join(ARCHIVE, name)
                if os.path.exists(dst):
                    os.unlink(src)
                    continue
                tmp = dst + ".moving"
                shutil.move(src, tmp)
                os.replace(tmp, dst)  # 同目录原子改名
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
