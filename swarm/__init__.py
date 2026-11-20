"""Swarm — 跨设备多智能体协作调度器。

同一个局域网里，每台跑着 agent 的 Linux（Termux、proot、容器、虚拟机都行）
跑一个 ``swarm node``；任意一台（通常在手机上）跑一个 ``swarm hub``。
hub 把大项目拆成带依赖的任务图，节点长轮询抢任务、在自己机器上干、
把改动回来的补丁交上去，hub 负责合并、判冲突、给你看板。

零第三方依赖，只用 CPython 3.8+ 标准库。
"""

__version__ = "1.2.0"
PROTO = 2
DEFAULT_PORT = 47770
DEFAULT_DISCOVERY_PORT = 47771
