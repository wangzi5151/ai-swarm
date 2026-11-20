# Swarm —— 局域网多 AI 协同调度器

[![CI](https://github.com/wangzi5151/ai-swarm/actions/workflows/ci.yml/badge.svg)](https://github.com/wangzi5151/ai-swarm/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![零第三方依赖](https://img.shields.io/badge/deps-0-brightgreen)](#环境要求)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> 你已经在不同手机/电脑上装了不同的 agent 应用，每个应用里都有自己的 Linux 和 agent。
> 这个工具把它们连起来：**一个大项目拆成任务图，谁空谁领，各自在自己的 Linux 里干，改动回来统一合并。**
> 手机浏览器打开一个网页就能看全程。

```
      ┌──────────────────────────────────────────────┐
      │   hub（任意一台机器，通常放手机上）            │
      │   拆任务 · 派发 · 收差分 · 合并 · 看板         │
      └───────┬──────────────┬──────────────┬────────┘
              │  同一局域网   │              │
      ┌───────┴──────┐ ┌─────┴────────┐ ┌───┴──────────┐
      │ 手机 A        │ │ 手机 B        │ │ 笔记本        │
      │ Termux+Ubuntu │ │ 另一款 agent  │ │ 本地大模型     │
      │ aider         │ │ claude/codex  │ │ ollama        │
      │ swarm node    │ │ swarm node    │ │ swarm node    │
      └───────────────┘ └───────────────┘ └───────────────┘
```

**它不要求你在一个 App 里装多个 Linux** —— 面向**已经存在的、分散在不同机器上的 Linux 和 agent**。
仓库里只有这一套工具：纯 Python 标准库，没有 Android 工程，不需要 Gradle、SDK 或 APK。

---

## 一分钟先看效果

零依赖（只要 Python 3.8+，不用装任何第三方包）：

```bash
git clone https://github.com/wangzi5151/ai-swarm.git
cd ai-swarm
./bin/swarm demo
```

它会在本机起 1 个 hub + 3 个模拟节点，把一个"记账 App"拆成 7 个任务并发跑完，
你能看到任务流水线、文件合并、git 提交，最后打印产出目录。

---

## 真机三步

### 1. 选一台当 hub（调度中心）

```bash
./bin/swarm hub --root ~/.swarm
```

启动后会打印：

```
看板   http://192.168.1.23:47770/   （手机浏览器直接开）
地址   192.168.1.23:47770
主令牌 3f9a…（一长串）
```

主令牌 = 管理钥匙（建项目、看板、发节点令牌、回滚），**别外传**，只存在 `~/.swarm/token`。

**给每台干活的机器单独发一个节点令牌**（推荐，可单独吊销）：

```bash
./bin/swarm tokens add 手机A --hub 127.0.0.1:47770
# 打印一次令牌值；第一次 join 时自动绑定到那台机器
```

这比所有节点共用主令牌安全：某台手机丢了，`./bin/swarm tokens revoke 手机A`
下一次请求就失效，其它机器不受影响。

### 2. 每台干活的机器跑一个节点

把仓库 `git clone` 过去（或只拷 `swarm/` + `bin/swarm`），然后：

```bash
./bin/swarm node --discover --token <节点令牌> --name 手机A --slots 2 \
  --agent cli --cmd 'aider --yes --no-auto-commits --message {prompt}' \
  --verify 'python3 -m compileall -q src'
```

- `--discover`：UDP 广播自动找 hub（找不到就用 `--hub 192.168.1.23:47770`）
- `--slots 2`：这台机器同时干 2 个任务
- `--verify`：每个任务交付前在本机跑的验收命令；失败则改动**不会**进主工程
- `--agent`：这个节点用谁干活，见下一节

### 3. 手机上看

浏览器打开 `http://192.168.1.23:47770/`，输入主令牌（或在地址里带一次 `/?t=主令牌`）。
登录会话是**服务端随机值 + HttpOnly cookie**，默认 12 小时；可以点"换令牌"，后端也支持 `/api/logout` 注销。

能看到：在线节点、任务状态/耗时/日志、验收结果、冲突 diff、
一键裁决/回滚、事件流。

### 4. 交一个项目

```bash
./bin/swarm submit -f 需求.md --watch
```

自动拆成"公共层 → 并行模块 → 集成 → 文档"的任务图，派给所有在线节点。

### 5. 从已有仓库开始（大型项目常用）

```bash
# 目录会先压缩（尊重 .gitignore，跳过 .git/build 等），再放到 hub 的项目树里
./bin/swarm import ./已有项目 --title "接手项目" --plan plan.json \
  --hub 192.168.1.23:47770 --watch
```

`--plan` 强烈建议给：它决定每个任务能看/能改哪些路径。不给就用默认拆解，
默认拆解只适合从零开始的新项目。hub 离线时也可以用 `--root ~/.swarm` 导入（需先停 hub）。

---

## 怎么接你已经装好的 agent

节点只负责"把任务说明交给一个命令，然后看文件变没变"。所以任何能改文件的 agent 都能接。

### 命令行类（`--agent cli`）

命令模板里可用占位符：`{prompt}` `{files}` `{workdir}` `{task_id}` `{title}` `{model}`。
同时会注入环境变量 `SWARM_PROMPT` `SWARM_FILES` `SWARM_TASK_FILE`（任务说明的 md 文件）等。

| 你的 agent | 命令模板（按自己版本微调） |
|---|---|
| Aider | `aider --yes --no-auto-commits --message "{prompt}" {files}` |
| Claude Code | `claude -p "{prompt}"` |
| Codex CLI | `codex exec "{prompt}"` |
| 本地 ollama + aider | `aider --model ollama/qwen2.5-coder --message "{prompt}"` |
| 自己写的脚本 | `python3 my_agent.py --task "{prompt}" --files {files}` |
| 只认 stdin 的工具 | `cat {workdir}/.swarm-task.md \| my-agent` |

> 命令在任务的临时工作目录里执行（`{workdir}`），agent 改哪个文件都行，
> 但只有落在任务 `write` 范围里的改动会被收走，越界改动直接丢弃并记一条警告。
> **agent 卡住不输出也会被硬超时杀掉**（整个进程组），不会永久占住槽位。

### 接口类（`--agent http`，OpenAI 兼容）

```bash
./bin/swarm node --discover --token <节点令牌> --agent http \
  --llm-base http://127.0.0.1:11434/v1 --model qwen2.5-coder:14b \
  --verify 'python3 -m compileall -q src'
```

LM Studio、vLLM、llama.cpp server、Ollama、以及各家云 API 都吃这一套。
节点会用自己机器的模型和 Key，不同节点互不影响。

### 不同任务用不同 agent

在 `plan.json` 里给单个任务指定命令/模型/验收，节点会优先用任务里的：

```json
{"tasks": [
  {"id": "t-ui", "title": "界面", "write": ["src/ui/**"],
   "cmd": "aider --yes --message {prompt} {files}",
   "verify": "python3 -m compileall -q src"},
  {"id": "t-api", "title": "接口", "write": ["src/api/**"],
   "cmd": "claude -p {prompt}", "model": "opus", "verify": "pytest -q tests/api"}
]}
```

`"agent": "cli"` 这类字段则是"只派给这种节点"的约束。

---

## 具体到各类 Android 上的 Linux

| 环境 | 准备工作 | 启动 |
|---|---|---|
| Termux | `pkg install python git` | `termux-wake-lock && ./bin/swarm node …` |
| Termux + proot Ubuntu | 进 Ubuntu 后 `apt install python3 git` | 同上（路径用 proot 里的） |
| Andronix / UserLAnd | 发行版内装 `python3` | 同上 |
| 自带 Linux 的 agent App | 只要能执行 shell | 在它的 Linux 里跑 `swarm node` |

要点：

1. **同一个 Wi-Fi**，且路由器没开"AP 隔离/客户端隔离"（开了就互相看不见，改用手机热点）。
2. 节点机别让系统把后台 App 杀掉：Android 设置里关掉电池优化 / 允许后台活动。
3. 端口：hub 用 TCP `47770` 和 UDP `47771`，一台机器只开 hub，其余机器用节点模式。
4. 手机热点也行：hub 连热点，其它设备也连热点，`--discover` 一般能自动找到。
5. 本机时钟要准（签名允许 ±180 秒）；时间差太大会鉴权失败。

---

## 它是怎么保证"不互相改坏"的

### 隔离与合并

1. **任务范围（`write`）**：每个任务只允许改指定目录。节点在按
   `(项目, 任务, 第几次尝试)` 命名的独立临时目录里干活，两个项目同名任务也不会互相覆盖。
2. **主工程单写者**：只有 hub 写主工程，节点永远碰不到主工程。
3. **内容哈希比对**：合并时校验"文件还是不是我发出去的那一版"（sha256）。
   变了 ⇒ 说明别的任务动过 ⇒ 判冲突，把两边版本都留着等你裁决，绝不静默覆盖。
4. **调度期就避开冲突**：写范围重叠的任务不会同时派发（`scopes_overlap`），
   并行任务天然不碰同一个文件；真碰上了也只会变成"待裁决"。
5. **越界内容双向拦截**：节点上传前过滤一次，hub 收到后还会再校验一次；
   路径里的 `..`、绝对路径、非法 sha 一律拒绝。`write` 会自动并入 `read`，
   避免"已有文件不在快照里"被误判成"两个任务同时新建"。

### 冲突、回滚、验收

- 冲突时看板/命令行都能看 unified diff：

  ```bash
  ./bin/swarm diff p1 t-api src/api/client.py
  ```

- 裁决：`用节点的版本` / `保留主工程`（保留主工程时节点版本仍会记录在 `discarded` 里）。
- 合并可回滚：

  ```bash
  ./bin/swarm rollback p1 --task t-api      # 撤销该任务的合并
  ./bin/swarm rollback p1                   # 撤销项目最近一次合并
  ```

  回滚会恢复合并前的文件（新建的文件删掉）。如果这些文件之后又被别的任务改过，
  默认拒绝；确认要覆盖才加 `--force`。

- **空交付不算完成**：agent 什么都没改、或改动全被范围过滤，hub 会判失败并重试，
  不会假装 `done`。
- **验收命令**：任务 `verify` 或节点 `--verify` 失败时，改动不落主工程，任务按失败重试；
  结果记录在看板和 `/api/tasklog`。
- 失败会向上游传播：重试用尽后任务 `failed`，依赖它的任务变 `blocked`，项目进终态，
  `--watch` 会退出并告诉你去看哪条日志。

---

## 为什么快

| 机制 | 效果 |
|---|---|
| 长轮询领任务（25s 挂起） | 任务一空出来，节点立刻拿到，没有轮询空转 |
| 依赖完成即解锁下游 | 流水线式推进，不等整批任务做完 |
| 按写范围分片 | 4 个模块就在 4 台机器上真并行，而不是抢同一份代码 |
| 内容寻址 blob（sha256） | 同样的文件全网只传一次，跨任务复用缓存 |
| 批量存在性查询 + 只传缺失 | 每个任务不再为每个文件发一次 HEAD |
| gzip + 8 路并发拉取/推送 + keep-alive | 文本文件真正按压缩后的体积走 |
| TCP_NODELAY | 小包不再每次多等一个延迟 ACK |
| 租约 + 心跳 | 谁掉线，它手上的任务几秒~几十秒内回到队列，不会卡死整个项目 |

可复现的数字（`tools/bench.py`，mock agent，**宿主 CPU 配额很低的容器**里测的）：

```
$ python3 tools/bench.py --tasks 24 --nodes 1
24 个任务 / 1 节点 2 槽：约 2~3 任务/秒，单任务均值 0.5~0.9s
上传 6.7 KB / 下载 0.1 KB（内容寻址，同文件只传一次）
```

在多核真机上，多节点会继续线性加速；这个容器里 3 节点并发反而被 CPU 配额拖慢，
所以 CI 只拿它做功能冒烟，不当作性能门槛。你可以自己跑：

```bash
python3 tools/bench.py --tasks 50 --nodes 4
```

---

## 命令清单

```bash
./bin/swarm hub        [--root ~/.swarm] [--port 47770] [--no-git] [--lease 180] [--stale 25]
./bin/swarm node       --discover | --hub ip:port  --token …  [--slots 2] [--agent mock|cli|http]
                       [--verify '验收命令'] [--keep-work]
./bin/swarm submit     -f spec.md [--watch] [--plan plan.json] [--hub …] [--token …]
./bin/swarm import     ./已有目录 --title T [--plan plan.json] [--watch] [--root …|--hub …]
./bin/swarm status     [--json]
./bin/swarm logs       <project> <task>
./bin/swarm diff       <project> <task> <path>          # 冲突/改动差异
./bin/swarm rollback   <project> [--task id] [--force]
./bin/swarm control    <project> pause|resume|retry|cancel|finish [--task id]
./bin/swarm tokens     [list] | add <名字> [--node id] | revoke <名字/前缀>
./bin/swarm token      [--root ~/.swarm]                # 查主令牌
./bin/swarm gc         [--grace 600] [--root …|--hub …] # 清理无引用 blob
./bin/swarm plan       -f spec.md [-o plan.json]        # 只看拆解，不派发
./bin/swarm export     p1 -o out.tar.gz                 # 打包合并后的项目
./bin/swarm demo       [--nodes 3] [--keep]
```

其它机器的环境变量写法（少打字）：

```bash
export SWARM_HUB=192.168.1.23:47770
export SWARM_TOKEN=<节点令牌>
export SWARM_AGENT_CMD='aider --yes --message {prompt}'
./bin/swarm node --discover
```

---

## 需求文档怎么写

默认按 `## 模块名 (目录/**)` 切分；没写标题就走默认脚手架（公共层 → 并行模块 → 集成 → 文档）。

```markdown
# 局域网记账 App

## 需求
两台手机互通，一起记一笔账；离线可用，回到同一 Wi-Fi 自动同步。

## 数据层 (src/data/**)
SQLite 表结构、迁移、聚合查询。

## 界面 (src/ui/**)
账单列表、记一笔、月度统计。

## 同步 (src/sync/**)
mDNS 发现 + 冲突合并。
```

想完全控制任务图，就先 `./bin/swarm plan -f spec.md -o plan.json`，
改完再 `./bin/swarm submit --plan plan.json`。任务字段：
`id/title/prompt/read/write/deps/priority/agent/cmd/model/verify`。

- `read` 会自动并入 `write`（任务必须能看见自己要改的已有文件）。
- `verify` 在该任务的工作目录里执行，退出码非 0 即失败。
- 同一份 plan 里任务 id 必须唯一（跨项目可以重名，系统内部按 `(project, task)` 定位）。

---

## 掉线 / 卡住 / 出错怎么办

- **节点被系统杀了**：心跳停 → `--stale` 秒后 hub 判定失联，任务回队列，别的节点接手；
  这种基础设施故障不消耗 agent 的重试次数。
- **节点活着但任务丢了**（交付被拒、hub 短暂 500）：租约到期后任务也会回队列。
- **agent 卡住不输出**：`cli` 模式硬超时（默认 `lease*4`，上限 2 小时），整个进程组被杀。
- **跑到一半断电**：hub 状态每 3 秒落盘（有变化时），重启 hub 后未完成任务自动回队列；
  正在跑的任务会被重置为 queued，节点重连后再领。
- **某个任务一直失败**：看板上点"重试"，或 `./bin/swarm control p1 retry --task t-api`。
  被阻塞的任务要先重试它的上游。
- **整体暂停**：看板"暂停"，或 `./bin/swarm control p1 pause`。
- **磁盘越来越大**：`./bin/swarm gc`（默认只删 10 分钟前、且当前无人引用的 blob）。
- 调参：`--stale 25`（多久算掉线）、`--lease 180`（租约秒数）、`--max-attempts 2`
  （agent 失败几次后判死；节点掉线不算）。

---

## 目录结构

```
swarm/
  proto.py       HTTP + HMAC 签名 + nonce 防重放 + gzip/keep-alive 客户端
  store.py       内容寻址存储（sha256，带 GC）
  workspace.py   快照 / 铺设 / 差异 / 范围判定 / .gitignore / 软链与路径安全
  planner.py     spec → 任务图（含环检测、关键路径优先级、read∪write）
  hub.py         调度、租约、合并、冲突、回滚、验收、令牌、看板 API
  node.py        发现、多槽并发、租约心跳、拉取/回传、任务级验收
  agents.py      mock / cli / http 适配器 + 硬超时
  discover.py    UDP 自动发现
  dashboard.py   手机看板（单文件 HTML）
  cli.py         命令行
  demo.py        一条命令演示
bin/swarm        启动器
docs/            连接与部署（各类 Android Linux 的坑）
examples/        agent 命令示例、plan 示例、需求示例
tools/           零依赖检查 + 吞吐基准
tests/           unittest（覆盖多项目、租约、失败传播、回滚、令牌、路径安全等）
.github/         CI：Python 3.8/3.10/3.12 + demo + 基准冒烟
```

跑测试：

```bash
python3 -m unittest discover -s tests -v     # 全量用例
SWARM_TEST_UDP=1 python3 -m unittest tests.test_discover -v   # 带上 UDP 广播用例
python3 tools/check_stdlib_only.py           # 确认没有偷偷引入第三方包
python3 tools/bench.py --tasks 24 --nodes 1  # 吞吐基准
```

---

## 安全边界（说清楚）

- 鉴权：每个请求用令牌做 HMAC-SHA256 签名 + 时间戳（±180s）+ **nonce**；
  nonce 签进签名，hub 记录见过的签名，窗口内原样重放会被拒绝。**令牌本身不上网**。
- **每节点独立令牌**：可单独吊销；节点令牌只能领/交付自己节点的任务，不能看看板、
  不能建项目、不能回滚。主令牌只留在 hub。
- 看板：用主令牌换服务端随机会话 cookie（HttpOnly、有有效期、可注销），
  不再把令牌写进浏览器 localStorage。
- 网络：只面向**同一局域网**。没有做 TLS，不要暴露到公网（别做端口映射）。
- 隔离是**逻辑隔离**：agent 以当前用户身份运行，能读整台机器；`write` 范围是提交后的闸门，
  防的是误改，不防恶意代码。只把令牌发给自己信任的设备。
- 超大文件（>8MB）不参与同步（节点日志会明确提示）；二进制文件可以，但按整文件传。

## 已知边界

- 不跨网段、不走公网穿透（本来就设计成局域网协作）；单个 hub 没有主备。
- HTTP 模式依赖模型按约定返回 JSON；不守规矩的模型需要换提示词或换 `cli` 模式。
- 合并按任务串行应用；冲突必须人工裁决一次（这是故意的，不自动猜），裁决后可以继续跑。
- 没有任务内的多轮流式对话；现在是一任务一次交付 + 验收。
- 大仓库首次派发会做一次全量快照（按 mtime 缓存哈希），之后只传变化。
- `.gitignore` 支持常见写法（`/` 锚定、`**`、目录尾 `/`、`!` 取反、嵌套文件）；
  被忽略目录里的反向例外不生效（与 git 的"父目录被忽略则整棵忽略"一致）。

---

## 环境要求

- **Python 3.8+**，只用标准库，`pip install` 一个包都不用。
- 能执行 shell 就行：Termux、proot-distro、UserLAnd、容器、虚拟机、普通 Linux 都一样。
- 不需要 Android SDK、Gradle、Android Studio，也不打 APK —— 仓库里没有 Android 工程。
- 可选：`git`（hub 合并后自动提交，多一层历史；没有就加 `--no-git`，文件级回滚仍可用）。
- 可选：`pipx install .` / `pip install .` 安装成 `swarm` 命令（`pyproject.toml` 已配好）。

## 测试与 CI

`.github/workflows/ci.yml` 在每次 push 和 PR 自动跑：

| 任务 | 内容 |
|---|---|
| 测试 × Python 3.8 / 3.10 / 3.12 | `compileall` → 零依赖检查 → 全量 unittest |
| demo 冒烟 | 真起 1 个 hub + 3 个节点，跑完一个完整项目 |
| 基准冒烟 | `tools/bench.py` 跑小规模任务图，确认调度链路没退化 |

UDP 广播发现用例默认跳过（云主机、容器常拦 `255.255.255.255`，在那种环境里跑只会误报红）；
CI 只跑单播和回环地址的用例，本地想覆盖广播就 `SWARM_TEST_UDP=1`。
也可以直接点开 [Actions](https://github.com/wangzi5151/ai-swarm/actions) 看最近一次结果。

---

## 许可证

MIT，见 [LICENSE](LICENSE)。
