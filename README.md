# Swarm —— 局域网多 AI 协同调度器

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

**和上一版的区别**：`app/` 里那个 Android 应用是"在一个 App 内开多个虚拟工作间"，方向不对，已保留但不使用。
现在这套东西不要求你在一个 App 里装多个 Linux —— 它面向**已经存在的、分散在不同机器上的 Linux 和 agent**。

---

## 一分钟先看效果

在任意一台有 Python 3.8+ 的机器上：

```bash
./bin/swarm demo
```

它会在本机起 1 个 hub + 3 个模拟节点，把一个"记账 App"拆成 6 个任务并发跑完，
你能看到任务流水线、文件合并、git 提交，最后打印产出目录。全过程约 13 秒。

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
令牌   3f9a…（一长串）
```

这个令牌 = 整个协作网的钥匙，**别外传**。它只在本机 `~/.swarm/token` 里。

### 2. 每台干活的机器跑一个节点

把 `swarm/` 目录和 `bin/swarm` 拷过去（或者整个仓库 `git clone`），然后：

```bash
./bin/swarm node --discover --token 3f9a… --name 手机A --slots 2 \
  --agent cli --cmd 'aider --yes --no-auto-commits --message {prompt}'
```

- `--discover`：UDP 广播自动找 hub，不用记 IP（找不到就用 `--hub 192.168.1.23:47770`）
- `--slots 2`：这台机器同时干 2 个任务
- `--name`：看板上显示的名字
- `--agent`：这个节点用谁干活，见下一节

### 3. 手机上看

浏览器打开 `http://192.168.1.23:47770/`，输入令牌（或在地址里带一次 `/?t=令牌`）。
能看到：在线节点、每个任务的状态/耗时/日志、冲突裁决按钮、事件流。

### 4. 交一个项目

```bash
./bin/swarm submit -f 需求.md --watch
```

自动拆成"公共层 → 并行模块 → 集成 → 文档"的任务图，派给所有在线节点。

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

### 接口类（`--agent http`，OpenAI 兼容）

```bash
./bin/swarm node --discover --token 3f9a… --agent http \
  --llm-base http://127.0.0.1:11434/v1 --model qwen2.5-coder:14b
```

LM Studio、vLLM、llama.cpp server、Ollama、以及各家云 API 都吃这一套。
节点会用自己机器的模型和 Key，不同节点互不影响。

### 不同任务用不同 agent

在 `plan.json` 里给单个任务指定命令/模型，节点会优先用任务里的：

```json
{"tasks": [
  {"id": "t-ui", "title": "界面", "write": ["src/ui/**"],
   "cmd": "aider --yes --message {prompt} {files}"},
  {"id": "t-api", "title": "接口", "write": ["src/api/**"],
   "cmd": "claude -p {prompt}", "model": "opus"}
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

---

## 它是怎么保证"不互相改坏"的

这是整套设计里最要紧的部分，四道闸：

1. **任务范围（write）**：每个任务只允许改指定目录。节点在临时目录里干活，
   越界改动的文件**不会被上传**，hub 还会因为收到越界内容而记冲突。
2. **主工程单写者**：只有 hub 写主工程，节点永远碰不到主工程。
3. **内容哈希比对**：合并时校验"文件还是不是我发出去的那一版"（sha256）。
   变了 ⇒ 说明别的任务动过 ⇒ 判冲突，把两边版本都留着等你裁决，绝不静默覆盖。
4. **调度期就避开冲突**：写范围重叠的任务不会同时派发（`scopes_overlap`），
   所以并行任务天然不碰同一个文件；真碰上了也只会变成"待裁决"，不会丢数据。

裁决在看板上：`用节点的版本` / `保留主工程`，点一下就落盘并留 git 记录。

---

## 为什么快

| 机制 | 效果 |
|---|---|
| 长轮询领任务（25s 挂起） | 任务一空出来，节点立刻拿到，没有轮询空转 |
| 依赖完成即解锁下游 | 流水线式推进，不等整批任务做完 |
| 按写范围分片 | 4 个模块就在 4 台机器上真并行，而不是抢同一份代码 |
| 内容寻址 blob（sha256） | 同样的文件全网只传一次，跨任务复用缓存 |
| 8 路并发拉取/推送 + keep-alive | 小文件多的时候不会一个个等 |
| 租约 + 心跳 | 谁掉线，它手上的任务 25 秒内回到队列，不会卡死整个项目 |

本机 demo 里：6 个任务、3 个节点、13 秒跑完（含两次依赖等待）。

---

## 命令清单

```bash
./bin/swarm hub        [--root ~/.swarm] [--port 47770] [--no-git] [--lease 180] [--stale 25]
./bin/swarm node       --discover | --hub ip:port  --token …  [--slots 2] [--agent mock|cli|http]
./bin/swarm submit     -f spec.md [--watch] [--plan plan.json] [--hub …] [--token …]
./bin/swarm status     [--json]
./bin/swarm logs       <project> <task>
./bin/swarm control    <project> pause|resume|cancel|finish [--task id]
./bin/swarm plan       -f spec.md [-o plan.json]        # 只看拆解结果，不派发
./bin/swarm token      [--root ~/.swarm]                # 忘记令牌时查
./bin/swarm export     p1 -o out.tar.gz                 # 打包合并后的项目
./bin/swarm demo       [--nodes 3] [--keep]
```

其它机器的环境变量写法（少打字）：

```bash
export SWARM_HUB=192.168.1.23:47770
export SWARM_TOKEN=3f9a…
export SWARM_AGENT_CMD='aider --yes --message {prompt}'
./bin/swarm node --discover
```

---

## 需求文档怎么写

默认按 `## 模块名 (目录/**)` 切分；没写标题就走默认脚手架（公共层 → 5 个模块 → 集成 → 文档）。

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
改完再 `./bin/swarm submit --plan plan.json`。任务字段：`id/title/prompt/read/write/deps/priority/agent/cmd/model`。

---

## 掉线 / 卡住怎么办

- **节点被系统杀了**：心跳停 → 25s 后 hub 判定失联，任务回队列，别的节点接手；重试次数用完才标失败。
- **跑到一半断电**：hub 的状态每 2 秒落盘，重启 hub 后未完成任务自动回队列。
- **某个任务一直失败**：看板上点"重试"，或 `./bin/swarm control p1 retry --task t-api`。
- **整体暂停**：看板"暂停"，或 `./bin/swarm control p1 pause`。
- 调参：`--stale 25`（多久算掉线）、`--lease 180`（租约秒数）、`--max-attempts 2`。

---

## 目录结构

```
swarm/
  proto.py       HTTP + HMAC 签名 + keep-alive 客户端
  store.py       内容寻址存储（sha256）
  workspace.py   快照 / 铺设 / 差异 / 范围判定
  planner.py     spec → 任务图（含环检测、关键路径优先级）
  hub.py         调度、租约、合并、冲突裁决、看板 API
  node.py        发现、多槽并发、拉取/回传
  agents.py      mock / cli / http 三种适配器
  discover.py    UDP 自动发现
  dashboard.py   手机看板（单文件 HTML）
  cli.py         命令行
  demo.py        一条命令演示
bin/swarm        启动器
tests/           unittest（12 个用例，含掉线重派、冲突裁决、越界拦截）
```

跑测试：

```bash
python3 -m unittest discover -s tests -v
```

---

## 安全边界（说清楚）

- 鉴权：每个请求用令牌做 HMAC-SHA256 签名 + 时间戳（±180s），**令牌本身不上网**；
  看板用一次性令牌换 cookie。
- 网络：只面向**同一局域网**。没有做 TLS，不要暴露到公网（别做端口映射）。
- 节点信任：能连上 hub 的节点等于能提交代码改动 —— 所以令牌只发给自己的设备。
- 超大文件（>8MB）不参与同步；二进制文件可以，但按整文件传。

## 已知边界

- 不跨网段、不走公网穿透（本来就设计成局域网协作）。
- HTTP 模式依赖模型按约定返回 JSON；不守规矩的模型需要换个提示词或换 `cli` 模式。
- 合并是"按任务顺序串行应用"，冲突需要人工裁决一次（这是故意的，不自动猜）。
- 还没有做单任务内的增量流式对话；现在是一任务一次交付。

---

## 和 `app/` 的关系

`app/` 是上一版思路（单个 Android App 内开多个虚拟工作间）的产物，可以编译出 APK，但**不是这条路线要的东西**。
保留是为了不丢历史（旧说明在 `docs-legacy-app.md`）；新功能都在 `swarm/` 里，不需要 Gradle、不需要 Android SDK。
如果之后想要"手机上点两下就起 hub"，可以再做一个只做进程管理 + 内嵌网页的壳，把 `swarm hub` 包进去。
