#!/bin/sh
# 各节点用什么 agent —— 挑一条改改就能用。
# 用法：./bin/swarm node --discover --token XXX --agent cli --cmd '<下面某一条>'
# 建议再加 --verify 'python3 -m compileall -q src'：验收失败的改动不会进主工程。

# 1) Aider（最省事：它自己会读文件、改文件、提交）
CMD_AIDER='aider --yes --no-auto-commits --message "{prompt}" {files}'

# 2) Aider + 本地 ollama 模型（不用云 API）
CMD_AIDER_OLLAMA='aider --model ollama/qwen2.5-coder:14b --yes --no-auto-commits --message "{prompt}" {files}'

# 3) Claude Code
CMD_CLAUDE='claude -p "{prompt}"'

# 4) Codex CLI
CMD_CODEX='codex exec "{prompt}"'

# 5) 自己写的脚本：约定 argv[1]=prompt，工作目录就是任务目录
CMD_MINE='python3 /data/agent/mine.py "{prompt}"'

# 6) 只认 stdin 的工具：把任务说明文件喂进去
CMD_STDIN='cat {workdir}/.swarm-task.md | my-agent --auto'

# 7) 带验收命令的完整例子
#    ./bin/swarm node --discover --token XXX --name 手机A --slots 2 \
#      --agent cli --cmd "$CMD_AIDER" --verify 'python3 -m compileall -q src'

# 8) 完全交给 OpenAI 兼容接口（不用命令行 agent）
#    ./bin/swarm node --discover --token XXX --agent http \
#      --llm-base http://127.0.0.1:11434/v1 --model qwen2.5-coder:14b

# 9) 失败重试时，上次的错误会出现在这些地方，喂给你的 agent：
#    - prompt 末尾（节点自动拼）
#    - {workdir}/.swarm-feedback.md
#    - 环境变量 SWARM_FEEDBACK
#    - 命令模板占位符 {feedback}
CMD_READ_FEEDBACK='cat {workdir}/.swarm-feedback.md | my-agent --prompt "{prompt}"'

# 10) hub 开了 TLS：节点加 --tls-pin，或让 --discover 自动带指纹
#    ./bin/swarm node --discover --token XXX --name 手机A \
#      --agent cli --cmd "$CMD_AIDER" --verify 'python3 -m compileall -q src'

echo "请从 $0 里挑一条，改掉 agent 名字后传给 --cmd"
