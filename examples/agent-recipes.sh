#!/bin/sh
# 各节点用什么 agent —— 挑一条改改就能用。
# 用法：./bin/swarm node --discover --token XXX --agent cli --cmd '<下面某一条>'

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

# 7) 完全交给 OpenAI 兼容接口（不用命令行 agent）
#    ./bin/swarm node --discover --token XXX --agent http \
#      --llm-base http://127.0.0.1:11434/v1 --model qwen2.5-coder:14b

echo "请从 $0 里挑一条，改掉 agent 名字后传给 --cmd"
