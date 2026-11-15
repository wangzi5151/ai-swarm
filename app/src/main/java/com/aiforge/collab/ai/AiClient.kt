package com.aiforge.collab.ai

import com.aiforge.collab.model.Agent
import com.aiforge.collab.model.Project
import com.aiforge.collab.model.TaskItem
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.HttpURLConnection
import java.net.URL

/**
 * 唯一对外的 AI 调用入口。
 * - mock 工作间：本地模拟，不需要网络和密钥，用来完整跑通流程。
 * - openai 工作间：任何 OpenAI 兼容接口（/chat/completions）。
 */
object AiClient {

    suspend fun runTask(
        agent: Agent,
        project: Project,
        task: TaskItem,
        mainFiles: List<String> = emptyList(),
        wsFiles: List<String> = emptyList()
    ): String =
        withContext(Dispatchers.IO) {
            if (agent.isMock()) {
                // 模拟一点"思考时间"，让手机上的状态流转看得见
                Thread.sleep(700L + (task.title.length * 37L) % 900L)
                MockBrain.respond(agent, project, task)
            } else {
                val prompt = buildPrompt(agent, project, task, mainFiles, wsFiles)
                httpChat(agent, prompt)
            }
        }

    fun buildPrompt(
        agent: Agent,
        project: Project,
        task: TaskItem,
        mainFiles: List<String> = emptyList(),
        wsFiles: List<String> = emptyList()
    ): String = buildString {
        append("你是「").append(agent.name).append("」")
        if (agent.role.isNotBlank()) append("，角色：").append(agent.role)
        append("。\n")
        append("你只负责这个大型项目里的一块工作，在自己独立的工作间里产出文件，不要动别人的文件。\n\n")
        append("项目：").append(project.name).append('\n')
        append("项目目标：").append(project.goal).append("\n\n")
        append("你这次的任务：").append(task.title).append('\n')
        if (task.detail.isNotBlank()) append("任务细节：").append(task.detail).append('\n')
        append('\n')
        if (mainFiles.isNotEmpty()) {
            append("主工程现有文件（只读参考，不要无故整份重写）：\n")
            mainFiles.take(40).forEach { append("- ").append(it).append('\n') }
            append('\n')
        }
        if (wsFiles.isNotEmpty()) {
            append("你自己的工作间里已有这些文件（可以改）：\n")
            wsFiles.take(40).forEach { append("- ").append(it).append('\n') }
            append('\n')
        }
        append("输出格式（必须严格遵守，否则系统解析失败）：\n")
        append("1. 每个要新增或修改的文件，用下面这个块给出（可以有多个）：\n")
        append("<<<FILE: 相对路径\n文件完整内容\n>>>END\n")
        append("2. 最后单独一行：<<<SUMMARY: 一句话说明你做了什么改动\n")
        append("3. 不要输出任何多余解释，不要使用 markdown 代码围栏。\n")
        append("4. 文件路径使用相对路径，例如 src/main/kotlin/app/Main.kt。\n")
    }

    private fun httpChat(agent: Agent, prompt: String): String {
        val base = agent.baseUrl.trimEnd('/')
        val url = URL(if (base.endsWith("/chat/completions")) base else "$base/chat/completions")
        val conn = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            connectTimeout = 20_000
            readTimeout = 120_000
            doOutput = true
            setRequestProperty("Content-Type", "application/json; charset=utf-8")
            setRequestProperty("Authorization", "Bearer " + agent.apiKey.trim())
        }
        val body = JSONObject().apply {
            put("model", agent.model.ifBlank { "gpt-4o-mini" })
            put("temperature", 0.2)
            put("messages", JSONArray().apply {
                put(JSONObject().put("role", "system").put("content", "你是一个严谨的工程实现者，只输出约定的文件块格式。"))
                put(JSONObject().put("role", "user").put("content", prompt))
            })
        }.toString()

        try {
            conn.outputStream.use { os: OutputStream ->
                os.write(body.toByteArray(Charsets.UTF_8))
                os.flush()
            }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val text = stream?.use { s ->
                BufferedReader(InputStreamReader(s, Charsets.UTF_8)).readText()
            }.orEmpty()
            if (code !in 200..299) {
                throw IllegalStateException("接口返回 $code：${text.take(300)}")
            }
            val obj = JSONObject(text)
            val choices = obj.optJSONArray("choices")
                ?: throw IllegalStateException("返回里没有 choices")
            if (choices.length() == 0) throw IllegalStateException("choices 为空")
            val msg = choices.getJSONObject(0).optJSONObject("message")
                ?: throw IllegalStateException("返回里没有 message")
            return msg.optString("content", "")
        } finally {
            conn.disconnect()
        }
    }
}

/**
 * 本地模拟大脑：不联网也能跑通"任务 → 工作间产出 → 补丁 → 合并"。
 * 不同工作间产出的同名文件内容不同，所以能真实触发冲突检测。
 */
object MockBrain {

    fun respond(agent: Agent, project: Project, task: TaskItem): String {
        val slug = slugOf(task.title)
        val sb = StringBuilder()

        sb.append("<<<FILE: docs/").append(slug).append(".md\n")
        sb.append("# ").append(task.title).append("\n\n")
        sb.append("- 负责工作间：").append(agent.name).append('\n')
        if (agent.role.isNotBlank()) sb.append("- 角色：").append(agent.role).append('\n')
        sb.append("- 所属项目：").append(project.name).append("\n\n")
        sb.append("## 目标\n\n").append(project.goal.ifBlank { "（未填写）" }).append("\n\n")
        sb.append("## 本任务范围\n\n").append(task.detail.ifBlank { "由 " + agent.name + " 自行拆解。" }).append("\n\n")
        sb.append("## 步骤\n\n1. 明确输入输出\n2. 实现核心逻辑\n3. 自测并交给主工程\n")
        sb.append(">>>END\n\n")

        sb.append("<<<FILE: src/main/kotlin/app/").append(slug).append(".kt\n")
        sb.append("package app\n\n")
        sb.append("// 由 ").append(agent.name).append(" 的工作间生成\n")
        sb.append("class ").append(slug).append(" {\n")
        sb.append("    fun describe(): String = \"").append(escape(task.title)).append("\"\n\n")
        sb.append("    fun run(input: String): String {\n")
        sb.append("        // TODO(").append(agent.name.replace('"', '\'')).append("): 实现 ").append(escape(task.title)).append("\n")
        sb.append("        return input\n")
        sb.append("    }\n")
        sb.append("}\n")
        sb.append(">>>END\n\n")

        sb.append("<<<FILE: tests/").append(slug).append("Spec.md\n")
        sb.append("# ").append(slug).append(" 验收清单\n\n")
        sb.append("- [ ] 实现 ").append(task.title).append('\n')
        sb.append("- [ ] 边界输入不崩溃\n")
        sb.append("- [ ] 由 ").append(agent.name).append(" 交付给主工程\n")
        sb.append(">>>END\n\n")

        sb.append("<<<SUMMARY: ").append(agent.name).append(" 交付 ").append(slug)
            .append("（文档 + 代码骨架 + 验收清单）\n")
        return sb.toString()
    }

    private fun escape(s: String): String = s.replace("\\", "\\\\").replace("\"", "\\\"")

    private fun slugOf(title: String): String {
        val cleaned = title.filter { it.isLetterOrDigit() }
        val base = if (cleaned.isBlank()) "Module" else cleaned.take(18)
        return base.replaceFirstChar { c -> if (c.isLowerCase()) c.uppercaseChar() else c }
    }
}
