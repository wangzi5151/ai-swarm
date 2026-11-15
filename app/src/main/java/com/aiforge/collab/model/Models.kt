package com.aiforge.collab.model

import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID

const val PROVIDER_MOCK = "mock"
const val PROVIDER_OPENAI = "openai"

const val TASK_TODO = "TODO"
const val TASK_RUNNING = "RUNNING"
const val TASK_REVIEW = "REVIEW"
const val TASK_MERGED = "MERGED"
const val TASK_FAILED = "FAILED"

const val CS_PENDING = "PENDING"
const val CS_MERGED = "MERGED"
const val CS_CONFLICT = "CONFLICT"
const val CS_REJECTED = "REJECTED"

fun newId(): String = UUID.randomUUID().toString().replace("-", "").take(10)

data class FileEdit(val path: String, val content: String)

/**
 * 一个"工作间"：一个 AI 的身份 + 连接方式。
 * provider = mock 时不联网，本地模拟产出，用来演示整条流水线。
 */
class Agent(
    var id: String = newId(),
    var name: String = "",
    var role: String = "",
    var provider: String = PROVIDER_MOCK,
    var baseUrl: String = "https://api.openai.com/v1",
    var apiKey: String = "",
    var model: String = "gpt-4o-mini"
) {
    fun isMock(): Boolean = provider == PROVIDER_MOCK

    fun toJson(): JSONObject = JSONObject().apply {
        put("id", id)
        put("name", name)
        put("role", role)
        put("provider", provider)
        put("baseUrl", baseUrl)
        put("apiKey", apiKey)
        put("model", model)
    }

    companion object {
        fun fromJson(o: JSONObject) = Agent(
            id = o.optString("id", newId()),
            name = o.optString("name", "AI"),
            role = o.optString("role", ""),
            provider = o.optString("provider", PROVIDER_MOCK),
            baseUrl = o.optString("baseUrl", "https://api.openai.com/v1"),
            apiKey = o.optString("apiKey", ""),
            model = o.optString("model", "gpt-4o-mini")
        )
    }
}

class TaskItem(
    var id: String = newId(),
    var title: String = "",
    var detail: String = "",
    var agentId: String = "",
    var status: String = TASK_TODO,
    var log: String = ""
) {
    fun toJson(): JSONObject = JSONObject().apply {
        put("id", id)
        put("title", title)
        put("detail", detail)
        put("agentId", agentId)
        put("status", status)
        put("log", log)
    }

    companion object {
        fun fromJson(o: JSONObject) = TaskItem(
            id = o.optString("id", newId()),
            title = o.optString("title"),
            detail = o.optString("detail"),
            agentId = o.optString("agentId"),
            status = o.optString("status", TASK_TODO),
            log = o.optString("log")
        )
    }
}

/** 一个 AI 交回来的一批改动（补丁包），必须先合并才进入主工程。 */
class ChangeSet(
    var id: String = newId(),
    var taskId: String = "",
    var agentId: String = "",
    var agentName: String = "",
    var summary: String = "",
    var edits: MutableList<FileEdit> = mutableListOf(),
    var status: String = CS_PENDING,
    var createdAt: Long = System.currentTimeMillis()
) {
    fun paths(): List<String> = edits.map { it.path }

    fun toJson(): JSONObject = JSONObject().apply {
        put("id", id)
        put("taskId", taskId)
        put("agentId", agentId)
        put("agentName", agentName)
        put("summary", summary)
        put("status", status)
        put("createdAt", createdAt)
        val arr = JSONArray()
        edits.forEach { e ->
            arr.put(JSONObject().put("path", e.path).put("content", e.content))
        }
        put("edits", arr)
    }

    companion object {
        fun fromJson(o: JSONObject): ChangeSet {
            val list = mutableListOf<FileEdit>()
            val arr = o.optJSONArray("edits") ?: JSONArray()
            for (i in 0 until arr.length()) {
                val e = arr.optJSONObject(i) ?: continue
                list.add(FileEdit(e.optString("path"), e.optString("content")))
            }
            return ChangeSet(
                id = o.optString("id", newId()),
                taskId = o.optString("taskId"),
                agentId = o.optString("agentId"),
                agentName = o.optString("agentName"),
                summary = o.optString("summary"),
                edits = list,
                status = o.optString("status", CS_PENDING),
                createdAt = o.optLong("createdAt", System.currentTimeMillis())
            )
        }
    }
}

/** 一个大项目：一个主工程 + 若干工作间 + 任务板 + 补丁队列。 */
class Project(
    var id: String = newId(),
    var name: String = "",
    var goal: String = "",
    val agents: MutableList<Agent> = mutableListOf(),
    val tasks: MutableList<TaskItem> = mutableListOf(),
    val changesets: MutableList<ChangeSet> = mutableListOf(),
    /** 主工程里每个文件最后是被哪个工作间写进去的，用来判断冲突。 */
    val owners: MutableMap<String, String> = mutableMapOf(),
    var createdAt: Long = System.currentTimeMillis()
) {
    fun agent(id: String?): Agent? = agents.firstOrNull { it.id == id }

    fun toJson(): JSONObject = JSONObject().apply {
        put("id", id)
        put("name", name)
        put("goal", goal)
        put("createdAt", createdAt)
        put("agents", JSONArray().also { a -> agents.forEach { a.put(it.toJson()) } })
        put("tasks", JSONArray().also { a -> tasks.forEach { a.put(it.toJson()) } })
        put("changesets", JSONArray().also { a -> changesets.forEach { a.put(it.toJson()) } })
        put("owners", JSONObject().also { o -> owners.forEach { (k, v) -> o.put(k, v) } })
    }

    companion object {
        fun fromJson(o: JSONObject): Project {
            val p = Project(
                id = o.optString("id", newId()),
                name = o.optString("name"),
                goal = o.optString("goal"),
                createdAt = o.optLong("createdAt", System.currentTimeMillis())
            )
            o.optJSONArray("agents")?.let { arr ->
                for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { p.agents.add(Agent.fromJson(it)) }
            }
            o.optJSONArray("tasks")?.let { arr ->
                for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { p.tasks.add(TaskItem.fromJson(it)) }
            }
            o.optJSONArray("changesets")?.let { arr ->
                for (i in 0 until arr.length()) arr.optJSONObject(i)?.let { p.changesets.add(ChangeSet.fromJson(it)) }
            }
            o.optJSONObject("owners")?.let { ob ->
                ob.keys().forEach { k -> p.owners[k] = ob.optString(k) }
            }
            return p
        }
    }
}
