package com.aiforge.collab.store

import android.content.Context
import com.aiforge.collab.ai.PatchParser
import com.aiforge.collab.model.ChangeSet
import com.aiforge.collab.model.CS_CONFLICT
import com.aiforge.collab.model.CS_MERGED
import com.aiforge.collab.model.CS_PENDING
import com.aiforge.collab.model.CS_REJECTED
import com.aiforge.collab.model.Project
import com.aiforge.collab.model.TASK_MERGED
import com.aiforge.collab.model.TASK_REVIEW
import org.json.JSONArray
import org.json.JSONObject
import java.io.File

data class MergeResult(val ok: Boolean, val conflicts: List<String>, val message: String)

/**
 * 项目仓库 + 文件系统布局：
 *
 * filesDir/forge/
 *   projects.json
 *   p_<项目id>/
 *     main/            主工程（只有"合并"才能写这里）
 *     ws/<工作间id>/    每个 AI 自己的独立工作间
 */
class WorkspaceRepo(context: Context) {

    private val root = File(context.filesDir, "forge")
    private val storeFile = File(root, "projects.json")
    private val lock = Any()

    val projects = mutableListOf<Project>()

    init {
        root.mkdirs()
        load()
    }

    fun find(id: String?): Project? = projects.firstOrNull { it.id == id }

    fun fileExists(id: String): Boolean = File(root, "p_$id").exists()

    // ---------- 基础路径 ----------

    fun projectDir(p: Project): File = File(root, "p_${p.id}")

    fun mainDir(p: Project): File = File(projectDir(p), "main").apply { mkdirs() }

    fun wsDir(p: Project, agentId: String): File =
        File(projectDir(p), "ws/$agentId").apply { mkdirs() }

    // ---------- 项目 ----------

    fun createProject(name: String, goal: String): Project {
        val p = Project(name = name.trim(), goal = goal.trim())
        projects.add(p)
        projectDir(p).mkdirs()
        mainDir(p).mkdirs()
        File(mainDir(p), "PROJECT.md").writeText(
            "# ${p.name}\n\n## 目标\n\n${p.goal}\n\n## 规则\n\n- 只有合并通过的补丁才会进入这里\n"
        )
        p.owners["PROJECT.md"] = "system"
        save()
        return p
    }

    fun deleteProject(p: Project) {
        projects.removeAll { it.id == p.id }
        projectDir(p).deleteRecursively()
        save()
    }

    // ---------- 补丁 ----------

    /** AI 干完活：把文件写进它自己的工作间，并生成一个待合并的补丁包。 */
    fun recordChangeset(p: Project, cs: ChangeSet) {
        synchronized(lock) {
            val ws = wsDir(p, cs.agentId)
            for (e in cs.edits) {
                val safe = PatchParser.sanitize(e.path) ?: continue
                val f = File(ws, safe)
                f.parentFile?.mkdirs()
                f.writeText(e.content)
            }
            p.changesets.add(0, cs)
            save()
        }
    }

    /**
     * 合并补丁进主工程。
     * 冲突规则：同一个文件如果已经被另一个工作间写过，且内容不一致，就算冲突，交给人决定。
     */
    fun merge(p: Project, cs: ChangeSet, force: Boolean): MergeResult {
        synchronized(lock) {
            if (cs.status == CS_MERGED) return MergeResult(true, emptyList(), "已经合并过了")
            val main = mainDir(p)
            val conflicts = ArrayList<String>()
            val ownersToWrite = LinkedHashMap<String, String>()
            val writes = LinkedHashMap<String, String>()

            for (e in cs.edits) {
                val safe = PatchParser.sanitize(e.path) ?: continue
                val target = File(main, safe)
                val existing = if (target.exists()) target.readText() else null
                val owner = p.owners[safe]

                // 新文件、或没人动过、或是自己上次写的 -> 直接过
                val foreign = owner != null && owner != "system" && owner != cs.agentId
                val changed = existing != null && existing != e.content
                if (foreign && changed && !force) {
                    conflicts.add(safe)
                    continue
                }
                writes[safe] = e.content
                ownersToWrite[safe] = cs.agentId
            }

            if (conflicts.isNotEmpty()) {
                cs.status = CS_CONFLICT
                save()
                return MergeResult(false, conflicts, "有 ${conflicts.size} 个文件被别的工作间改过")
            }

            for ((path, content) in writes) {
                val f = File(main, path)
                f.parentFile?.mkdirs()
                f.writeText(content)
            }
            p.owners.putAll(ownersToWrite)
            cs.status = CS_MERGED
            p.tasks.firstOrNull { it.id == cs.taskId }?.let { it.status = TASK_MERGED }
            save()
            return MergeResult(true, emptyList(), "已合并 ${writes.size} 个文件")
        }
    }

    fun reject(p: Project, cs: ChangeSet) {
        cs.status = CS_REJECTED
        p.tasks.firstOrNull { it.id == cs.taskId }?.let {
            if (it.status == TASK_REVIEW) it.status = com.aiforge.collab.model.TASK_TODO
        }
        save()
    }

    // ---------- 文件列表 ----------

    /** 某个工作间沙盒里的文件（相对路径）。 */
    fun listWsFiles(p: Project, agentId: String, limit: Int = 60): List<String> {
        val base = wsDir(p, agentId)
        if (!base.exists()) return emptyList()
        return base.walkTopDown()
            .filter { it.isFile }
            .map { it.relativeTo(base).path.replace('\\', '/') }
            .take(limit)
            .sorted()
            .toList()
    }

    fun listMainFiles(p: Project, limit: Int = 200): List<Pair<String, Long>> {
        val base = mainDir(p)
        if (!base.exists()) return emptyList()
        val out = ArrayList<Pair<String, Long>>()
        base.walkTopDown()
            .filter { it.isFile }
            .forEach {
                val rel = it.relativeTo(base).path.replace('\\', '/')
                out.add(rel to it.length())
                if (out.size >= limit) return@forEach
            }
        out.sortBy { it.first }
        return out
    }

    fun readWsFile(p: Project, agentId: String, path: String): String? {
        val safe = PatchParser.sanitize(path) ?: return null
        val f = File(wsDir(p, agentId), safe)
        return if (f.exists()) f.readText() else null
    }

    // ---------- 持久化 ----------

    private fun load() {
        if (!storeFile.exists()) return
        try {
            val arr = JSONArray(storeFile.readText())
            for (i in 0 until arr.length()) {
                arr.optJSONObject(i)?.let { projects.add(Project.fromJson(it)) }
            }
        } catch (t: Throwable) {
            // 存档损坏时不要让应用起不来
            storeFile.renameTo(File(root, "projects.broken.json"))
        }
    }

    fun save() {
        synchronized(lock) {
            try {
                val arr = JSONArray()
                projects.forEach { arr.put(it.toJson()) }
                File(root, "projects.json.tmp").writeText(arr.toString())
                File(root, "projects.json.tmp").renameTo(storeFile)
            } catch (t: Throwable) {
                // 忽略写盘失败，内存里的数据仍然可用
            }
        }
    }
}
