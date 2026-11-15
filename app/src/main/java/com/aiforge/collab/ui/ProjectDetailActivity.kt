package com.aiforge.collab.ui

import android.os.Bundle
import android.text.TextUtils
import android.view.Gravity
import android.view.ViewGroup
import android.widget.LinearLayout
import android.widget.ProgressBar
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import com.aiforge.collab.App
import com.aiforge.collab.R
import com.aiforge.collab.ai.AiClient
import com.aiforge.collab.ai.PatchParser
import com.aiforge.collab.model.Agent
import com.aiforge.collab.model.CS_CONFLICT
import com.aiforge.collab.model.CS_MERGED
import com.aiforge.collab.model.CS_PENDING
import com.aiforge.collab.model.ChangeSet
import com.aiforge.collab.model.PROVIDER_MOCK
import com.aiforge.collab.model.PROVIDER_OPENAI
import com.aiforge.collab.model.Project
import com.aiforge.collab.model.TASK_FAILED
import com.aiforge.collab.model.TASK_MERGED
import com.aiforge.collab.model.TASK_REVIEW
import com.aiforge.collab.model.TASK_RUNNING
import com.aiforge.collab.model.TASK_TODO
import com.aiforge.collab.model.TaskItem
import com.aiforge.collab.store.MergeResult
import com.aiforge.collab.store.WorkspaceRepo
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch

/**
 * 一个项目的作战室：
 * 工作间 / 任务板 / 补丁队列 / 主工程文件，全部在一页里。
 */
class ProjectDetailActivity : AppCompatActivity() {

    private val repo: WorkspaceRepo
        get() = (application as App).repo

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main)

    private lateinit var content: LinearLayout
    private lateinit var head: TextView
    private lateinit var sub: TextView

    private var pid: String = ""
    private var project: Project? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        pid = intent.getStringExtra("pid").orEmpty()

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(color(R.color.bg))
            setPadding(dp(16f), dp(22f), dp(16f), dp(10f))
        }

        head = textView("", 20f, R.color.text_primary, bold = true)
        root.addView(head)

        sub = textView("", 11f, R.color.text_secondary, bold = true).apply {
            letterSpacing = 0.10f
            setPadding(0, dp(5f), 0, 0)
        }
        root.addView(sub)

        val actions = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(12f), 0, dp(6f))
        }
        actions.addView(
            ghostButton("＋ 工作间") { addAgentFlow() },
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f)
        )
        actions.addView(
            ghostButton("＋ 任务") { addTaskFlow() },
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f).apply { leftMargin = dp(8f) }
        )
        actions.addView(
            ghostButton("自动拆解") { planFlow() },
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f).apply { leftMargin = dp(8f) }
        )
        root.addView(actions)

        content = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        val scroll = ScrollView(this).apply {
            isFillViewport = true
            addView(content)
        }
        root.addView(
            scroll,
            LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f)
        )

        setContentView(root)
    }

    override fun onResume() {
        super.onResume()
        render()
    }

    override fun onDestroy() {
        scope.cancel()
        super.onDestroy()
    }

    // ---------- 渲染 ----------

    private fun render() {
        val p = repo.find(pid)
        if (p == null) {
            toast("项目不存在")
            finish()
            return
        }
        project = p
        content.removeAllViews()

        head.text = p.name
        val merged = p.changesets.count { it.status == CS_MERGED }
        sub.text = "${p.agents.size} 工作间 · ${p.tasks.size} 任务 · 补丁 $merged/${p.changesets.size} 已合并"

        // 工作间
        content.addView(sectionTitle("工作间 · 每个 AI 一个独立沙盒"))
        if (p.agents.isEmpty()) {
            content.addView(hint("还没有工作间。点「＋ 工作间」建一个，或直接点「自动拆解」自动建 4 个。"))
        } else {
            p.agents.forEach { content.addView(agentRow(p, it)) }
        }

        // 任务板
        content.addView(sectionTitle("任务板 · 点任务把它交给对应的工作间"))
        if (p.tasks.isEmpty()) {
            content.addView(hint("任务板是空的。"))
        } else {
            p.tasks.forEach { content.addView(taskRow(p, it)) }
        }

        // 补丁
        content.addView(sectionTitle("补丁队列 · 合并前不碰主工程"))
        if (p.changesets.isEmpty()) {
            content.addView(hint("AI 干完活会交一个补丁包到这里，等你合并或拒绝。"))
        } else {
            p.changesets.forEach { content.addView(patchRow(p, it)) }
        }

        // 主工程
        content.addView(sectionTitle("主工程文件 · 只有合并才会写进来"))
        val files = repo.listMainFiles(p)
        if (files.isEmpty()) {
            content.addView(hint("主工程还是空的。"))
        } else {
            val box = panel()
            files.forEach { (path, size) ->
                box.addView(textView("$path   ${size}B", 12f, R.color.mono_text, mono = true))
            }
            content.addView(box)
        }
    }

    private fun agentRow(p: Project, a: Agent): LinearLayout {
        val box = panel()
        box.isClickable = true
        box.setOnClickListener { editAgentFlow(a) }
        box.setOnLongClickListener {
            confirmDialog("删除工作间", "「${a.name}」的工作间目录和它的历史都会被移除。", "删除") {
                p.agents.removeAll { it.id == a.id }
                p.tasks.forEach { if (it.agentId == a.id) it.agentId = "" }
                repo.save()
                render()
            }
            true
        }

        val top = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        top.addView(
            textView(a.name, 15f, R.color.text_primary, bold = true),
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f)
        )
        top.addView(
            if (a.isMock()) tagChip("本地模拟", R.color.success)
            else tagChip(a.model.ifBlank { "接口" }, R.color.accent)
        )
        box.addView(top)

        box.addView(
            textView(a.role.ifBlank { "（没写专长，长按可删除）" }, 12f, R.color.text_secondary).apply {
                setPadding(0, dp(4f), 0, 0)
            }
        )
        return box
    }

    private fun taskRow(p: Project, t: TaskItem): LinearLayout {
        val box = panel()
        val agent = p.agent(t.agentId)

        val top = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        top.addView(
            textView(t.title, 14f, R.color.text_primary, bold = true),
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f)
        )
        top.addView(statusChip(t.status))
        box.addView(top)

        box.addView(
            textView(
                "指派给：" + (agent?.name ?: "未分配"),
                12f,
                if (agent == null) R.color.warning else R.color.text_secondary
            ).apply { setPadding(0, dp(4f), 0, 0) }
        )

        if (t.detail.isNotBlank()) {
            box.addView(
                textView(t.detail, 12f, R.color.text_secondary).apply {
                    maxLines = 2
                    ellipsize = TextUtils.TruncateAt.END
                    setPadding(0, dp(2f), 0, 0)
                }
            )
        }
        if (t.log.isNotBlank() && t.status == TASK_FAILED) {
            box.addView(
                textView(t.log.take(200), 11f, R.color.danger).apply {
                    setPadding(0, dp(4f), 0, 0)
                }
            )
        }

        val row = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(10f), 0, 0)
        }
        when (t.status) {
            TASK_TODO, TASK_FAILED -> row.addView(
                actionChip(if (t.status == TASK_FAILED) "↻ 重试" else "▶ 交给它做", R.color.accent) {
                    runTask(t)
                }
            )

            TASK_RUNNING -> row.addView(tagChip("工作中…", R.color.accent))
            TASK_REVIEW -> row.addView(
                actionChip("查看补丁", R.color.warning) {
                    p.changesets.firstOrNull { it.taskId == t.id }?.let { viewChangeSet(it) }
                        ?: toast("没找到这个任务的补丁")
                }
            )

            TASK_MERGED -> row.addView(tagChip("已进入主工程", R.color.success))
        }
        if (row.childCount > 0) box.addView(row)

        box.setOnClickListener {
            if (t.status == TASK_TODO || t.status == TASK_FAILED) runTask(t)
            else if (t.status == TASK_REVIEW) {
                p.changesets.firstOrNull { it.taskId == t.id }?.let { viewChangeSet(it) }
            }
        }
        return box
    }

    private fun patchRow(p: Project, cs: ChangeSet): LinearLayout {
        val box = panel()

        val top = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
        }
        top.addView(
            textView(cs.agentName.ifBlank { "未知工作间" }, 14f, R.color.text_primary, bold = true),
            LinearLayout.LayoutParams(0, ViewGroup.LayoutParams.WRAP_CONTENT, 1f)
        )
        top.addView(statusChip(cs.status))
        box.addView(top)

        if (cs.summary.isNotBlank()) {
            box.addView(
                textView(cs.summary, 12f, R.color.text_secondary).apply {
                    setPadding(0, dp(4f), 0, 0)
                    maxLines = 2
                    ellipsize = TextUtils.TruncateAt.END
                }
            )
        }
        box.addView(
            textView(
                "${cs.edits.size} 个文件：" + cs.edits.take(3).joinToString("、") { it.path } +
                    if (cs.edits.size > 3) " 等" else "",
                11f, R.color.mono_text, mono = true
            ).apply { setPadding(0, dp(6f), 0, 0) }
        )

        val row = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            setPadding(0, dp(10f), 0, 0)
        }
        row.addView(actionChip("查看文件", R.color.text_primary) { viewChangeSet(cs) })

        if (cs.status == CS_PENDING || cs.status == CS_CONFLICT) {
            row.addView(
                actionChip("合并进主工程", R.color.success) { mergeFlow(cs) }.apply {
                    setPadding(dp(12f), dp(7f), dp(12f), dp(7f))
                    hp(8f)
                }
            )
            row.addView(actionChip("拒绝", R.color.danger) {
                confirmDialog("拒绝这个补丁", "「${cs.agentName}」这次的 ${cs.edits.size} 个文件不会进入主工程。", "拒绝") {
                    repo.reject(p, cs)
                    render()
                }
            }.apply { hp(8f) })
        }
        box.addView(row)
        return box
    }

    private fun TextView.hp(v: Float) {
        val lp = (layoutParams as? LinearLayout.LayoutParams) ?: LinearLayout.LayoutParams(
            ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT
        )
        lp.leftMargin = dp(v)
        layoutParams = lp
    }

    // ---------- 跑任务 ----------

    private fun runTask(t: TaskItem) {
        val p = project ?: return
        val agent = p.agent(t.agentId)
        if (agent == null) {
            toast("这个任务还没指派工作间")
            return
        }
        if (t.status == TASK_RUNNING) return

        t.status = TASK_RUNNING
        t.log = ""
        repo.save()
        render()

        val dlg = progressDialog("「${agent.name}」正在自己的沙盒里干活…")
        scope.launch {
            try {
                val mainFiles = repo.listMainFiles(p, 60).map { it.first }
                val wsFiles = repo.listWsFiles(p, agent.id)
                val raw = AiClient.runTask(agent, p, t, mainFiles, wsFiles)
                val parsed = PatchParser.parse(raw)
                if (parsed.edits.isEmpty()) {
                    throw IllegalStateException("AI 输出里没有可识别的文件块（格式没按约定）")
                }
                val cs = ChangeSet(
                    taskId = t.id,
                    agentId = agent.id,
                    agentName = agent.name,
                    summary = parsed.summary.ifBlank { "${agent.name} 交付了 ${parsed.edits.size} 个文件" },
                    edits = parsed.edits.toMutableList()
                )
                repo.recordChangeset(p, cs)
                t.status = TASK_REVIEW
                t.log = parsed.summary
                repo.save()
                dlg.dismiss()
                toast("交付 ${parsed.edits.size} 个文件，去补丁队列合并")
                render()
            } catch (e: Throwable) {
                t.status = TASK_FAILED
                t.log = e.message ?: e.toString()
                repo.save()
                dlg.dismiss()
                toast("失败：" + (e.message ?: "未知错误"))
                render()
            }
        }
    }

    // ---------- 合并 ----------

    private fun mergeFlow(cs: ChangeSet) {
        val p = project ?: return
        val first: MergeResult = repo.merge(p, cs, false)
        if (first.ok) {
            toast(first.message)
            render()
            return
        }
        confirmDialog(
            "发现冲突",
            "这些文件已经被别的工作间改过了：\n\n" + first.conflicts.joinToString("\n") +
                "\n\n要强制用「${cs.agentName}」的版本覆盖吗？",
            "强制覆盖"
        ) {
            val forced = repo.merge(p, cs, true)
            toast(if (forced.ok) "已强制合并" else forced.message)
            render()
        }
        render()
    }

    // ---------- 弹窗 ----------

    private fun progressDialog(msg: String): AlertDialog {
        val box = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL
            gravity = Gravity.CENTER_VERTICAL
            setPadding(dp(20f), dp(18f), dp(20f), dp(18f))
        }
        box.addView(ProgressBar(this))
        box.addView(
            textView(msg, 13f, R.color.text_primary).apply {
                setPadding(dp(12f), 0, 0, 0)
            }
        )
        return AlertDialog.Builder(this).setView(box).setCancelable(false).create().apply { show() }
    }

    private fun viewChangeSet(cs: ChangeSet) {
        val box = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(dp(16f), dp(8f), dp(16f), dp(8f))
        }
        box.addView(textView(cs.summary.ifBlank { "（没有说明）" }, 13f, R.color.text_secondary))
        cs.edits.forEach { e ->
            box.addView(
                textView(e.path, 12f, R.color.accent, mono = true, bold = true).apply {
                    setPadding(0, dp(12f), 0, dp(2f))
                }
            )
            box.addView(textView(e.content.take(3000), 11f, R.color.mono_text, mono = true))
        }
        val scroll = ScrollView(this).apply { addView(box) }
        AlertDialog.Builder(this)
            .setTitle("补丁内容 · ${cs.agentName}")
            .setView(scroll)
            .setPositiveButton("关闭", null)
            .show()
    }

    private fun addAgentFlow() {
        val p = project ?: return
        chooseDialog(
            "这个工作间用什么大脑？",
            listOf("本地模拟（免配置，直接能跑通流程）", "OpenAI 兼容接口（用你自己的 key）")
        ) { idx ->
            if (idx == 0) {
                showForm(
                    "新建工作间",
                    listOf(
                        FormField("名字", "AI-0${p.agents.size + 1}"),
                        FormField("专长 / 角色", hint = "例如：数据库与数据层")
                    )
                ) { v ->
                    val name = v.getOrNull(0).orEmpty().ifBlank { "AI-0${p.agents.size + 1}" }
                    p.agents.add(
                        Agent(
                            name = name,
                            role = v.getOrNull(1).orEmpty(),
                            provider = PROVIDER_MOCK
                        )
                    )
                    repo.save()
                    render()
                    toast("工作间「$name」已建好（本地模拟）")
                }
            } else {
                showForm(
                    "新建工作间 · 真实接口",
                    listOf(
                        FormField("名字", "AI-0${p.agents.size + 1}"),
                        FormField("专长 / 角色", hint = "例如：界面与交互"),
                        FormField("接口地址", "https://api.openai.com/v1"),
                        FormField("模型", "gpt-4o-mini"),
                        FormField("API Key", secret = true, hint = "sk-...")
                    )
                ) { v ->
                    val name = v.getOrNull(0).orEmpty().ifBlank { "AI-0${p.agents.size + 1}" }
                    p.agents.add(
                        Agent(
                            name = name,
                            role = v.getOrNull(1).orEmpty(),
                            provider = PROVIDER_OPENAI,
                            baseUrl = v.getOrNull(2).orEmpty().ifBlank { "https://api.openai.com/v1" },
                            model = v.getOrNull(3).orEmpty().ifBlank { "gpt-4o-mini" },
                            apiKey = v.getOrNull(4).orEmpty()
                        )
                    )
                    repo.save()
                    render()
                    toast("工作间「$name」已建好（真实接口）")
                }
            }
        }
    }

    private fun editAgentFlow(a: Agent) {
        showForm(
            "编辑工作间",
            listOf(
                FormField("名字", a.name),
                FormField("专长 / 角色", a.role),
                FormField("接口地址", a.baseUrl),
                FormField("模型", a.model),
                FormField("API Key", a.apiKey, secret = true)
            )
        ) { v ->
            a.name = v.getOrNull(0).orEmpty().ifBlank { a.name }
            a.role = v.getOrNull(1).orEmpty()
            a.baseUrl = v.getOrNull(2).orEmpty().ifBlank { a.baseUrl }
            a.model = v.getOrNull(3).orEmpty().ifBlank { a.model }
            val key = v.getOrNull(4).orEmpty()
            if (key.isNotBlank()) {
                a.apiKey = key
                a.provider = PROVIDER_OPENAI
            }
            repo.save()
            render()
            toast("已更新「${a.name}」")
        }
    }

    private fun addTaskFlow() {
        val p = project ?: return
        if (p.agents.isEmpty()) {
            toast("先建一个工作间，再派任务")
            return
        }
        showForm(
            "新建任务",
            listOf(
                FormField("任务标题", hint = "例如：收支记录的数据库设计"),
                FormField("任务细节", hint = "要它交付什么？")
            )
        ) { v ->
            val title = v.getOrNull(0).orEmpty()
            if (title.isBlank()) {
                toast("标题不能为空")
                return@showForm
            }
            chooseDialog("指派给哪个工作间？", p.agents.map { it.name }) { i ->
                val a = p.agents.getOrNull(i) ?: return@chooseDialog
                p.tasks.add(
                    TaskItem(
                        title = title,
                        detail = v.getOrNull(1).orEmpty(),
                        agentId = a.id
                    )
                )
                repo.save()
                render()
                toast("已把「$title」交给 ${a.name}")
            }
        }
    }

    private fun planFlow() {
        val p = project ?: return
        if (p.agents.isEmpty()) {
            listOf(
                "架构师" to "需求拆解、模块边界、接口定义",
                "实现工程师" to "核心逻辑与数据层",
                "界面工程师" to "界面与交互",
                "测试工程师" to "测试与验收"
            ).forEachIndexed { i, pair ->
                p.agents.add(
                    Agent(name = "AI-0${i + 1} ${pair.first}", role = pair.second, provider = PROVIDER_MOCK)
                )
            }
        }
        val plan = listOf(
            "项目目标与模块划分" to 0,
            "数据模型与核心逻辑" to 1,
            "界面与交互" to 2,
            "测试清单与验收" to 3
        )
        plan.forEach { pair ->
            val agent = p.agents.getOrNull(pair.second) ?: p.agents.first()
            p.tasks.add(
                TaskItem(
                    title = pair.first,
                    detail = "围绕目标「${p.goal.ifBlank { p.name }}」完成「${pair.first}」这一块。",
                    agentId = agent.id
                )
            )
        }
        repo.save()
        render()
        toast("已建 ${p.agents.size} 个工作间、拆出 ${plan.size} 个任务")
    }
}
