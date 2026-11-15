package com.aiforge.collab.ui

import android.content.Intent
import android.os.Bundle
import android.text.TextUtils
import android.view.ViewGroup
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import androidx.appcompat.app.AppCompatActivity
import com.aiforge.collab.App
import com.aiforge.collab.R
import com.aiforge.collab.model.CS_MERGED
import com.aiforge.collab.model.Project
import com.aiforge.collab.store.WorkspaceRepo

class MainActivity : AppCompatActivity() {

    private val repo: WorkspaceRepo
        get() = (application as App).repo

    private lateinit var listBox: LinearLayout
    private lateinit var sub: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(color(R.color.bg))
            setPadding(dp(16f), dp(26f), dp(16f), dp(14f))
        }

        root.addView(textView("AI 协作工坊", 26f, R.color.text_primary, bold = true))

        sub = textView("", 11f, R.color.text_secondary, bold = true).apply {
            letterSpacing = 0.12f
            setPadding(0, dp(6f), 0, dp(14f))
        }
        root.addView(sub)

        listBox = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        val scroll = ScrollView(this).apply {
            isFillViewport = true
            addView(listBox)
        }
        root.addView(
            scroll,
            LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f)
        )

        root.addView(
            primaryButton("＋ 新建大型项目") { newProjectFlow() },
            LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = dp(10f) }
        )

        setContentView(root)
    }

    override fun onResume() {
        super.onResume()
        render()
    }

    private fun render() {
        listBox.removeAllViews()
        sub.text = "独立工作间 · 补丁制合并 · 共 ${repo.projects.size} 个项目"

        if (repo.projects.isEmpty()) {
            listBox.addView(
                hint(
                    "还没有项目。\n\n" +
                        "点下面的「新建大型项目」，给它一个目标（比如：做一个记账 App）。\n\n" +
                        "然后每个 AI 会拿到一个独立工作间，各自干活、各自交补丁，" +
                        "只有你点「合并」的改动才会进入主工程。"
                )
            )
            return
        }

        repo.projects.sortedByDescending { it.createdAt }.forEach { listBox.addView(projectCard(it)) }
    }

    private fun projectCard(p: Project): LinearLayout {
        val card = panel()
        card.isClickable = true
        card.setOnClickListener { open(p) }
        card.setOnLongClickListener {
            confirmDialog("删除项目", "「${p.name}」以及它的工作间、补丁都会被删掉。", "删除") {
                repo.deleteProject(p)
                render()
                toast("已删除 ${p.name}")
            }
            true
        }

        card.addView(textView(p.name, 16f, R.color.text_primary, bold = true))
        if (p.goal.isNotBlank()) {
            card.addView(
                textView(p.goal, 13f, R.color.text_secondary).apply {
                    maxLines = 2
                    ellipsize = TextUtils.TruncateAt.END
                    setPadding(0, dp(4f), 0, 0)
                }
            )
        }
        val merged = p.changesets.count { it.status == CS_MERGED }
        val pending = p.changesets.count { it.status != CS_MERGED }
        card.addView(
            textView(
                "${p.agents.size} 工作间 · ${p.tasks.size} 任务 · 已合并 $merged · 待处理 $pending",
                11f, R.color.mono_text, mono = true
            ).apply { setPadding(0, dp(8f), 0, 0) }
        )
        return card
    }

    private fun open(p: Project) {
        startActivity(
            Intent(this, ProjectDetailActivity::class.java).putExtra("pid", p.id)
        )
    }

    private fun newProjectFlow() {
        showForm(
            "新建大型项目",
            listOf(
                FormField("项目名称", hint = "例如：记账 App"),
                FormField("项目目标（越具体越好）", hint = "例如：记录收支、图表统计、数据导出，本地存储优先")
            )
        ) { values ->
            val name = values.getOrNull(0).orEmpty()
            val goal = values.getOrNull(1).orEmpty()
            if (name.isBlank()) {
                toast("项目名称不能为空")
                return@showForm
            }
            val p = repo.createProject(name, goal)
            toast("已创建「${p.name}」，先去加几个工作间")
            open(p)
        }
    }
}
