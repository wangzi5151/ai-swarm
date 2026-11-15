package com.aiforge.collab.ui

import android.content.Context
import android.graphics.drawable.GradientDrawable
import android.text.InputType
import android.util.TypedValue
import android.view.View
import android.view.ViewGroup
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.core.content.ContextCompat
import com.aiforge.collab.R

// ---------- 小工具 ----------

fun Context.color(id: Int): Int = ContextCompat.getColor(this, id)

fun Context.dp(v: Float): Int = (v * resources.displayMetrics.density + 0.5f).toInt()

fun Context.toast(msg: String) = Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()

fun Context.rounded(fill: Int, stroke: Int? = null, radiusDp: Float = 14f): GradientDrawable =
    GradientDrawable().apply {
        shape = GradientDrawable.RECTANGLE
        cornerRadius = dp(radiusDp).toFloat()
        setColor(fill)
        if (stroke != null) setStroke(dp(1f).coerceAtLeast(1), stroke)
    }

fun Context.textView(
    text: String,
    sizeSp: Float = 14f,
    colorId: Int = R.color.text_primary,
    mono: Boolean = false,
    bold: Boolean = false
): TextView = TextView(this).apply {
    this.text = text
    setTextSize(TypedValue.COMPLEX_UNIT_SP, sizeSp)
    setTextColor(color(colorId))
    if (mono) typeface = android.graphics.Typeface.MONOSPACE
    if (bold) setTypeface(typeface, android.graphics.Typeface.BOLD)
}

/** 一块深色面板，用于把一组内容包起来。 */
fun Context.panel(): LinearLayout = LinearLayout(this).apply {
    orientation = LinearLayout.VERTICAL
    background = rounded(color(R.color.surface), color(R.color.stroke), 14f)
    setPadding(dp(14f), dp(12f), dp(14f), dp(12f))
    layoutParams = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT,
        ViewGroup.LayoutParams.WRAP_CONTENT
    ).apply { bottomMargin = dp(10f) }
}

fun Context.sectionTitle(text: String): TextView = textView(text, 12f, R.color.text_secondary, bold = true).apply {
    letterSpacing = 0.12f
    layoutParams = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT,
        ViewGroup.LayoutParams.WRAP_CONTENT
    ).apply {
        topMargin = dp(18f)
        bottomMargin = dp(8f)
    }
}

fun Context.hint(text: String): TextView = textView(text, 13f, R.color.text_secondary).apply {
    layoutParams = LinearLayout.LayoutParams(
        ViewGroup.LayoutParams.MATCH_PARENT,
        ViewGroup.LayoutParams.WRAP_CONTENT
    ).apply { bottomMargin = dp(10f) }
}

private fun statusColorId(status: String): Int = when (status) {
    "TODO" -> R.color.text_secondary
    "RUNNING" -> R.color.accent
    "REVIEW", "PENDING" -> R.color.warning
    "MERGED" -> R.color.success
    "CONFLICT", "FAILED" -> R.color.danger
    "REJECTED" -> R.color.text_secondary
    else -> R.color.text_secondary
}

fun Context.statusChip(status: String, label: String? = null): TextView {
    val c = color(statusColorId(status))
    return textView(label ?: statusCn(status), 11f, statusColorId(status), bold = true).apply {
        background = rounded(ColorTransparent(c, 0.14f), c, 8f)
        setPadding(dp(8f), dp(3f), dp(8f), dp(3f))
    }
}

/** 任意颜色的胶囊标签（用来标"本地模拟 / 真实接口"这类信息）。 */
fun Context.tagChip(text: String, colorId: Int = R.color.accent, alpha: Float = 0.14f): TextView {
    val c = color(colorId)
    return textView(text, 11f, colorId, bold = true).apply {
        background = rounded(ColorTransparent(c, alpha), c, 8f)
        setPadding(dp(8f), dp(3f), dp(8f), dp(3f))
    }
}

private fun ColorTransparent(base: Int, alpha: Float): Int {
    val a = (alpha * 255).toInt().coerceIn(0, 255)
    return (a shl 24) or (base and 0x00FFFFFF)
}

fun statusCn(status: String): String = when (status) {
    "TODO" -> "待分配"
    "RUNNING" -> "工作中"
    "REVIEW" -> "待合并"
    "MERGED" -> "已合并"
    "FAILED" -> "失败"
    "PENDING" -> "待合并"
    "CONFLICT" -> "冲突"
    "REJECTED" -> "已拒绝"
    else -> status
}

/** 长得像按钮的小标签。 */
fun Context.actionChip(text: String, colorId: Int = R.color.accent, onClick: () -> Unit): TextView {
    val c = color(colorId)
    return textView(text, 12f, colorId, bold = true).apply {
        background = rounded(color(R.color.surface_high), c, 10f)
        setPadding(dp(12f), dp(7f), dp(12f), dp(7f))
        isClickable = true
        setOnClickListener { onClick() }
    }
}

fun horizontal(spacingDp: Float = 8f, vararg views: View): LinearLayout {
    val ctx = views.first().context
    return LinearLayout(ctx).apply {
        orientation = LinearLayout.HORIZONTAL
        views.forEachIndexed { i, v ->
            if (v.parent != null) (v.parent as? ViewGroup)?.removeView(v)
            if (i > 0) v.layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.WRAP_CONTENT,
                ViewGroup.LayoutParams.WRAP_CONTENT
            ).apply { leftMargin = ctx.dp(spacingDp) }
            addView(v)
        }
    }
}

/** 主按钮。 */
fun Context.primaryButton(text: String, onClick: () -> Unit): com.google.android.material.button.MaterialButton =
    com.google.android.material.button.MaterialButton(this).apply {
        this.text = text
        isAllCaps = false
        textSize = 14f
        setTextColor(android.graphics.Color.parseColor("#06131F"))
        backgroundTintList = android.content.res.ColorStateList.valueOf(color(R.color.accent))
        setOnClickListener { onClick() }
    }

/** 次要按钮（描边）。 */
fun Context.ghostButton(text: String, onClick: () -> Unit): com.google.android.material.button.MaterialButton =
    com.google.android.material.button.MaterialButton(
        this, null, com.google.android.material.R.attr.materialButtonOutlinedStyle
    ).apply {
        this.text = text
        isAllCaps = false
        textSize = 13f
        setTextColor(color(R.color.text_primary))
        strokeColor = android.content.res.ColorStateList.valueOf(color(R.color.stroke))
        setOnClickListener { onClick() }
    }

// ---------- 表单对话框 ----------

class FormField(
    val label: String,
    val value: String = "",
    val hint: String = "",
    val secret: Boolean = false
)

fun Context.showForm(title: String, fields: List<FormField>, okText: String = "确定", onOk: (List<String>) -> Unit) {
    val box = LinearLayout(this).apply {
        orientation = LinearLayout.VERTICAL
        setPadding(dp(20f), dp(8f), dp(20f), dp(4f))
    }
    val inputs = ArrayList<EditText>()
    for (f in fields) {
        box.addView(textView(f.label, 12f, R.color.text_secondary).apply {
            layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
            ).apply { topMargin = dp(10f) }
        })
        val et = EditText(this).apply {
            setText(f.value)
            hint = f.hint
            setTextColor(color(R.color.text_primary))
            setHintTextColor(color(R.color.text_secondary))
            setTextSize(TypedValue.COMPLEX_UNIT_SP, 14f)
            inputType = if (f.secret) {
                InputType.TYPE_CLASS_TEXT or InputType.TYPE_TEXT_VARIATION_PASSWORD
            } else {
                InputType.TYPE_CLASS_TEXT
            }
            layoutParams = LinearLayout.LayoutParams(
                ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT
            )
        }
        inputs.add(et)
        box.addView(et)
    }

    val scroll = ScrollView(this).apply {
        addView(box)
    }

    AlertDialog.Builder(this)
        .setTitle(title)
        .setView(scroll)
        .setPositiveButton(okText) { _, _ ->
            onOk(inputs.map { it.text.toString().trim() })
        }
        .setNegativeButton("取消", null)
        .show()
}

fun Context.simpleDialog(title: String, message: String, okText: String = "知道了", onOk: (() -> Unit)? = null) {
    AlertDialog.Builder(this)
        .setTitle(title)
        .setMessage(message)
        .setPositiveButton(okText) { _, _ -> onOk?.invoke() }
        .show()
}

fun Context.chooseDialog(title: String, items: List<String>, onPick: (Int) -> Unit) {
    AlertDialog.Builder(this)
        .setTitle(title)
        .setItems(items.toTypedArray()) { _, which -> onPick(which) }
        .setNegativeButton("取消", null)
        .show()
}

fun Context.confirmDialog(title: String, message: String, okText: String = "确定", onOk: () -> Unit) {
    AlertDialog.Builder(this)
        .setTitle(title)
        .setMessage(message)
        .setPositiveButton(okText) { _, _ -> onOk() }
        .setNegativeButton("取消", null)
        .show()
}
