package com.aiforge.collab.ai

import com.aiforge.collab.model.FileEdit

/**
 * 解析 AI 输出的文件块：
 *
 * <<<FILE: src/main/kotlin/Foo.kt
 * ...文件内容...
 * >>>END
 *
 * <<<SUMMARY: 一句话说明
 */
object PatchParser {

    data class Parsed(val edits: List<FileEdit>, val summary: String)

    private val fileRe = Regex("<<<FILE:\\s*(.+?)\\r?\\n([\\s\\S]*?)>>>END")
    private val summaryRe = Regex("<<<SUMMARY:\\s*(.+?)(?:\\r?\\n|$)")

    fun parse(text: String): Parsed {
        val edits = ArrayList<FileEdit>()
        for (m in fileRe.findAll(text)) {
            val rawPath = m.groupValues[1].trim().trim('`', '"', '*', ' ')
            val path = sanitize(rawPath) ?: continue
            val body = m.groupValues[2].trimEnd('\n', '\r')
            edits.add(FileEdit(path, body))
        }
        val summary = summaryRe.find(text)?.groupValues?.get(1)?.trim()?.take(200).orEmpty()
        return Parsed(edits, summary)
    }

    /** 只允许工程内的相对路径。 */
    fun sanitize(path: String): String? {
        var p = path.replace('\\', '/').trim()
        p = p.removePrefix("./")
        p = p.trimStart('/')
        if (p.isEmpty()) return null
        if (p.contains("..")) return null
        if (p.length > 200) return null
        return p
    }
}
