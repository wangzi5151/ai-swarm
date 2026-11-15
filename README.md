# AI 协作工坊 (AICollabForge)

让**多个不同 AI** 在**各自独立的工作间（沙盒）**里共同开发一个大项目，产出的改动以"补丁"形式交给你，只有你点「合并」才会进入主工程。

## 一、为什么这样设计

单个 AI 接大项目，容易上下文爆掉、越改越乱、互相覆盖。
这里把项目切成任务，每个 AI 只在自己的目录里干活，谁都不许直接动主工程：

```
你：我要做一个记账 App
      ↓
「自动拆解」→ 建 4 个工作间 + 4 个任务
      ↓
AI-01 架构师   AI-02 实现   AI-03 界面   AI-04 测试
   各自在 自己的沙盒目录 里改文件
      ↓
每人交一个「补丁包」（改了哪些文件）
      ↓
你点「合并」→ 逐文件写入主工程
      ↓
同一文件被两个工作间都改过 → 报冲突，由你决定是否强制覆盖
```

## 二、手机上的四个区域

1. **项目页**：项目列表，长按可删除。
2. **工作间**：每个 AI 一个条目 = 名字 + 专长 + 用的是「本地模拟」还是「真实接口」。
3. **任务板**：点任务 -> 交给它对应的那个工作间去做；状态：待分配 / 工作中 / 待合并 / 已合并 / 失败。
4. **补丁队列**：AI 交付的改动在这里，可「查看文件」→「合并进主工程」或「拒绝」。
5. **主工程文件**：只有合并才会写进来的目录树。

## 三、两种 AI 后端

| 类型 | 说明 | 需要什么 |
| --- | --- | --- |
| 本地模拟 | 按任务标题造出 `docs/xxx.md`、`src/main/kotlin/app/Xxx.kt`、`tests/xxxSpec.md` | 什么都不用，装完就能跑通全流程 |
| 真实接口 | 任何 OpenAI 兼容的 `/chat/completions` | 接口地址、模型名、API Key |

真实接口的约定输出格式（系统会把任务和目标一起发过去）：

```
<<<FILE: src/main/kotlin/app/Foo.kt
...文件完整内容...
>>>END

<<<SUMMARY: 一句话说明改了什么
```

解析器只接受工程内的相对路径，`..` 和绝对路径会被丢弃。

## 四、数据存在哪

```
filesDir/forge/
  projects.json              项目/工作间/任务/补丁（JSON）
  p_<项目id>/
    main/                    主工程（只有合并能写）
    ws/<工作间id>/           该 AI 的独立工作间
```

## 五、构建与安装

在项目根目录执行（工具链已预装，不需要装任何东西）：

```bash
gradle --no-daemon --console=plain assembleDebug
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

- 首次构建会下载少量间接依赖，需要几分钟；之后再构建会快很多。
- 如果网络不可用，可以先试 `gradle --offline ...`，但只有本地 Maven 缓存里齐全时才能成功。

## 六、当前边界（还没做的）

- 没有逐行 diff 视图，只有"文件内容预览"。
- 工作间之间不能互相读取对方目录（这是有意的隔离），只能由你合并后共享。
- 没有"回滚到某个合并点"的版本快照。
- 真实接口暂不含流式输出与重试策略。

## 七、代码结构

```
app/src/main/java/com/aiforge/collab/
  App.kt                       Application，持有仓库
  model/Models.kt              Project / Agent / TaskItem / ChangeSet
  store/WorkspaceRepo.kt       持久化 + 目录布局 + 合并与冲突判定
  ai/AiClient.kt               OpenAI 兼容调用 + 本地模拟大脑
  ai/PatchParser.kt            <<<FILE / <<<SUMMARY 解析与路径消毒
  ui/MainActivity.kt           项目列表
  ui/ProjectDetailActivity.kt  作战室：工作间 / 任务 / 补丁 / 主工程
  ui/Ui.kt                     纯代码 UI 组件（面板、胶囊、表单弹窗）
```
