# ExecPlan（阿里百炼云端改造）

## ExecPlan（2026-03-13：现状链路盘点与 Rust/Tauri 重构报告）

### 目标
- 基于当前 fork 的真实运行代码，梳理“你实际在用”的两条链路：
  - 快捷键实时语音输入：按下录音 -> 云端实时识别 -> 文本上屏
  - 文件转写：拖文件启动客户端 -> 上传 -> 云端 REST 转写 -> 落盘
- 明确哪些模块是这两条链路的硬依赖，哪些只是仓库内的扩展能力或历史遗留能力。
- 输出一份可直接喂给编码 AI 的技术报告，为后续 `Rust + Tauri` 单独新项目重构提供高密度上下文。

### 产物
1. `docs/rust_tauri_rebuild_report.md`
- 记录当前系统的真实入口、调用链、协议边界、配置项、依赖模块与副作用。
- 区分“必须迁移 / 建议延后 / 当前可不迁移”的功能范围。
- 给出基于 Tauri 2 官方架构思路的模块拆分建议。

2. `PROGRESS.md`
- 更新当前结论、已完成项、下一步动作，避免后续讨论时上下文漂移。

### 执行步骤
1. 盘点客户端入口、服务端入口、配置与文档。
2. 顺着两条真实业务链路做代码级追踪。
3. 标记当前默认会执行但对核心需求非必需的附属能力。
4. 输出 Rust/Tauri 重构建议与阶段性迁移路线。
5. 更新 `PROGRESS.md` 并提交。

### 验证
- 报告必须覆盖两条真实链路的逐步流程与关键文件路径。
- 报告必须明确“文件转写已独立，不依赖本地实时服务端 WebSocket”这一现状。
- 报告必须明确“热词只作用于麦克风最终结果后处理，不作用于文件转写结果”这一边界。
- 报告必须明确说明当前项目缺少系统化自动化测试，现阶段以手工联调为主。

## ExecPlan（2026-02-18：百炼 FunASR Skill 沉淀）

### 目标
- 基于当前项目实装代码与阿里云官方文档，沉淀一个可复用 Skill。
- 覆盖三条核心链路：实时 WebSocket、录音文件 REST 异步、`oss://` 临时 URL 上传。
- 形成“可直接复用到新项目”的最小交互模板与排错清单。

### 产物
1. `skills/aliyun-bailian-funasr/SKILL.md`
- 定义 Skill 触发语义与执行工作流。
- 规定信息收集、接口调用、状态机拼接、收尾与验收步骤。

2. `skills/aliyun-bailian-funasr/references/api-spec.md`
- 汇总官方接口的请求/响应格式与字段语义。
- 给出实时 WS 事件流、REST 任务状态流、上传中转流程的结构化示例。
- 标注与本项目现有实现对应的代码位置，方便迁移。

### 执行步骤
1. 检索并整理项目内阿里云调用代码与配置。
2. 对照官方文档补齐协议细节与边界约束。
3. 生成 Skill 目录并编写 `SKILL.md` 与 `references/api-spec.md`。
4. 运行 `quick_validate.py` 做格式校验。
5. 更新 `PROGRESS.md`，记录结论、决策与下一步。

### 验证
- Skill frontmatter 合法（`name`、`description` 完整）。
- 参考文档包含三条链路完整字段：WS、REST、上传。
- 明确“去重拼接策略”与“等待 `task-finished` 收尾”的最终文本组装规则。

### 执行进度（2026-02-18）
- [x] 已完成项目内百炼调用代码与配置检索。
- [x] 已完成官方文档字段核对（WS/REST/上传）。
- [x] 已完成 Skill 文件落地与引用规范编写。
- [x] 已完成 Skill 快速校验（`quick_validate.py` 通过）。

## ExecPlan（2026-02-18：实时链路状态机重构）

### 目标
- 去掉实时链路的本地“60 秒工程分段 + 本地文本拼接”依赖。
- 改为“单次按键会话 = 单个云端 WebSocket 任务”。
- 仅在 `sentence_end=true` 或 `end_time!=null` 时将句子定稿，避免中间快照重复/覆盖。

### 改造范围
1. `util/server/asr_aliyun_realtime.py`
- 重写为“会话管理器”模式：
  - 首包打开云端 WS + `run-task`
  - 流式送音频 chunk
  - 后台消费 `result-generated` 事件
  - 使用句子状态机（按 `begin_time` 归档）维护最终句列表
  - `finish-task` 后等待 `task-finished`，返回最终文本

2. `util/server/server_ws_recv.py`
- `model_type=aliyun_realtime` + `source=mic` 时：
  - 不再按 `seg_duration/seg_overlap` 切 60 秒工程片段
  - 改为传输层 100ms chunk 入队
  - 结束时发送一个“final 控制任务”触发 `finish-task`

3. `util/server/server_init_recognizer.py`
- aliyun 模式不再走 `recognize()` 的本地拼接流程
- 改为调用 `AliyunRealtimeRecognizer.process_task(task)`
- 仅在会话结束时产出 `Result(is_final=True)` 发回客户端

### 验证
- 语法检查：`python -m py_compile` 覆盖改动文件。
- 关键日志验证：
  - 会话开始/结束日志
  - `task-finished` 到达
  - 最终文本长度与句子数

### 风险与回滚
- 风险：若云端回包缺少 `begin_time`，句子归档可能退化。
- 缓解：增加“未知句 fallback key”与最终兜底拼接。
- 回滚：可回退到 `b1ea34e` 并恢复原 `recognize()` 路径。

## 背景
- 当前目录是可运行快照（含用户本地热词与启动脚本），但不是 Git 仓库。
- 目标是在用户 GitHub Fork 中继续开发，并将 ASR 从本地模型迁移到阿里云百炼。
- 关键约束：实时听写能力不能被文件转写任务阻塞。

## 目标
1. 把当前本地快照完整同步到用户 Fork 仓库并提交。
2. 接入阿里百炼实时 WebSocket（用于麦克风实时听写）。
3. 接入阿里百炼录音文件 REST 异步转写（用于文件转录）。
4. 让文件转写与实时听写并行执行，互不阻塞。
5. 补充可复用文档，并沉淀后续 Skill 化方案。

## 实施步骤
1. Fork 与仓库接管
- 创建 `ZJHSteven/CapsWriter-Offline` fork。
- 将当前本地快照同步到 fork 本地工作副本。
- 建立 `origin`（fork）与 `upstream`（原仓库）远端。

2. 文档与资料基线
- 新增 `PROGRESS.md` 与本文件，持续记录状态。
- 记录 Context7 可用数据源与官方文档链接。

3. 代码改造（阶段一：实时）
- 新增阿里百炼实时 ASR 适配层（WebSocket 客户端）。
- 替换服务端本地识别调用，但保持现有客户端协议字段不变：
  - `text`
  - `text_accu`
  - `tokens`
  - `timestamps`
  - `is_final`

4. 代码改造（阶段二：文件）
- 为文件转写增加 REST 异步任务通道（提交任务+轮询结果）。
- 保留现有结果落盘（txt/json/srt/merge）与后处理体验。
- 明确与实时链路解耦：文件转写不再经过本地实时识别服务端队列。
- 增加本地文件到可访问 URL 的上传适配层（支持临时签名上传 / 自定义上传接口）。

5. 并发与稳定性
- 将文件转写任务放入独立异步执行器，不占用实时链路关键资源。
- 增加失败重试、超时、取消、错误日志。

6. 交付与验证
- 手工验证：麦克风实时输入、文件转写、两者并发。
- 输出迁移说明与配置步骤。

## 风险与缓解
- 风险：录音文件 REST 仅支持 URL 输入，不支持本地文件直传。
  - 缓解：文件转写阶段增加“上传到可访问对象存储”的中转接口。
- 风险：实时 API 返回字段与本地模型字段不完全一致。
  - 缓解：在服务端适配层做统一映射，保持客户端无感。
- 风险：API 限流与网络抖动。
  - 缓解：连接复用、指数退避重试、降级提示。

## ExecPlan（2026-02-22：实时云端 ASR 稳健性与保底恢复）

### 目标
- 避免“只差 `task-finished` 就全丢”的惨痛场景。
- 中途网络断开 / `task-failed` / finish 超时都进入统一失败保底链路。
- 提供失败任务落盘与独立手动重试入口，保留整次按下/抬起录音会话文件。

### 分阶段状态
- Phase 1（止血）：已完成（首版）
  - [x] 会话级 PCM 临时落盘（整次会话）
  - [x] 失败保底快照（已定稿 + 未定稿）
  - [x] 服务端终端打印保底文本
  - [x] 失败任务落盘（文本/音频/事件摘要）
  - [x] 任务级异常隔离（识别子进程不因单任务崩溃）
  - [x] 自动重试一次（基于 PCM 重放）
- Phase 2（有限并发）：未完成
  - [ ] recognizer 主循环非阻塞分发
  - [ ] 会话并发上限与 pending 队列
- Phase 3（手动重试工具）：已完成（首版）
  - [x] `retry_failed_tasks.py` 终端交互列出/选择/重试
  - [x] 重试成功写文字备份并标记成功
  - [x] `retry_failed_tasks.bat` Windows 启动入口
  - [x] 手动重试入口瘦身：`util.client` 改为懒加载，`retry_failed_tasks.bat` 改为 `uv run` 启动，裸 `python` 会给出明确缺依赖提示
- Phase 4（备份策略完善）：部分完成
  - [x] 文字备份与音频备份解耦
  - [x] 文字备份按保留天数清理（默认 30 天）
  - [ ] 失败音频安全上限天数清理策略（可选）

### 当前约束（明确说明）
- 当前仍是识别子进程串行调度；即使单任务失败不再崩，`final` 任务等待期间仍可能阻塞后续任务（并发化待 Phase 2）。
- 自动重试为“整段 PCM 重放到新云端会话”，不是续传原会话。

## ExecPlan（2026-03-04：官方临时 OSS 上传链路补齐与联调）

### 目标
- 补齐阿里百炼“官方临时 OSS 上传”能力：`getPolicy -> OSS 表单上传 -> oss://key`。
- 当文件 URL 为 `oss://` 时，调用 REST 识别自动带上 `X-DashScope-OssResourceResolve: enable`。
- 使用真实本地测试文件走完整链路，验证官方地址可访问、提交流程可跑通。

### 改造范围
1. `config.py`
- 新增官方临时 OSS 上传模式配置项（模式名、凭证地址、模型参数、超时）。

2. `util/client/transcribe/file_upload_resolver.py`
- 新增 `dashscope_temp_oss` 上传模式：
  - 调用 `GET /api/v1/uploads?action=getPolicy&model=...`
  - 解析凭证字段并执行 multipart/form-data 上传
  - 返回 `oss://{key}` 临时地址
- 补齐关键日志与错误信息，便于排障。

3. `util/client/transcribe/file_transcriber.py`
- 创建上传解析器时注入 DashScope API Key 与官方上传配置。

4. `util/client/transcribe/dashscope_rest_client.py`
- 提交任务前自动判断 `file_url` 是否为 `oss://`。
- 若是 `oss://`，自动附加 `X-DashScope-OssResourceResolve: enable` 请求头。

### 验证步骤
1. 语法检查：`python -m py_compile` 覆盖修改文件。
2. 功能联调（真实文件）：
- 准备一个本地测试音频文件。
- 走 `dashscope_temp_oss` 模式上传并拿到 `oss://`。
- 提交 REST 异步任务并轮询状态。
3. 结果判定：
- 若返回 `task_id` 且状态进入 `PENDING/RUNNING/SUCCEEDED`，视为路径跑通。
- 记录失败信息（鉴权/网络/配额）并给出可复现日志。

### 执行进度（2026-03-04）
- [x] 已完成配置项与上传模式代码改造。
- [x] 已完成 `oss://` 提交头自动适配。
- [x] 已完成真实文件端到端测试。
