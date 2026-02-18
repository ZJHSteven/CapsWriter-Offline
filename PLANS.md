# ExecPlan（阿里百炼云端改造）

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
