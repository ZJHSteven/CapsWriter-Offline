# 项目状态快照

## 当前结论（必须最新）
- 现状：已在 GitHub fork 分支 `feat/bailian-cloud-migration` 完成云端迁移基线，并进入“实时链路状态机重构”阶段。
- 已完成：
  - 已核对官方文档与 Context7 来源，可支撑本次改造。
  - 已完成本地快照基线提交并推送到 fork 分支。
  - 服务端新增阿里云百炼实时适配器 `util/server/asr_aliyun_realtime.py`。
  - 服务端任务队列已拆分为麦克风/文件双队列，并实现麦克风优先调度。
  - `config.py` 已加入百炼配置项，并将默认模型切换为 `aliyun_realtime`。
  - `readme.md` 已补充云端模式 `DASHSCOPE_API_KEY` 配置说明。
  - 新增资料策略文档 `docs/bailian_context7_strategy.md`。
  - 修复服务端启动导入错误：`aliyun_realtime` 模式下改为按需导入 `create_asr_engine`，避免触发 `util.fun_asr_gguf` 循环导入。
  - 新增独立文件转写 REST 客户端：`util/client/transcribe/dashscope_rest_client.py`。
  - 新增本地文件上传 URL 解析器：`util/client/transcribe/file_upload_resolver.py`（支持 `presigned_put` 与 `custom_api`）。
  - `util/client/transcribe/file_transcriber.py` 已重构为独立 REST 通道，不再依赖本地实时服务端 WebSocket。
  - `config.py` 已新增文件 REST 与上传通道配置项。
  - 新增实时链路重构执行计划（`PLANS.md`）：明确“单次会话直连云端 + 句子状态机定稿”的改造方向。
  - `util/server/asr_aliyun_realtime.py` 已重写为会话管理器：
    - 一个本地任务对应一个云端 WS 会话
    - 按 `sentence_end/end_time` 做句子定稿
    - `finish-task` 后等待 `task-finished` 再返回最终文本
  - `util/server/server_ws_recv.py` 已在 aliyun 实时模式下切换为“传输层 100ms 分帧入队”，不再走本地 60 秒工程切段。
  - `util/server/server_init_recognizer.py` 已为 aliyun 模式接入新会话接口，仅在 final 时回传结果，避免中间快照干扰输出。
  - 已新增百炼 Fun-ASR 可复用 Skill：`skills/aliyun-bailian-funasr/SKILL.md`。
  - 已新增 Skill 协议规范：`skills/aliyun-bailian-funasr/references/api-spec.md`（覆盖实时 WS、文件 REST、临时 `oss://` URL 上传）。
  - 已完成 Skill 格式校验：`uv run --with pyyaml quick_validate.py` 通过（启用 `PYTHONUTF8=1`）。
  - `PLANS.md` 已追加本次 Skill 沉淀任务执行进度，便于后续继续迭代。
  - 已将 Skill 同步到全局目录：`C:\Users\ZJHSteven\.codex\skills\aliyun-bailian-funasr`，可在后续项目直接复用。
- 正在做：联调“长按说话 -> 单会话云端识别 -> 松开后一次性上屏”的端到端链路。
- 下一步：
  - 用真实语音流验证句子状态机在连续说话场景下无“覆盖前文/重复叠加”问题。
  - 评估是否需要把客户端发送节奏也统一为固定 100ms（当前已在服务端做 100ms 传输分帧）。
  - 补充 readme 的“实时链路状态机”说明与调参建议。
  - 用新 Skill 在独立示例项目复用一次，验证可迁移性与文档完备性。

## 关键决策与理由（防止“吃书”）
- 决策A：实时听写主链路采用 WebSocket，而不是仅 REST。
  - 原因：当前项目是输入法实时交互；REST 文件转写接口要求 URL 输入且是异步任务，不适合作为麦克风主链路。
- 决策B：文件转写走 REST 异步，作为独立通道。
  - 原因：可利用官方文件识别能力与可选时间戳能力，同时避免拖慢实时链路。
- 决策C：优先保持客户端协议不变，只在服务端做适配。
  - 原因：最大限度复用已有热词、后处理、上屏、LLM 等成熟能力，减少回归风险。
- 决策D：先做服务端队列分流（mic/file）与麦克风优先。
  - 原因：在不大改客户端流程的前提下，先降低文件任务对实时听写的阻塞影响。
- 决策E：最终文件转写必须独立于实时识别服务端，直接走 REST 异步。
  - 原因：彻底解耦后端资源占用，避免文件任务影响日常麦克风实时听写。
- 决策F：上传层采用“临时签名上传 + 自定义上传 API”双模式。
  - 原因：避免硬编码单一 OSS SDK，兼容不同对象存储与企业内网网关方案。
- 决策G：实时链路改为“单会话直连云端 + 句子状态机定稿”，不再依赖本地 60 秒工程分段与文本拼接。
  - 原因：`result-generated` 是句子快照更新，不是稳定增量；本地分段拼接会放大覆盖/重复风险。
- 决策H：将 Fun-ASR 协议知识沉淀为独立 Skill（含参考规范文件）。
  - 原因：后续跨项目复用时可直接套用标准状态机与接口模板，降低重复沟通与实现偏差。

## 常见坑 / 复现方法
- 坑1：REST 文件识别不支持本地文件直传与 base64。
  - 复现：直接把本地路径或 base64 放到 `input`，接口会失败。
- 坑2：文件转写与实时听写共用同一阻塞执行路径会互相影响。
  - 复现：启动长视频转写后，按热键录音会出现响应变慢或等待。
- 坑3：云端模式下若在模块顶层导入本地 GGUF 引擎，可能触发循环导入并导致服务端启动失败。
  - 复现：`model_type=aliyun_realtime`，但 `server_init_recognizer.py` 顶层仍导入 `util.fun_asr_gguf`。
- 坑4：把 `result-generated` 当“追加文本”而不是“同句覆盖更新”，会出现 A + A' + B 的重复拼接问题。
  - 复现：连续说话时对每条 `result-generated` 直接 `total += text`，最终文本会重复堆叠。
- 坑5：在 Windows + uv 管理 Python 环境下直接运行 Skill 校验脚本，可能出现 `yaml` 缺失或默认 GBK 解码失败。
  - 复现：直接执行 `python quick_validate.py`，未用 `uv run --with pyyaml` 且未设置 `PYTHONUTF8=1`。
