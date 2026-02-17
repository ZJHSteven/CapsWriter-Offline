# 项目状态快照

## 当前结论（必须最新）
- 现状：已在 GitHub fork 分支 `feat/bailian-cloud-migration` 完成基线同步，开始云端识别改造。
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
- 正在做：将文件转写从本地 WebSocket 识别链路迁移到百炼 REST 异步链路（独立通道）。
- 下一步：
  - 跑通语法检查与最小链路自测。
  - 联调 presigned_put / custom_api 两种上传模式。
  - 补充 readme 示例配置与使用步骤。

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

## 常见坑 / 复现方法
- 坑1：REST 文件识别不支持本地文件直传与 base64。
  - 复现：直接把本地路径或 base64 放到 `input`，接口会失败。
- 坑2：文件转写与实时听写共用同一阻塞执行路径会互相影响。
  - 复现：启动长视频转写后，按热键录音会出现响应变慢或等待。
- 坑3：云端模式下若在模块顶层导入本地 GGUF 引擎，可能触发循环导入并导致服务端启动失败。
  - 复现：`model_type=aliyun_realtime`，但 `server_init_recognizer.py` 顶层仍导入 `util.fun_asr_gguf`。
