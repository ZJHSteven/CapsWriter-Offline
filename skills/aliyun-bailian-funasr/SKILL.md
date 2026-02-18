---
name: aliyun-bailian-funasr
description: 面向阿里云百炼 Fun-ASR 的协议化接入技能。用于实现或排查 Fun-ASR 实时 WebSocket 识别、录音文件 REST 异步识别、以及 oss:// 临时 URL 上传与调用链路；当用户提到 run-task/result-generated/task-finished、file_urls/task_id/task_status、上传凭证 getPolicy、X-DashScope-OssResourceResolve 等关键词时使用本技能。
---

# 阿里云百炼 Fun-ASR 协议技能

按以下步骤执行，避免“分包发送逻辑”和“服务端断句逻辑”混用导致的文本重复、丢字或乱序。

## 1. 先确认任务类型

先判断用户需求属于哪一类：

1. 实时输入（边说边出字，低延迟）：走 WebSocket 实时链路。
2. 文件转写（可等待结果，支持批量 URL）：走 REST 异步链路。
3. 本地文件暂存为 `oss://` 临时 URL：走上传凭证 + OSS 表单上传链路。

当用户同时涉及两类以上需求时，先分别保证每条链路单独可跑通，再做编排。

## 2. 读取参考文档

优先读取 `references/api-spec.md`，并按场景只加载必要章节：

1. 做实时识别：读取“1) 实时 WebSocket API”。
2. 做文件转写：读取“2) 录音文件 REST API”。
3. 做临时 URL 上传：读取“3) 临时 OSS URL 上传 API”。
4. 对接本仓库代码：读取“4) 与本项目代码映射”。

## 3. 实时 WebSocket 的强制实现规则

实现实时 WS 时，必须遵循以下规则：

1. 先发 `run-task`，收到 `task-started` 后再发音频二进制帧。
2. 音频分包（例如 20ms/40ms/100ms）仅影响传输节奏，不直接决定断句。
3. `result-generated` 里的同一 `begin_time` 视为“同一句更新”，必须覆盖更新，不可直接追加。
4. 仅在 `sentence_end=true` 或 `end_time!=null` 时将句子定稿。
5. 发送 `finish-task` 后，必须继续收包直到 `task-finished` 再收尾。
6. 若收到 `task-failed`，立即视为任务失败并中止当前连接复用。

## 4. 文件 REST 的强制实现规则

实现文件 REST 时，必须遵循以下规则：

1. 提交任务必须携带 `X-DashScope-Async: enable`。
2. 输入必须是公网可访问 URL（`http/https`）或按文档生成的临时 `oss://` URL。
3. 轮询 `task_id` 直到终态（至少处理 `PENDING`、`RUNNING`、`SUCCEEDED`、`FAILED`）。
4. `task_status=SUCCEEDED` 不代表所有子任务成功，必须检查每个 `subtask_status`。
5. 结果链接 `transcription_url` 有有效期，需尽快下载并落盘。

## 5. 临时 `oss://` URL 的强制实现规则

处理本地文件上传时，必须遵循以下规则：

1. 先调用 `GET /api/v1/uploads?action=getPolicy&model=...` 获取上传凭证。
2. 使用返回的 `upload_host` + multipart/form-data 字段上传文件。
3. 用 `oss://` + `key` 生成临时 URL（48 小时有效）。
4. 使用该 `oss://` URL 调用模型时，HTTP Header 必须加 `X-DashScope-OssResourceResolve: enable`。
5. 临时存储链路限流 100 QPS，不用于生产高并发；生产场景改用自有 OSS 长期 URL。

## 6. 输出要求（给调用者）

完成任务后，输出内容至少包含：

1. 本次使用的是哪条链路（WS/REST/上传）。
2. 请求格式、关键参数、关键事件/状态、收尾条件。
3. 最小可运行示例（可直接复制运行）。
4. 失败排查入口（鉴权、URL 可达性、Header 缺失、任务状态异常）。

## 7. 代码生成偏好

生成代码时遵循：

1. 先给“最小可运行版本”，再补充可选高级参数。
2. 优先将异常处理、状态机拼接、重试逻辑做成独立函数，避免与主流程混写。
3. 统一日志字段：`task_id`、`event/status`、`elapsed_ms`、`request_id`。
4. 所有时间戳统一内部单位（建议秒），外部接口毫秒字段在边界处转换。
