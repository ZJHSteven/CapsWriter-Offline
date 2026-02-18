# 阿里云百炼 Fun-ASR API 交互规范（实时 WS + 文件 REST + 临时 OSS URL）

本文档用于在工程中快速复用百炼 Fun-ASR 的三条核心链路，并避免常见实现误区。

## 0) 信息来源与校验时间

- 校验时间：2026-02-18
- 官方文档（实时 WS）：`https://help.aliyun.com/zh/model-studio/fun-asr-realtime-websocket-api`
- 官方文档（录音文件 REST）：`https://help.aliyun.com/zh/model-studio/fun-asr-recorded-speech-recognition-restful-api`
- 官方文档（上传临时 URL）：`https://help.aliyun.com/zh/model-studio/upload-files-through-api`
- 项目内实现映射：
  - `util/server/asr_aliyun_realtime.py`
  - `util/client/transcribe/dashscope_rest_client.py`
  - `util/client/transcribe/file_upload_resolver.py`
  - `util/client/transcribe/file_transcriber.py`

## 1) 实时 WebSocket API

### 1.1 连接与鉴权

- Endpoint：`wss://dashscope.aliyuncs.com/api-ws/v1/inference`
- Header：`Authorization: Bearer $DASHSCOPE_API_KEY`
- 建议：每次按键会话对应一个 `task_id`，避免多会话串写。

### 1.2 客户端 -> 服务端：`run-task`

请求（JSON 文本帧）示例：

```json
{
  "header": {
    "action": "run-task",
    "task_id": "uuid-xxxx",
    "streaming": "duplex"
  },
  "payload": {
    "task_group": "audio",
    "task": "asr",
    "function": "recognition",
    "model": "fun-asr-realtime-v2",
    "parameters": {
      "format": "pcm",
      "sample_rate": 16000,
      "semantic_punctuation_enabled": false,
      "max_sentence_silence": 800,
      "disfluency_removal_enabled": false
    },
    "input": {}
  }
}
```

常用参数（以官方文档与 SDK 示例交集为准）：

- `format`：音频格式，常用 `pcm`。
- `sample_rate`：采样率，常用 `16000`。
- `semantic_punctuation_enabled`：是否语义断句。`true` 时延迟更高但断句更语义化。
- `max_sentence_silence`：VAD 静音断句阈值（ms），仅在 `semantic_punctuation_enabled=false` 时生效。
- `multi_threshold_mode_enabled`：VAD 多阈值模式，仅在 `semantic_punctuation_enabled=false` 时生效。
- `disfluency_removal_enabled`：是否去除口语赘词（如“嗯”“啊”）。
- `source_language` / `language_hints`：语言提示（按文档版本选择其一；若都支持，优先与当前模型示例一致）。
- `punctuation_prediction_enabled`、`inverse_text_normalization_enabled`：SDK 示例常见选项；若文档页面未列全，按实测兼容性开启。

### 1.3 客户端 -> 服务端：音频二进制帧

- 类型：Binary 帧。
- 内容：原始音频字节（如 PCM16）。
- 分包建议：20ms/40ms/100ms 均可；分包大小影响传输延迟，不直接决定服务端断句。

### 1.4 客户端 -> 服务端：`finish-task`

请求（JSON 文本帧）示例：

```json
{
  "header": {
    "action": "finish-task",
    "task_id": "uuid-xxxx",
    "streaming": "duplex"
  },
  "payload": {
    "input": {}
  }
}
```

### 1.5 服务端事件：`task-started`

- 语义：任务创建成功，可开始送音频。
- 建议：未收到该事件时不发送音频，避免隐性丢包。

### 1.6 服务端事件：`result-generated`

核心字段位于 `payload.output.sentence`：

- `begin_time`：句子起点（毫秒）。
- `end_time`：句子终点（毫秒）。中间结果常为 `null`。
- `text`：当前句文本。
- `sentence_end`：是否句子结束。
- `words`：可选词级时间戳信息。
- `heartbeat`：心跳标记，可忽略该条结果。

关键规则：

- 同一个 `begin_time` 的多次 `result-generated` 是“同句修正”，必须覆盖更新，不可盲目追加。
- 仅在 `sentence_end=true` 或 `end_time!=null` 时，把该句并入最终结果集合。

### 1.7 服务端事件：`task-finished` / `task-failed`

- `task-finished`：任务结束，不再返回新识别结果；此时做最终拼接与收尾。
- `task-failed`：任务失败，读取 `error_code` 与 `error_message` 做日志与重试策略。

### 1.8 推荐的最终拼接状态机

```python
# 说明：该示例强调“句子级覆盖更新 + 定稿归档”，用于彻底避免 A + A' + B 重复拼接问题。
final_sentences = {}          # key: begin_time, value: (begin_time, text)
partial_sentences = {}        # key: begin_time, value: text

def on_result_generated(sentence: dict) -> None:
    # 心跳包直接跳过，不参与拼接逻辑。
    if sentence.get("heartbeat") is True:
        return

    # begin_time 是句子稳定主键，缺失时要回退到兜底键，避免崩溃。
    begin_time = sentence.get("begin_time")
    key = begin_time if begin_time is not None else f"fallback-{id(sentence)}"
    text = str(sentence.get("text") or "")
    sentence_end = bool(sentence.get("sentence_end"))
    end_time = sentence.get("end_time")

    # 中间结果：仅覆盖同 key 的当前文本，不做最终追加。
    if not sentence_end and end_time is None:
        partial_sentences[key] = text
        return

    # 定稿结果：写入 final，并清理 partial。
    final_sentences[key] = (begin_time or 0, text)
    partial_sentences.pop(key, None)

def build_final_text() -> str:
    # 结束时按 begin_time 排序，拼接得到最终全文。
    ordered = sorted(final_sentences.values(), key=lambda x: x[0])
    return "".join(item[1] for item in ordered).strip()
```

## 2) 录音文件 REST API（异步任务）

### 2.1 提交任务

- URL：`POST https://dashscope.aliyuncs.com/api/v1/services/audio/asr/transcription`
- Headers：
  - `Authorization: Bearer $DASHSCOPE_API_KEY`
  - `Content-Type: application/json`
  - `X-DashScope-Async: enable`

请求示例：

```json
{
  "model": "fun-asr",
  "input": {
    "file_urls": [
      "https://example.com/audio.wav"
    ]
  },
  "parameters": {
    "channel_id": [
      0
    ],
    "disfluency_removal_enabled": false,
    "punctuation_prediction_enabled": true,
    "inverse_text_normalization_enabled": true
  }
}
```

提交响应常见字段：

- `output.task_id`
- `output.task_status`（通常初始 `PENDING`）
- `request_id`

### 2.2 查询任务

- URL：`https://dashscope.aliyuncs.com/api/v1/tasks/{task_id}`
- 官方文档示例以 `curl --location` 查询为主（即 GET 语义）。
- 状态流常见为：`PENDING` -> `RUNNING` -> `SUCCEEDED` / `FAILED`。

查询成功后常见字段：

- `output.task_status`
- `output.results[]`
  - `transcription_url`：可下载 JSON 结果链接（有有效期）。
  - `subtask_status`：每个文件/子任务状态。
  - `subtask_result`：子任务识别元信息。

### 2.3 下载结果文件（`transcription_url`）

下载 JSON 常见结构：

- `transcripts[]`
  - `text`：展示文本
  - `sentences[]`
    - `text`
    - `begin_time` / `end_time`
    - `words[]`
      - `text`
      - `begin_time` / `end_time`
      - `punctuation`

工程建议：

- 若有 `words`，优先用词级时间戳生成 `tokens/timestamps`。
- 若无 `words`，回退到“按字符切分 + 均匀时间戳”兜底，保证下游格式稳定。

### 2.4 文件与限额约束（按官方页面）

- 单文件最大 2GB。
- 音频时长上限 2 小时。
- 仅支持 URL 输入，不支持本地路径直传到该识别接口本身。

## 3) 临时 `oss://` URL 上传 API（HTTP 调用）

### 3.1 获取上传凭证

- URL：`GET https://dashscope.aliyuncs.com/api/v1/uploads?action=getPolicy&model={model}`
- Header：`Authorization: Bearer $DASHSCOPE_API_KEY`

响应常见字段：

- `upload_host`
- `upload_dir`
- `policy`
- `signature`
- `oss_access_key_id`
- `x_oss_security_token`
- `expire_time`

### 3.2 上传文件到 OSS

- 对 `upload_host` 发 multipart/form-data。
- 表单包含上一步凭证字段与 `file`。
- 上传成功后会得到对象 `key`（或由客户端按规则拼出）。

### 3.3 组装并使用临时 URL

- 临时 URL 形式：`oss://{key}`（有效期 48 小时，临时存储限流 100 QPS）。
- 用该 URL 调模型时，HTTP Header 增加：
  - `X-DashScope-OssResourceResolve: enable`

### 3.4 生产建议

- 临时 `oss://` 适合开发联调与低并发。
- 生产高并发改用自有 OSS/对象存储长期 URL，以便权限、审计、生命周期统一治理。

## 4) 与本项目代码映射（CapsWriter-Offline-fork）

### 4.1 实时链路

- 文件：`util/server/asr_aliyun_realtime.py`
- 已做：
  - `run-task` -> 发送音频帧 -> `finish-task` -> 等 `task-finished`。
  - 解析 `result-generated` 的 `sentence.text` 与 `sentence.words`。
- 待增强：
  - 当前以“最后一次文本”为主，建议升级到“按 `begin_time` 的句子状态机拼接”。

### 4.2 文件 REST 链路

- 文件：`util/client/transcribe/dashscope_rest_client.py`
- 已做：
  - 提交异步任务、轮询 `task_id`、解析 `results/transcripts/sentences/words`。
  - 输出统一结构：`text_display/text_accu/tokens/timestamps`。
- 待增强：
  - 若文档未来将查询接口严格化为 `POST`，需加可配置方法或自动回退机制。

### 4.3 上传链路

- 文件：`util/client/transcribe/file_upload_resolver.py`
- 已做：
  - `presigned_put`（第三方预签名）与 `custom_api`（自定义上传）两种模式。
- 可选补充：
  - 增加“官方 `getPolicy` + 直传 OSS + 生成 `oss://`”模式，减少外部依赖。

## 5) 最小可运行示例（教学模板）

### 5.1 实时 WS（最小状态机版）

```python
import asyncio
import json
import uuid
import websockets

API_KEY = "你的DASHSCOPE_API_KEY"
ENDPOINT = "wss://dashscope.aliyuncs.com/api-ws/v1/inference"

async def run_realtime(pcm_chunks: list[bytes]) -> str:
    # 每个会话使用独立 task_id，避免跨会话事件混淆。
    task_id = uuid.uuid4().hex
    final_sentences = {}
    partial_sentences = {}

    # run-task 请求，声明识别任务和参数。
    run_task = {
        "header": {"action": "run-task", "task_id": task_id, "streaming": "duplex"},
        "payload": {
            "task_group": "audio",
            "task": "asr",
            "function": "recognition",
            "model": "fun-asr-realtime-v2",
            "parameters": {"format": "pcm", "sample_rate": 16000, "semantic_punctuation_enabled": False},
            "input": {}
        }
    }

    # finish-task 请求，告知服务端音频发送完毕。
    finish_task = {
        "header": {"action": "finish-task", "task_id": task_id, "streaming": "duplex"},
        "payload": {"input": {}}
    }

    async with websockets.connect(
        ENDPOINT,
        additional_headers={"Authorization": f"Bearer {API_KEY}"},
        max_size=None
    ) as ws:
        # 第一步：发 run-task。
        await ws.send(json.dumps(run_task, ensure_ascii=False))

        # 第二步：等待 task-started 再送音频，避免早发导致丢帧。
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("header", {}).get("event") == "task-started":
                break

        # 第三步：按 chunk 连续发送音频二进制帧。
        for chunk in pcm_chunks:
            await ws.send(chunk)

        # 第四步：发送 finish-task。
        await ws.send(json.dumps(finish_task, ensure_ascii=False))

        # 第五步：持续收包直到 task-finished。
        while True:
            msg = json.loads(await ws.recv())
            event = msg.get("header", {}).get("event")
            if event == "result-generated":
                sentence = msg.get("payload", {}).get("output", {}).get("sentence", {}) or {}
                if sentence.get("heartbeat") is True:
                    continue
                begin = sentence.get("begin_time")
                key = begin if begin is not None else f"fallback-{id(sentence)}"
                text = str(sentence.get("text") or "")
                if sentence.get("sentence_end") or sentence.get("end_time") is not None:
                    final_sentences[key] = (begin or 0, text)
                    partial_sentences.pop(key, None)
                else:
                    partial_sentences[key] = text
            elif event == "task-finished":
                break
            elif event == "task-failed":
                raise RuntimeError(msg)

    # 任务结束后按 begin_time 排序拼接最终全文。
    return "".join(x[1] for x in sorted(final_sentences.values(), key=lambda x: x[0])).strip()

```

### 5.2 文件 REST（提交 + 轮询 + 下载）

```python
import requests
import time

API_KEY = "你的DASHSCOPE_API_KEY"
SUBMIT_URL = "https://dashscope.aliyuncs.com/api/v1/services/audio/asr/transcription"

def transcribe_file_url(file_url: str) -> dict:
    # 提交异步任务，务必带 X-DashScope-Async。
    submit_resp = requests.post(
        SUBMIT_URL,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "X-DashScope-Async": "enable",
        },
        json={"model": "fun-asr", "input": {"file_urls": [file_url]}},
        timeout=60,
    )
    submit_resp.raise_for_status()
    task_id = submit_resp.json()["output"]["task_id"]

    # 轮询任务状态直到终态。
    task_url = f"https://dashscope.aliyuncs.com/api/v1/tasks/{task_id}"
    while True:
        q = requests.get(task_url, headers={"Authorization": f"Bearer {API_KEY}"}, timeout=30)
        q.raise_for_status()
        data = q.json()
        status = data.get("output", {}).get("task_status")
        if status == "SUCCEEDED":
            return data
        if status in ("FAILED", "CANCELED"):
            raise RuntimeError(data)
        time.sleep(1.0)
```
