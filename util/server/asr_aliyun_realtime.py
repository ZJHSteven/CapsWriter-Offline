# coding: utf-8
"""
阿里云百炼实时 ASR 适配器。

本模块的目标是把阿里云百炼实时 WebSocket 接口，封装成与当前项目
识别流程兼容的「recognizer + stream」接口：

- recognizer.create_stream()
- stream.accept_waveform(...)
- recognizer.decode_stream(stream)
- stream.result.{text,tokens,timestamps}

这样可以复用 `server_recognize.py` 现有的拼接、去重、后处理逻辑，
减少迁移时对主流程的改动范围。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import List, Tuple

import numpy as np
import websockets

from util.logger import get_logger

logger = get_logger('server')


@dataclass
class AliyunStreamResult:
    """单个片段识别结果容器。"""
    text: str = ''
    tokens: List[str] = field(default_factory=list)
    timestamps: List[float] = field(default_factory=list)


class AliyunRealtimeStream:
    """
    与现有识别流程兼容的流对象。

    当前实现按“单片段请求”工作：
    - `accept_waveform` 保存本片段音频
    - `decode_stream` 时一次性提交到云端并等待结果
    """

    def __init__(self) -> None:
        self.samplerate: int = 16000
        self.samples: np.ndarray = np.array([], dtype=np.float32)
        self.result = AliyunStreamResult()

    def accept_waveform(self, samplerate: int, samples: np.ndarray) -> None:
        self.samplerate = int(samplerate)
        self.samples = np.asarray(samples, dtype=np.float32)


class AliyunRealtimeRecognizer:
    """
    百炼实时识别器适配类。

    注意：
    - 这里使用 WebSocket 原生协议，避免引入额外 SDK 依赖。
    - 返回结果优先使用 `words` 字段提取 token 与时间戳；若服务端未返回
      words，则回退为“文本拆字 + 无时间戳”模式，由上层执行兜底。
    """

    def __init__(
        self,
        api_key: str,
        endpoint: str,
        model: str,
        source_language: str = 'auto',
        max_sentence_silence: int = 800,
        punctuation_enabled: bool = True,
        itn_enabled: bool = True,
        connect_timeout: float = 20.0,
        response_timeout: float = 40.0,
    ) -> None:
        if not api_key:
            raise ValueError("DASHSCOPE_API_KEY 未配置，无法使用 aliyun_realtime 模式")

        self.api_key = api_key
        self.endpoint = endpoint
        self.model = model
        self.source_language = source_language
        self.max_sentence_silence = int(max_sentence_silence)
        self.punctuation_enabled = bool(punctuation_enabled)
        self.itn_enabled = bool(itn_enabled)
        self.connect_timeout = float(connect_timeout)
        self.response_timeout = float(response_timeout)

    def create_stream(self) -> AliyunRealtimeStream:
        return AliyunRealtimeStream()

    def decode_stream(self, stream: AliyunRealtimeStream) -> None:
        """
        按本项目既有同步接口执行解码。

        识别子进程主循环是同步调用，因此这里使用 `asyncio.run` 执行
        一次异步 WebSocket 请求。
        """
        text, tokens, timestamps = asyncio.run(
            self._decode_once(stream.samples, stream.samplerate)
        )
        stream.result.text = text
        stream.result.tokens = tokens
        stream.result.timestamps = timestamps

    async def _decode_once(self, samples: np.ndarray, samplerate: int) -> Tuple[str, List[str], List[float]]:
        pcm16_bytes = self._float32_to_pcm16_bytes(samples)
        if not pcm16_bytes:
            return "", [], []

        task_id = uuid.uuid4().hex
        headers = {"Authorization": f"Bearer {self.api_key}"}

        run_task = {
            "header": {
                "action": "run-task",
                "task_id": task_id,
                "streaming": "duplex",
            },
            "payload": {
                "task_group": "audio",
                "task": "asr",
                "function": "recognition",
                "model": self.model,
                "parameters": {
                    "format": "pcm",
                    "sample_rate": samplerate,
                    "source_language": self.source_language,
                    "punctuation_prediction_enabled": self.punctuation_enabled,
                    "inverse_text_normalization_enabled": self.itn_enabled,
                    "max_sentence_silence": self.max_sentence_silence,
                },
                "input": {},
            },
        }

        finish_task = {
            "header": {
                "action": "finish-task",
                "task_id": task_id,
                "streaming": "duplex",
            },
            "payload": {
                "input": {},
            },
        }

        last_text = ""
        last_tokens: List[str] = []
        last_timestamps: List[float] = []

        async with websockets.connect(
            self.endpoint,
            additional_headers=headers,
            open_timeout=self.connect_timeout,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:
            await ws.send(json.dumps(run_task, ensure_ascii=False))

            # 以 100ms 帧发送，兼顾兼容性与实时服务稳定性。
            frame_bytes = max(1, int(samplerate * 0.1) * 2)
            for offset in range(0, len(pcm16_bytes), frame_bytes):
                await ws.send(pcm16_bytes[offset: offset + frame_bytes])

            await ws.send(json.dumps(finish_task, ensure_ascii=False))

            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.response_timeout)
                if isinstance(raw, bytes):
                    continue

                message = json.loads(raw)
                header = message.get("header", {})
                event = header.get("event", "")

                if event == "result-generated":
                    text, tokens, timestamps = self._extract_result(message)
                    if text:
                        last_text = text
                    if tokens:
                        last_tokens = tokens
                        last_timestamps = timestamps
                    continue

                if event == "task-failed":
                    raise RuntimeError(f"阿里云实时识别失败: {message}")

                if event == "task-finished":
                    break

        # 若服务端没有返回 words 字段，回退为文本拆字。
        if not last_tokens and last_text:
            last_tokens = [ch for ch in last_text.replace(" ", "")]
            last_timestamps = []

        return last_text, last_tokens, last_timestamps

    @staticmethod
    def _float32_to_pcm16_bytes(samples: np.ndarray) -> bytes:
        clipped = np.clip(samples, -1.0, 1.0)
        pcm16 = (clipped * 32767.0).astype(np.int16)
        return pcm16.tobytes()

    @staticmethod
    def _extract_result(message: dict) -> Tuple[str, List[str], List[float]]:
        payload = message.get("payload", {})
        output = payload.get("output", {})
        sentence = output.get("sentence", {}) if isinstance(output, dict) else {}

        text = sentence.get("text") or output.get("text") or ""
        words = sentence.get("words", []) if isinstance(sentence, dict) else []

        tokens: List[str] = []
        timestamps: List[float] = []

        for item in words:
            if not isinstance(item, dict):
                continue
            token = str(item.get("text", "")).strip()
            if not token:
                continue

            begin_time = item.get("begin_time")
            if begin_time is None:
                continue

            try:
                ts = float(begin_time)
            except (TypeError, ValueError):
                continue

            # 官方事件示例常见毫秒单位，这里统一转为秒。
            if ts > 1000:
                ts = ts / 1000.0

            tokens.append(token)
            timestamps.append(ts)

        return text, tokens, timestamps
