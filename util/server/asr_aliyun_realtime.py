# coding: utf-8
"""
阿里云百炼实时 ASR 会话管理器。

设计目标：
1. 一个本地录音任务（task_id）对应一个云端 WebSocket 会话，不再本地 60 秒切段后再拼接。
2. 让云端负责分句与标点，本地只做句子级状态机归档：
   - `sentence_end=false`: 覆盖更新当前句
   - `sentence_end=true` 或 `end_time!=null`: 句子定稿
3. 在 `finish-task` + `task-finished` 后一次性产出最终文本，避免中间快照重复叠加。
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import websockets

from util.logger import get_logger

logger = get_logger('server')


@dataclass
class AliyunFinalResult:
    """单次实时会话的最终结果。"""
    text: str
    tokens: List[str]
    timestamps: List[float]
    duration: float
    time_start: float
    time_submit: float


@dataclass
class _SentenceState:
    """句子状态：同一句会被多次覆盖更新，直到定稿。"""
    key: str
    order: int
    begin_time: Optional[float] = None
    text: str = ''
    is_final: bool = False
    tokens: List[str] = field(default_factory=list)
    timestamps: List[float] = field(default_factory=list)


@dataclass
class _SessionState:
    """云端实时会话状态。"""
    local_task_id: str
    cloud_task_id: str
    samplerate: int
    ws: object
    reader_task: asyncio.Task
    time_start: float
    time_submit: float
    duration: float = 0.0
    finished_event: asyncio.Event = field(default_factory=asyncio.Event)
    error_message: str = ''
    sentence_seq: int = 0
    sentences: Dict[str, _SentenceState] = field(default_factory=dict)


class AliyunRealtimeRecognizer:
    """
    百炼实时识别器（会话模式）。

    调用方式（同步）：
    - `process_task(task)` 非 final：推送音频 chunk，返回 None
    - `process_task(task)` final：发送 finish-task，等待 task-finished，返回最终结果
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

        self._sessions: Dict[str, _SessionState] = {}
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._run_loop,
            name='aliyun-realtime-loop',
            daemon=True,
        )
        self._loop_thread.start()

    def process_task(self, task) -> Optional[AliyunFinalResult]:
        """
        处理单个输入任务（由识别进程主循环同步调用）。

        输入任务来自 `server_ws_recv.py`：
        - 非 final 任务：携带 100ms 左右音频数据
        - final 任务：仅作为“会话结束”控制信号
        """
        self._ensure_session(
            local_task_id=task.task_id,
            samplerate=int(task.samplerate),
            time_start=float(task.time_start),
            time_submit=float(task.time_submit),
        )
        session = self._sessions[task.task_id]

        samples = np.frombuffer(task.data, dtype=np.float32) if task.data else np.array([], dtype=np.float32)
        if samples.size > 0:
            session.duration += len(samples) / max(1, int(task.samplerate))
            pcm_bytes = self._float32_to_pcm16_bytes(samples)
            self._run_coro(
                self._send_pcm(local_task_id=task.task_id, pcm_bytes=pcm_bytes),
                timeout=self.response_timeout,
            )

        if not task.is_final:
            return None

        return self._run_coro(
            self._finish_and_collect(local_task_id=task.task_id),
            timeout=max(120.0, self.response_timeout * 3),
        )

    def close(self) -> None:
        """关闭全部会话并停止后台事件循环线程。"""
        try:
            for local_task_id in list(self._sessions.keys()):
                try:
                    self._run_coro(self._close_session(local_task_id), timeout=10.0)
                except Exception:
                    logger.warning(f"关闭会话失败: {local_task_id}", exc_info=True)
        finally:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            if self._loop_thread.is_alive():
                self._loop_thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        """后台线程：专门运行一个 asyncio 事件循环，承载所有云端 WS 会话。"""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run_coro(self, coro, timeout: float):
        """在后台事件循环中同步执行协程。"""
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout=timeout)

    def _ensure_session(
        self,
        local_task_id: str,
        samplerate: int,
        time_start: float,
        time_submit: float,
    ) -> None:
        """按需创建云端会话，保证一个 local task 对应一个 WS 连接。"""
        if local_task_id in self._sessions:
            return
        self._run_coro(
            self._open_session(
                local_task_id=local_task_id,
                samplerate=samplerate,
                time_start=time_start,
                time_submit=time_submit,
            ),
            timeout=self.connect_timeout + 10.0,
        )

    async def _open_session(
        self,
        local_task_id: str,
        samplerate: int,
        time_start: float,
        time_submit: float,
    ) -> None:
        """建立云端 WS 连接并发送 run-task。"""
        cloud_task_id = uuid.uuid4().hex
        headers = {"Authorization": f"Bearer {self.api_key}"}

        ws = await websockets.connect(
            self.endpoint,
            additional_headers=headers,
            open_timeout=self.connect_timeout,
            max_size=None,
            ping_interval=20,
            ping_timeout=20,
        )

        run_task = {
            "header": {
                "action": "run-task",
                "task_id": cloud_task_id,
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
        await ws.send(json.dumps(run_task, ensure_ascii=False))

        session = _SessionState(
            local_task_id=local_task_id,
            cloud_task_id=cloud_task_id,
            samplerate=samplerate,
            ws=ws,
            reader_task=None,
            time_start=time_start,
            time_submit=time_submit,
        )
        session.reader_task = asyncio.create_task(self._reader_loop(session))
        self._sessions[local_task_id] = session
        logger.info(f"已创建百炼实时会话: local_task_id={local_task_id}, cloud_task_id={cloud_task_id}")

    async def _send_pcm(self, local_task_id: str, pcm_bytes: bytes) -> None:
        """向指定会话发送音频二进制帧。"""
        if not pcm_bytes:
            return
        session = self._sessions.get(local_task_id)
        if not session:
            return
        await session.ws.send(pcm_bytes)

    async def _finish_and_collect(self, local_task_id: str) -> AliyunFinalResult:
        """发送 finish-task，等待 task-finished，然后汇总最终文本。"""
        session = self._sessions.get(local_task_id)
        if not session:
            return AliyunFinalResult(
                text='',
                tokens=[],
                timestamps=[],
                duration=0.0,
                time_start=0.0,
                time_submit=0.0,
            )

        finish_task = {
            "header": {
                "action": "finish-task",
                "task_id": session.cloud_task_id,
                "streaming": "duplex",
            },
            "payload": {"input": {}},
        }
        try:
            await session.ws.send(json.dumps(finish_task, ensure_ascii=False))
            await asyncio.wait_for(session.finished_event.wait(), timeout=max(120.0, self.response_timeout * 3))
            if session.error_message:
                raise RuntimeError(session.error_message)

            text, tokens, timestamps = self._build_final_text(session)
            return AliyunFinalResult(
                text=text,
                tokens=tokens,
                timestamps=timestamps,
                duration=session.duration,
                time_start=session.time_start,
                time_submit=session.time_submit,
            )
        finally:
            await self._close_session(local_task_id)

    async def _close_session(self, local_task_id: str) -> None:
        """关闭并清理会话对象。"""
        session = self._sessions.pop(local_task_id, None)
        if not session:
            return

        try:
            if session.reader_task and not session.reader_task.done():
                session.reader_task.cancel()
                try:
                    await session.reader_task
                except asyncio.CancelledError:
                    pass
        finally:
            try:
                await session.ws.close()
            except Exception:
                pass
        logger.info(f"已关闭百炼实时会话: local_task_id={local_task_id}")

    async def _reader_loop(self, session: _SessionState) -> None:
        """后台读取云端事件并更新句子状态机。"""
        try:
            while True:
                raw = await session.ws.recv()
                if isinstance(raw, bytes):
                    continue

                message = json.loads(raw)
                header = message.get("header", {})
                event = header.get("event", "")

                if event == "result-generated":
                    self._consume_result_generated(session, message)
                    continue

                if event == "task-finished":
                    session.finished_event.set()
                    return

                if event == "task-failed":
                    session.error_message = f"阿里云实时识别失败: {message}"
                    session.finished_event.set()
                    return
        except Exception as e:
            session.error_message = f"读取云端结果失败: {e}"
            session.finished_event.set()

    def _consume_result_generated(self, session: _SessionState, message: dict) -> None:
        """
        消费一条 result-generated 事件。

        状态机规则：
        - 同一句（key 通常由 begin_time 决定）反复覆盖更新
        - `sentence_end=true` 或 `end_time!=null` 时标记为最终句
        """
        payload = message.get("payload", {})
        output = payload.get("output", {}) if isinstance(payload, dict) else {}
        sentence = output.get("sentence", {}) if isinstance(output, dict) else {}
        if not isinstance(sentence, dict):
            return

        # 文档说明 heartbeat 可以直接跳过，不参与文本汇总。
        heartbeat = bool(sentence.get("heartbeat") or output.get("heartbeat"))
        if heartbeat:
            return

        text = str(sentence.get("text") or output.get("text") or "").strip()
        words = sentence.get("words", [])
        begin_time = self._to_seconds(sentence.get("begin_time"))
        end_time = sentence.get("end_time")
        sentence_end = bool(sentence.get("sentence_end"))

        if not text and not words:
            return

        key = self._build_sentence_key(session=session, begin_time=begin_time, sentence=sentence)
        sentence_state = session.sentences.get(key)
        if sentence_state is None:
            sentence_state = _SentenceState(key=key, order=session.sentence_seq, begin_time=begin_time)
            session.sentence_seq += 1
            session.sentences[key] = sentence_state

        if begin_time is not None:
            sentence_state.begin_time = begin_time
        if text:
            sentence_state.text = text

        tokens, timestamps = self._extract_words(words)
        if tokens:
            sentence_state.tokens = tokens
            sentence_state.timestamps = timestamps

        if sentence_end or end_time is not None:
            sentence_state.is_final = True

    def _build_sentence_key(
        self,
        session: _SessionState,
        begin_time: Optional[float],
        sentence: dict,
    ) -> str:
        """
        构造句子 key。

        优先使用 begin_time（官方推荐可区分句子），缺失时退化到句子 ID / 未完成句。
        """
        if begin_time is not None:
            return f"begin:{int(begin_time * 1000)}"

        sentence_id = sentence.get("sentence_id")
        if sentence_id is not None:
            return f"id:{sentence_id}"

        for key, state in session.sentences.items():
            if not state.is_final:
                return key

        return f"unknown:{session.sentence_seq}"

    def _build_final_text(self, session: _SessionState) -> Tuple[str, List[str], List[float]]:
        """把句子状态机汇总为最终文本与时间戳。"""
        finals = [item for item in session.sentences.values() if item.is_final]
        if not finals:
            finals = list(session.sentences.values())

        finals.sort(key=lambda item: (
            item.begin_time if item.begin_time is not None else float('inf'),
            item.order,
        ))

        text = ''.join(item.text for item in finals if item.text).strip()

        tokens: List[str] = []
        timestamps: List[float] = []
        for item in finals:
            if not item.tokens:
                continue
            tokens.extend(item.tokens)
            timestamps.extend(item.timestamps)

        # 兜底：若 words 缺失，仍保证 text_accu 链路可继续。
        if not tokens and text:
            tokens = [ch for ch in text.replace(' ', '')]
            timestamps = []

        return text, tokens, timestamps

    @staticmethod
    def _extract_words(words: object) -> Tuple[List[str], List[float]]:
        """从 words 字段提取 token 与时间戳（秒）。"""
        if not isinstance(words, list):
            return [], []

        tokens: List[str] = []
        timestamps: List[float] = []
        for item in words:
            if not isinstance(item, dict):
                continue

            base_text = str(item.get("text", "")).strip()
            punctuation = str(item.get("punctuation", ""))
            token = f"{base_text}{punctuation}".strip()
            if not token:
                continue

            begin_time = AliyunRealtimeRecognizer._to_seconds(item.get("begin_time"))
            if begin_time is None:
                continue

            tokens.append(token)
            timestamps.append(begin_time)

        return tokens, timestamps

    @staticmethod
    def _to_seconds(value: object) -> Optional[float]:
        """把毫秒或秒统一转成秒。"""
        if value is None:
            return None
        try:
            # JSON 里大多是整数毫秒；字符串纯数字也按毫秒处理。
            if isinstance(value, int):
                return float(value) / 1000.0
            if isinstance(value, str) and value.isdigit():
                return float(value) / 1000.0

            number = float(value)
            if number > 1000:
                return number / 1000.0
            return number
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _float32_to_pcm16_bytes(samples: np.ndarray) -> bytes:
        """把 float32 [-1,1] 音频转为 PCM16 字节流。"""
        clipped = np.clip(samples, -1.0, 1.0)
        pcm16 = (clipped * 32767.0).astype(np.int16)
        return pcm16.tobytes()
