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
from concurrent.futures import TimeoutError as FutureTimeoutError
import json
import os
from pathlib import Path
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import websockets

from util.logger import get_logger
from util.server.failed_task_store import FailedTaskStore
from util.server.server_cosmic import console

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
    # 扩展字段：成功/失败保底都走同一个结果对象，方便上层统一处理。
    status: str = 'success_confirmed'
    error_code: str = ''
    error_message: str = ''
    salvage_text_finalized: str = ''
    salvage_text_partial: str = ''
    needs_manual_retry: bool = False
    retry_task_ref: str = ''


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
    ws: Optional[object]
    reader_task: Optional[asyncio.Task]
    time_start: float
    time_submit: float
    duration: float = 0.0
    finished_event: asyncio.Event = field(default_factory=asyncio.Event)
    error_message: str = ''
    sentence_seq: int = 0
    sentences: Dict[str, _SentenceState] = field(default_factory=dict)
    # 下面是“稳健性与保底恢复”所需状态字段
    last_event_time: float = 0.0
    result_generated_count: int = 0
    final_sentence_count: int = 0
    partial_sentence_count: int = 0
    finish_sent_time: float = 0.0
    task_finished_received: bool = False
    failure_reason: str = ''
    failure_detail: str = ''
    pcm_spool_path: str = ''
    ws_events: List[dict] = field(default_factory=list)
    retry_attempted: bool = False
    stream_unavailable: bool = False


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
        finish_confirm_timeout: Optional[float] = None,
        auto_retry_once: bool = True,
        retry_backoff_seconds: float = 0.5,
        failed_task_store_enabled: bool = True,
        failed_task_store_dir: str = 'runtime/failed_tasks',
        failed_audio_delete_on_replay_success: bool = True,
        ws_log_verbosity: str = 'summary',
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
        self.finish_confirm_timeout = float(finish_confirm_timeout) if finish_confirm_timeout else max(120.0, self.response_timeout * 3)
        self.auto_retry_once = bool(auto_retry_once)
        self.retry_backoff_seconds = float(retry_backoff_seconds)
        self.failed_task_store_enabled = bool(failed_task_store_enabled)
        self.failed_audio_delete_on_replay_success = bool(failed_audio_delete_on_replay_success)
        self.ws_log_verbosity = str(ws_log_verbosity or 'summary')
        self._failed_task_store = FailedTaskStore(failed_task_store_dir) if self.failed_task_store_enabled else None
        self._spool_dir = Path(failed_task_store_dir) / '_spool'
        self._spool_dir.mkdir(parents=True, exist_ok=True)

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
        samples = np.frombuffer(task.data, dtype=np.float32) if task.data else np.array([], dtype=np.float32)
        pcm_bytes = b''
        if samples.size > 0:
            pcm_bytes = self._float32_to_pcm16_bytes(samples)

        # 先尝试确保会话存在；如果建连失败，也要保留会话级别上下文（用于最终保底与重试）。
        try:
            self._ensure_session(
                local_task_id=task.task_id,
                samplerate=int(task.samplerate),
                time_start=float(task.time_start),
                time_submit=float(task.time_submit),
            )
        except Exception as e:
            session = self._sessions.get(task.task_id)
            if session is None:
                session = self._create_placeholder_session(
                    local_task_id=task.task_id,
                    samplerate=int(task.samplerate),
                    time_start=float(task.time_start),
                    time_submit=float(task.time_submit),
                )
                self._sessions[task.task_id] = session
            self._mark_session_failure(session, 'open_session_error', f'{type(e).__name__}: {e}')
            logger.error("创建百炼实时会话失败: local_task_id=%s", task.task_id, exc_info=True)

        session = self._sessions[task.task_id]

        # 不论云端会话是否正常，都先把整次会话音频持续落盘，保证后续可重试。
        if samples.size > 0:
            session.duration += len(samples) / max(1, int(task.samplerate))
            self._append_pcm_spool(session, pcm_bytes)
            if not session.failure_reason:
                try:
                    self._run_coro(
                        self._send_pcm(local_task_id=task.task_id, pcm_bytes=pcm_bytes),
                        timeout=self.response_timeout,
                    )
                except FutureTimeoutError as e:
                    self._mark_session_failure(session, 'send_pcm_error', f'发送音频超时: {e}')
                except Exception as e:
                    self._mark_session_failure(session, 'send_pcm_error', f'{type(e).__name__}: {e}')

        if not task.is_final:
            return None

        return self._finalize_task(task)

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

    def _create_placeholder_session(
        self,
        *,
        local_task_id: str,
        samplerate: int,
        time_start: float,
        time_submit: float,
    ) -> _SessionState:
        """
        创建“占位会话”。

        作用：
        - 当云端连接在任务早期就失败时，仍然保留会话级状态（音频落盘、错误信息、后续重试）。
        - 这样用户继续说话直到抬键，整次录音仍然能作为一个会话文件重试。
        """
        return _SessionState(
            local_task_id=local_task_id,
            cloud_task_id='',
            samplerate=samplerate,
            ws=None,
            reader_task=None,
            time_start=time_start,
            time_submit=time_submit,
            last_event_time=time.time(),
            pcm_spool_path=str(self._build_spool_path(local_task_id, time_start)),
        )

    def _build_spool_path(self, local_task_id: str, time_start: float) -> Path:
        """为当前录音会话生成 PCM 临时落盘路径（按日期分目录）。"""
        date_key = time.strftime('%Y%m%d', time.localtime(time_start or time.time()))
        folder = self._spool_dir / date_key
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f'{local_task_id}.pcm'

    def _append_pcm_spool(self, session: _SessionState, pcm_bytes: bytes) -> None:
        """把收到的 PCM16 数据追加写入会话临时文件，供失败后重试使用。"""
        if not pcm_bytes:
            return
        if not session.pcm_spool_path:
            session.pcm_spool_path = str(self._build_spool_path(session.local_task_id, session.time_start))
        path = Path(session.pcm_spool_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('ab') as f:
            f.write(pcm_bytes)

    def _mark_session_failure(self, session: _SessionState, reason: str, detail: str) -> None:
        """
        标记会话失败（幂等）。

        注意：这里不立即删除会话，因为用户可能还没抬键，后续音频仍需继续落盘。
        """
        if not session.failure_reason:
            session.failure_reason = reason
            session.failure_detail = detail
        else:
            # 保留首个失败原因，后续异常追加到 detail 末尾便于排障。
            if detail and detail not in session.failure_detail:
                session.failure_detail = f"{session.failure_detail} | {detail}".strip(" |")
        session.stream_unavailable = True
        session.error_message = session.error_message or detail or reason
        session.finished_event.set()
        session.ws_events.append({
            't': time.time(),
            'event': 'local-failure',
            'reason': session.failure_reason,
            'detail': session.failure_detail,
            'local_task_id': session.local_task_id,
            'cloud_task_id': session.cloud_task_id,
        })
        logger.warning(
            "百炼实时会话标记失败: local_task_id=%s, reason=%s, detail=%s",
            session.local_task_id, session.failure_reason, session.failure_detail
        )

    def _finalize_task(self, task) -> AliyunFinalResult:
        """
        处理 final 控制任务：
        - 正常路径：发送 finish-task，等待 task-finished，返回最终结果
        - 异常路径：保底汇总、保存失败任务、尝试自动重试一次
        """
        session = self._sessions.get(task.task_id)
        if session is None:
            return AliyunFinalResult(
                text='',
                tokens=[],
                timestamps=[],
                duration=0.0,
                time_start=float(task.time_start),
                time_submit=float(task.time_submit),
                status='failed_no_text',
                error_code='session_missing',
                error_message='final 到达时找不到会话状态',
                needs_manual_retry=False,
            )

        try:
            # 若会话已在中途失败（例如网络断开），直接走保底/重试，不再等待 finish。
            if session.failure_reason:
                result = self._build_failure_result(session, error_code=session.failure_reason)
            else:
                result = self._run_coro(
                    self._finish_and_collect(local_task_id=task.task_id),
                    timeout=max(self.finish_confirm_timeout + 15.0, self.response_timeout * 4),
                )
        except FutureTimeoutError as e:
            self._mark_session_failure(session, 'finish_confirm_timeout', f'等待 finish 确认超时: {e}')
            result = self._build_failure_result(session, error_code='finish_confirm_timeout')
        except Exception as e:
            self._mark_session_failure(session, 'finish_collect_error', f'{type(e).__name__}: {e}')
            logger.error("final 收尾失败: local_task_id=%s", task.task_id, exc_info=True)
            result = self._build_failure_result(session, error_code='finish_collect_error')

        # 自动重试一次（使用整次会话 PCM 临时文件），成功则当作最终成功结果返回。
        if (
            result.status != 'success_confirmed'
            and self.auto_retry_once
            and not session.retry_attempted
            and session.pcm_spool_path
            and Path(session.pcm_spool_path).exists()
        ):
            session.retry_attempted = True
            retry_result = self._auto_retry_from_spool(session)
            if retry_result is not None and retry_result.status == 'success_confirmed':
                logger.warning("失败任务自动重试成功: local_task_id=%s", session.local_task_id)
                self._delete_spool_file(session.pcm_spool_path)
                result = retry_result

        # 未成功则保存失败任务记录（包含音频/保底文本/事件摘要）
        if result.status != 'success_confirmed':
            task_ref = self._persist_failure_record(session, result)
            result.needs_manual_retry = bool(task_ref)
            result.retry_task_ref = task_ref or ''

        # 成功时可删除临时 PCM，避免长期堆积
        if result.status == 'success_confirmed':
            self._delete_spool_file(session.pcm_spool_path)

        # 无论成功/失败，都清理会话对象；失败音频已复制到 failed_task_store 时不影响。
        try:
            self._run_coro(self._close_session(task.task_id), timeout=10.0)
        except Exception:
            logger.warning("关闭会话失败: %s", task.task_id, exc_info=True)
        return result

    def _build_failure_result(self, session: _SessionState, error_code: str) -> AliyunFinalResult:
        """把当前会话状态构造成“失败但有保底内容”的统一结果对象。"""
        finalized_text, partial_text, combined_text, tokens, timestamps, summary = self.build_salvage_snapshot(session)
        status = 'failed_no_text'
        if finalized_text or partial_text:
            status = 'timeout_salvaged' if error_code == 'finish_confirm_timeout' else 'error_salvaged'

        # 在服务端终端打印保底内容，方便现场人工复制。
        self._print_salvage_to_console(
            session=session,
            error_code=error_code,
            finalized_text=finalized_text,
            partial_text=partial_text,
        )

        return AliyunFinalResult(
            text=combined_text,
            tokens=tokens,
            timestamps=timestamps,
            duration=session.duration,
            time_start=session.time_start,
            time_submit=session.time_submit,
            status=status,
            error_code=error_code,
            error_message=session.failure_detail or session.error_message or error_code,
            salvage_text_finalized=finalized_text,
            salvage_text_partial=partial_text,
            needs_manual_retry=False,
            retry_task_ref='',
        )

    def _persist_failure_record(self, session: _SessionState, result: AliyunFinalResult) -> str:
        """把失败任务落盘到 failed_task_store，返回 task_ref（失败时返回空字符串）。"""
        if not self._failed_task_store:
            return ''
        try:
            finalized_text, partial_text, combined_text, _tokens, _timestamps, sentences_summary = self.build_salvage_snapshot(session)
            return self._failed_task_store.record_failure(
                task_id=session.local_task_id,
                source='mic',
                time_start=session.time_start,
                error_code=result.error_code or session.failure_reason or 'unknown_error',
                error_message=result.error_message or session.failure_detail or session.error_message or '',
                salvage_text_finalized=finalized_text,
                salvage_text_partial=partial_text,
                salvage_text_combined=combined_text,
                sentences_summary=sentences_summary,
                ws_events=session.ws_events,
                audio_pcm_path=session.pcm_spool_path or None,
                metadata_extra={
                    'samplerate': session.samplerate,
                    'duration': session.duration,
                    'cloud_task_id': session.cloud_task_id,
                    'result_generated_count': session.result_generated_count,
                    'final_sentence_count': session.final_sentence_count,
                    'partial_sentence_count': session.partial_sentence_count,
                },
            )
        except Exception:
            logger.error("保存失败任务记录失败: local_task_id=%s", session.local_task_id, exc_info=True)
            return ''

    def _auto_retry_from_spool(self, session: _SessionState) -> Optional[AliyunFinalResult]:
        """
        使用整次会话 PCM 文件自动重试一次。

        说明：
        - 这里是“重新发整段会话音频到新云端会话”，不是断点续传原会话。
        - 成功后返回正常 `success_confirmed` 结果；失败则返回 None，交给手动重试入口。
        """
        pcm_path = Path(session.pcm_spool_path)
        if not pcm_path.exists():
            return None

        if self.retry_backoff_seconds > 0:
            time.sleep(self.retry_backoff_seconds)

        try:
            logger.warning("开始自动重试失败任务: local_task_id=%s, pcm=%s", session.local_task_id, pcm_path)
            return self.transcribe_pcm_file(
                pcm_path=str(pcm_path),
                samplerate=session.samplerate or 16000,
                time_start=session.time_start,
                time_submit=time.time(),
                local_task_id=f"{session.local_task_id}-retry",
                enable_spool=False,
            )
        except Exception:
            logger.error("自动重试失败: local_task_id=%s", session.local_task_id, exc_info=True)
            return None

    def _delete_spool_file(self, pcm_spool_path: str) -> None:
        """删除临时 PCM 会话文件（失败记录已复制后即可删除源文件）。"""
        if not pcm_spool_path:
            return
        try:
            path = Path(pcm_spool_path)
            if path.exists():
                path.unlink()
        except Exception:
            logger.warning("删除临时 PCM 文件失败: %s", pcm_spool_path, exc_info=True)

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
            last_event_time=time.time(),
            pcm_spool_path=str(self._build_spool_path(local_task_id, time_start)),
        )
        session.reader_task = asyncio.create_task(self._reader_loop(session))
        self._sessions[local_task_id] = session
        logger.info(f"已创建百炼实时会话: local_task_id={local_task_id}, cloud_task_id={cloud_task_id}")

    async def _send_pcm(self, local_task_id: str, pcm_bytes: bytes) -> None:
        """向指定会话发送音频二进制帧。"""
        if not pcm_bytes:
            return
        session = self._sessions.get(local_task_id)
        if not session or session.ws is None:
            return
        await session.ws.send(pcm_bytes)

    async def _finish_and_collect(self, local_task_id: str) -> AliyunFinalResult:
        """发送 finish-task，等待 task-finished，然后汇总最终文本（仅成功路径）。"""
        session = self._sessions.get(local_task_id)
        if not session:
            return AliyunFinalResult(
                text='',
                tokens=[],
                timestamps=[],
                duration=0.0,
                time_start=0.0,
                time_submit=0.0,
                status='failed_no_text',
                error_code='session_missing',
                error_message='会话不存在',
            )
        if session.ws is None:
            raise RuntimeError('会话未建立成功，无法发送 finish-task')

        finish_task = {
            "header": {
                "action": "finish-task",
                "task_id": session.cloud_task_id,
                "streaming": "duplex",
            },
            "payload": {"input": {}},
        }
        session.finish_sent_time = time.time()
        await session.ws.send(json.dumps(finish_task, ensure_ascii=False))
        self._record_ws_event(session, event='finish-task-sent', message=None)
        try:
            await asyncio.wait_for(session.finished_event.wait(), timeout=self.finish_confirm_timeout)
        except asyncio.TimeoutError as e:
            self._mark_session_failure(session, 'finish_confirm_timeout', f'等待 task-finished 超时: {e}')
            raise

        if session.error_message and not session.task_finished_received:
            raise RuntimeError(session.error_message)

        text, tokens, timestamps = self._build_final_text(session)
        return AliyunFinalResult(
            text=text,
            tokens=tokens,
            timestamps=timestamps,
            duration=session.duration,
            time_start=session.time_start,
            time_submit=session.time_submit,
            status='success_confirmed',
        )

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
                if session.ws is not None:
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
                session.last_event_time = time.time()
                self._record_ws_event(session, event=event, message=message)

                if event == "result-generated":
                    self._consume_result_generated(session, message)
                    continue

                if event == "task-started":
                    continue

                if event == "task-finished":
                    session.task_finished_received = True
                    session.finished_event.set()
                    return

                if event == "task-failed":
                    session.error_message = f"阿里云实时识别失败: {message}"
                    self._mark_session_failure(session, 'task_failed_event', session.error_message)
                    return
        except Exception as e:
            session.error_message = f"读取云端结果失败: {e}"
            self._mark_session_failure(session, 'ws_read_error', session.error_message)

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
            self._update_sentence_counters(session)
            return

        text = str(sentence.get("text") or output.get("text") or "").strip()
        words = sentence.get("words", [])
        begin_time = self._to_seconds(sentence.get("begin_time"))
        end_time = sentence.get("end_time")
        sentence_end = bool(sentence.get("sentence_end"))

        if not text and not words:
            self._update_sentence_counters(session)
            return

        session.result_generated_count += 1

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
            was_final = sentence_state.is_final
            sentence_state.is_final = True
            if not was_final and sentence_state.text:
                # 句子定稿时立即在服务端终端打印，出现最终失败时至少终端有“战果”。
                console.print(
                    f"[cyan]云端定稿[{session.local_task_id[:8]}][/]: {sentence_state.text}"
                )
                logger.info(
                    "云端句子定稿: local_task_id=%s, begin=%.3f, text=%s",
                    session.local_task_id,
                    sentence_state.begin_time or -1.0,
                    sentence_state.text,
                )
        self._update_sentence_counters(session)

    def _update_sentence_counters(self, session: _SessionState) -> None:
        """维护定稿句/未定稿句计数，便于日志摘要与失败元数据落盘。"""
        final_count = 0
        partial_count = 0
        for item in session.sentences.values():
            if item.is_final:
                final_count += 1
            else:
                partial_count += 1
        session.final_sentence_count = final_count
        session.partial_sentence_count = partial_count

    def _record_ws_event(self, session: _SessionState, event: str, message: Optional[dict]) -> None:
        """
        记录云端 WS 结构化事件摘要。

        默认只记录摘要（不落完整 payload），避免日志暴涨与敏感文本泄露。
        """
        now_ts = time.time()
        summary: Dict[str, Any] = {
            't': now_ts,
            'event': event or '',
            'local_task_id': session.local_task_id,
            'cloud_task_id': session.cloud_task_id,
            'final_sentence_count': session.final_sentence_count,
            'partial_sentence_count': session.partial_sentence_count,
        }
        if session.finish_sent_time:
            summary['finish_wait_s'] = round(max(0.0, now_ts - session.finish_sent_time), 3)

        if isinstance(message, dict):
            payload = message.get('payload', {})
            output = payload.get('output', {}) if isinstance(payload, dict) else {}
            sentence = output.get('sentence', {}) if isinstance(output, dict) else {}
            if isinstance(sentence, dict):
                summary['begin_time'] = sentence.get('begin_time')
                summary['end_time'] = sentence.get('end_time')
                summary['sentence_end'] = sentence.get('sentence_end')
                summary['heartbeat'] = sentence.get('heartbeat')
                text = str(sentence.get('text') or output.get('text') or '')
                summary['text_len'] = len(text)
                if self.ws_log_verbosity == 'full' and text:
                    summary['text_preview'] = text[:120]
                words = sentence.get('words')
                if isinstance(words, list):
                    summary['words_count'] = len(words)

            header = message.get('header', {})
            if isinstance(header, dict):
                error_msg = header.get('error_message') or header.get('message')
                if error_msg:
                    summary['header_message'] = str(error_msg)[:200]

        session.ws_events.append(summary)
        if len(session.ws_events) > 2000:
            # 防止超长会话无限增长，占用过多内存；失败排障留最近 2000 条摘要足够。
            session.ws_events = session.ws_events[-2000:]

        if self.ws_log_verbosity != 'error_only':
            logger.info("ASR_WS %s", json.dumps(summary, ensure_ascii=False))

    def build_salvage_snapshot(
        self,
        session: _SessionState,
    ) -> Tuple[str, str, str, List[str], List[float], List[dict]]:
        """
        构建失败保底快照。

        返回：
        - finalized_text: 已定稿句拼接文本
        - partial_text: 未定稿句拼接文本（可能不完整）
        - combined_text: 用于日志/客户端展示的组合文本（未定稿前加标记）
        - tokens/timestamps: 仅来自已定稿句（避免把未定稿时间戳当成可靠结果）
        - sentences_summary: 每句摘要，用于失败任务落盘
        """
        all_items = list(session.sentences.values())
        all_items.sort(key=lambda item: (
            item.begin_time if item.begin_time is not None else float('inf'),
            item.order,
        ))

        finals = [item for item in all_items if item.is_final]
        partials = [item for item in all_items if not item.is_final]

        finalized_text = ''.join(item.text for item in finals if item.text).strip()
        partial_text_raw = ''.join(item.text for item in partials if item.text).strip()
        partial_text = f"[未定稿]{partial_text_raw}" if partial_text_raw else ''
        combined_text = (finalized_text + partial_text).strip()

        tokens: List[str] = []
        timestamps: List[float] = []
        for item in finals:
            if item.tokens:
                tokens.extend(item.tokens)
                timestamps.extend(item.timestamps)

        if not tokens and finalized_text:
            tokens = [ch for ch in finalized_text.replace(' ', '')]

        sentences_summary: List[dict] = []
        for item in all_items:
            sentences_summary.append({
                'key': item.key,
                'order': item.order,
                'begin_time': item.begin_time,
                'is_final': item.is_final,
                'text': item.text,
                'tokens_count': len(item.tokens),
            })
        return finalized_text, partial_text, combined_text, tokens, timestamps, sentences_summary

    def _print_salvage_to_console(
        self,
        *,
        session: _SessionState,
        error_code: str,
        finalized_text: str,
        partial_text: str,
    ) -> None:
        """失败时把保底结果打印到服务端终端，防止最后一步失败导致战果完全不可见。"""
        console.print(
            f"[yellow]ASR任务失败[{session.local_task_id[:8]}][/]: {error_code} "
            f"(云端任务={session.cloud_task_id or 'N/A'})"
        )
        if finalized_text:
            console.print(f"  [cyan]已定稿保底[/]: {finalized_text}")
        if partial_text:
            console.print(f"  [yellow]未定稿保底[/]: {partial_text}")
        if not finalized_text and not partial_text:
            console.print("  [yellow]未拿到可用文本保底[/]")

    def transcribe_pcm_file(
        self,
        *,
        pcm_path: str,
        samplerate: int = 16000,
        time_start: Optional[float] = None,
        time_submit: Optional[float] = None,
        local_task_id: Optional[str] = None,
        enable_spool: bool = False,
    ) -> AliyunFinalResult:
        """
        使用 PCM16 文件执行一次“完整重放识别”（用于自动/手动重试）。

        参数约束：
        - `pcm_path` 是单声道 PCM16 原始数据文件（与实时发送格式一致）
        - 该方法不会自动上屏，只返回识别结果供调用方决定如何处理
        """
        pcm_file = Path(pcm_path)
        if not pcm_file.exists():
            raise FileNotFoundError(f"PCM 文件不存在: {pcm_file}")

        local_task_id = local_task_id or f"replay-{uuid.uuid4().hex}"
        ts_start = float(time_start or time.time())
        ts_submit = float(time_submit or time.time())

        self._ensure_session(
            local_task_id=local_task_id,
            samplerate=int(samplerate),
            time_start=ts_start,
            time_submit=ts_submit,
        )
        session = self._sessions[local_task_id]

        try:
            chunk_bytes = max(320, int(int(samplerate) * 2 * 0.1))  # 100ms PCM16 mono
            with pcm_file.open('rb') as f:
                while True:
                    chunk = f.read(chunk_bytes)
                    if not chunk:
                        break
                    session.duration += len(chunk) / (2.0 * max(1, int(samplerate)))
                    if enable_spool:
                        self._append_pcm_spool(session, chunk)
                    self._run_coro(self._send_pcm(local_task_id=local_task_id, pcm_bytes=chunk), timeout=self.response_timeout)

            result = self._run_coro(
                self._finish_and_collect(local_task_id=local_task_id),
                timeout=max(self.finish_confirm_timeout + 15.0, self.response_timeout * 4),
            )
            return result
        finally:
            try:
                self._run_coro(self._close_session(local_task_id), timeout=10.0)
            except Exception:
                logger.warning("重放识别关闭会话失败: %s", local_task_id, exc_info=True)

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
