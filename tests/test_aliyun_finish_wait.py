# coding: utf-8
"""
阿里云实时识别 finish 等待策略测试。

这些测试不连接真实阿里云 WebSocket，只验证本地等待规则：
1. 云端持续有事件时，不应被固定 120 秒式的等待阈值误杀。
2. 云端完全没事件时，应按空闲超时失败，并保留结构化失败原因。
3. 长音频重放应得到更长的总上限，避免几分钟音频在重放阶段被固定小超时截断。
"""

from __future__ import annotations

import asyncio
import unittest

from util.server.asr_aliyun_realtime import (
    AliyunRealtimeRecognizer,
    _EmptyFinalSpan,
    _SentenceState,
    _SessionState,
)


def _new_recognizer_for_wait_test(
    *,
    finish_confirm_timeout: float = 0.2,
    response_timeout: float = 0.1,
) -> AliyunRealtimeRecognizer:
    """
    创建一个“只用于测试等待算法”的识别器对象。

    注意：
    - 这里故意使用 `__new__`，不走正常构造函数。
    - 正常构造函数会启动后台事件循环线程并校验真实 API Key；本测试只测纯等待逻辑，
      不需要也不应该连接外部服务。
    """
    recognizer = AliyunRealtimeRecognizer.__new__(AliyunRealtimeRecognizer)
    recognizer.finish_confirm_timeout = finish_confirm_timeout
    recognizer.response_timeout = response_timeout
    recognizer.empty_result_retry_on_voice = True
    recognizer.voice_rms_threshold = 0.003
    recognizer.voice_peak_threshold = 0.02
    recognizer.tail_empty_retry_enabled = True
    recognizer.tail_empty_min_seconds = 15.0
    recognizer.tail_empty_end_tolerance_seconds = 3.0
    recognizer.ws_log_verbosity = 'error_only'
    return recognizer


def _new_session_for_wait_test(*, duration: float = 0.0) -> _SessionState:
    """
    创建最小可用会话状态。

    字段说明：
    - `finished_event` 由 `_SessionState` 默认工厂创建，用来模拟 `task-finished` 到达。
    - `last_event_time` / `finish_sent_time` 在具体测试里按时间线写入。
    """
    return _SessionState(
        local_task_id="unit-test-task",
        cloud_task_id="unit-test-cloud-task",
        samplerate=16000,
        ws=None,
        reader_task=None,
        time_start=0.0,
        time_submit=0.0,
        duration=duration,
    )


class AliyunFinishWaitTest(unittest.TestCase):
    """覆盖 `finish-task` 后的空闲超时与长音频总上限策略。"""

    def test_cloud_events_extend_finish_wait(self) -> None:
        """
        云端持续返回事件时，应继续等待直到 `task-finished`。

        这个用例模拟：
        - 空闲超时是 0.2 秒。
        - 0.15 秒时后台 reader 又收到一个云端事件，刷新 `last_event_time`。
        - 0.30 秒时 `task-finished` 到达。

        如果仍然使用旧的“固定 0.2 秒等待”，这个用例会失败；新策略应通过。
        """

        async def scenario() -> None:
            recognizer = _new_recognizer_for_wait_test(finish_confirm_timeout=0.2)
            session = _new_session_for_wait_test()
            now = asyncio.get_running_loop().time()

            # `time.time()` 与事件循环时间不是同一来源，测试中用真实 wall clock 更贴近生产代码。
            import time

            session.finish_sent_time = time.time()
            session.last_event_time = session.finish_sent_time

            async def refresh_then_finish() -> None:
                await asyncio.sleep(0.15)
                session.last_event_time = time.time()
                await asyncio.sleep(0.15)
                session.task_finished_received = True
                session.finished_event.set()

            asyncio.create_task(refresh_then_finish())
            await recognizer._wait_for_finish_confirmation(session)

            # 断言等待确实跨过了最初 0.2 秒阈值，证明“有云端事件就续等”生效。
            self.assertGreaterEqual(asyncio.get_running_loop().time() - now, 0.25)
            self.assertEqual(session.failure_reason, "")

        asyncio.run(scenario())

    def test_idle_timeout_marks_structured_failure(self) -> None:
        """
        云端没有任何新事件时，应按空闲超时失败。

        这能防止网络断开、服务端不再推送事件时，进程一直挂住。
        """

        async def scenario() -> None:
            recognizer = _new_recognizer_for_wait_test(finish_confirm_timeout=0.05)
            session = _new_session_for_wait_test()

            import time

            session.finish_sent_time = time.time()
            session.last_event_time = session.finish_sent_time

            with self.assertRaises(asyncio.TimeoutError):
                await recognizer._wait_for_finish_confirmation(session)

            self.assertEqual(session.failure_reason, "finish_confirm_timeout")
            self.assertIn("空闲超时", session.failure_detail)
            self.assertTrue(session.stream_unavailable)

        asyncio.run(scenario())

    def test_long_audio_gets_longer_total_timeout(self) -> None:
        """
        长音频重放应按音频时长放大总上限。

        这里不关心真实云端速度，只检查策略本身：
        - 短音频总上限主要由空闲超时/响应超时决定。
        - 长音频总上限会按 `duration * 1.5 + idle_timeout` 增长。
        """
        recognizer = _new_recognizer_for_wait_test(
            finish_confirm_timeout=120.0,
            response_timeout=40.0,
        )
        short_session = _new_session_for_wait_test(duration=10.0)
        long_session = _new_session_for_wait_test(duration=550.25)

        self.assertGreater(
            recognizer._compute_finish_total_timeout(long_session),
            recognizer._compute_finish_total_timeout(short_session),
        )
        self.assertGreaterEqual(
            recognizer._compute_finish_total_timeout(long_session),
            550.25 * 1.5 + 120.0,
        )

    def test_empty_text_with_silent_audio_does_not_retry(self) -> None:
        """
        云端正常结束但文本为空，且本地音量低于阈值时，应判为“疑似没录到声音”。

        这个用例保护一个关键产品行为：
        - 静音/近静音任务不进入自动重试，避免重复扣费和堆积无意义失败记录。
        - 客户端仍能拿到清晰错误状态，用于提示用户检查麦克风或说话音量。
        """
        recognizer = _new_recognizer_for_wait_test()
        session = _new_session_for_wait_test(duration=1.0)

        result = recognizer._build_empty_result(session)

        self.assertEqual(result.status, "failed_silent_audio")
        self.assertFalse(result.audio_diagnostics["has_voice"])
        self.assertFalse(recognizer._should_auto_retry_result(result))
        self.assertFalse(recognizer._should_persist_failure_result(result))

    def test_empty_text_with_voice_is_retryable_empty_result(self) -> None:
        """
        云端正常结束但文本为空，且本地检测到有效声音时，应判为“云端空识别”。

        这正是用户现场遇到的问题：人确实说话了，但云端返回 `words_count=0`。
        该状态应允许自动重试一次；如果重试仍为空，再进入失败任务落盘。
        """
        recognizer = _new_recognizer_for_wait_test()
        session = _new_session_for_wait_test(duration=1.0)
        session.audio_sample_count = 16000
        session.audio_square_sum = 16000 * (0.01 ** 2)
        session.audio_abs_peak = 0.08
        session.audio_frame_count = 10
        session.audio_voiced_frame_count = 8

        result = recognizer._build_empty_result(session)

        self.assertEqual(result.status, "failed_empty_result")
        self.assertTrue(result.audio_diagnostics["has_voice"])
        self.assertTrue(recognizer._should_auto_retry_result(result))
        self.assertTrue(recognizer._should_persist_failure_result(result))

    def test_final_text_success_remains_success_confirmed(self) -> None:
        """
        正常有文本的任务应保持原成功路径。

        这个用例防止空结果诊断误伤普通识别：只要句子状态机汇总出了文本，
        最终结果仍然是 `success_confirmed`，后续热词、LLM、上屏链路不变。
        """

        class _FakeWebSocket:
            """测试用假 WebSocket，只记录发送内容，不连接真实云端。"""

            def __init__(self) -> None:
                self.sent_messages: list[str] = []

            async def send(self, message: str) -> None:
                self.sent_messages.append(message)

        async def scenario() -> None:
            recognizer = _new_recognizer_for_wait_test()
            session = _new_session_for_wait_test(duration=1.0)
            session.ws = _FakeWebSocket()
            session.task_finished_received = True
            session.finished_event.set()
            session.sentences["begin:0"] = _SentenceState(
                key="begin:0",
                order=0,
                begin_time=0.0,
                end_time=1.0,
                text="你好。",
                is_final=True,
            )
            recognizer._sessions = {session.local_task_id: session}

            result = await recognizer._finish_and_collect(session.local_task_id)

            self.assertEqual(result.status, "success_confirmed")
            self.assertEqual(result.text, "你好。")
            self.assertEqual(result.error_code, "")

        asyncio.run(scenario())

    def test_middle_empty_span_does_not_mark_success_as_retryable(self) -> None:
        """
        中间出现较长空段，但后面又有有效文字时，不应误判为尾部漏识别。

        用户真实使用时可能按着快捷键思考、查资料、短暂停顿；这些空段只要不是
        “一路空到录音结束”，就不应该触发重试，避免无意义扣费和干扰上屏。
        """
        recognizer = _new_recognizer_for_wait_test()
        session = _new_session_for_wait_test(duration=50.0)
        session.sentences["begin:0"] = _SentenceState(
            key="begin:0",
            order=0,
            begin_time=0.0,
            end_time=10.0,
            text="前半句。",
            is_final=True,
        )
        session.empty_final_spans.append(_EmptyFinalSpan(
            begin_time=20.0,
            end_time=30.0,
            duration=10.0,
            final_sentence_count_before=1,
        ))
        session.sentences["begin:35"] = _SentenceState(
            key="begin:35",
            order=1,
            begin_time=35.0,
            end_time=48.0,
            text="后半句。",
            is_final=True,
        )

        text, tokens, timestamps = recognizer._build_final_text(session)
        result = recognizer._build_tail_empty_result_if_needed(session, text, tokens, timestamps)

        self.assertIsNone(result)

    def test_tail_empty_span_marks_result_retryable(self) -> None:
        """
        前面已有文字，但最后一路空到结束时，应改判为可重试的尾部空结果。

        这个用例对应用户现场现象：
        - 前 58 秒云端正常定稿。
        - 58 秒之后到 111 秒云端仍返回 `sentence_end=true`，但 `text_len=0`。
        - 总文本不为空，所以旧逻辑会误判 `success_confirmed` 并删除 PCM。
        - 新逻辑应保留现有文本作为保底，同时允许自动重试整段 PCM。
        """
        recognizer = _new_recognizer_for_wait_test()
        session = _new_session_for_wait_test(duration=112.8)
        session.sentences["begin:0"] = _SentenceState(
            key="begin:0",
            order=0,
            begin_time=0.0,
            end_time=58.58,
            text="前面已经识别出来的文本。",
            is_final=True,
        )
        session.empty_final_spans.append(_EmptyFinalSpan(
            begin_time=58.84,
            end_time=111.06,
            duration=52.22,
            final_sentence_count_before=1,
        ))

        text, tokens, timestamps = recognizer._build_final_text(session)
        result = recognizer._build_tail_empty_result_if_needed(session, text, tokens, timestamps)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.status, "failed_tail_empty_result")
        self.assertEqual(result.error_code, "tail_empty_result")
        self.assertEqual(result.salvage_text_finalized, "前面已经识别出来的文本。")
        self.assertTrue(recognizer._should_auto_retry_result(result))
        self.assertTrue(recognizer._should_persist_failure_result(result))


if __name__ == "__main__":
    unittest.main()
