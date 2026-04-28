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

from util.server.asr_aliyun_realtime import AliyunRealtimeRecognizer, _SessionState


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


if __name__ == "__main__":
    unittest.main()
